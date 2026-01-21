// Copyright 2023, The Android Open Source Project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! NFCC and RF emulator.

use anyhow::Result;
use argh::FromArgs;
use log::{error, info, warn};
use pdl_runtime::Packet;
use rustutils::inherited_fd;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::pin::{pin, Pin};
use std::task::Context;
use std::task::Poll;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, UnixListener};
use tokio::select;
use tokio::sync::mpsc;

pub mod controller;
pub mod crc;
pub mod packets;

use controller::Controller;
use packets::{nci, rf};

const MAX_DEVICES: usize = 128;
type Id = u16;

/// Read RF Control and Data packets received on the RF transport.
/// Performs recombination of the segmented packets.
pub struct RfReader {
    socket: Pin<Box<dyn AsyncRead>>,
}

/// Write RF Control and Data packets received to the RF transport.
/// Performs segmentation of the packets.
pub struct RfWriter {
    socket: Pin<Box<dyn AsyncWrite>>,
}

impl RfReader {
    /// Create a new RF reader from an `AsyncRead` implementation.
    pub fn new(socket: impl AsyncRead + 'static) -> Self {
        RfReader { socket: Box::pin(socket) }
    }

    /// Read a single RF packet from the reader.
    /// RF packets are framed with the byte size encoded as little-endian u16.
    pub async fn read(&mut self) -> Result<Vec<u8>> {
        const HEADER_SIZE: usize = 2;
        let mut header_bytes = [0; HEADER_SIZE];

        // Read the header bytes.
        self.socket.read_exact(&mut header_bytes[0..HEADER_SIZE]).await?;
        let packet_length = u16::from_le_bytes(header_bytes) as usize;

        // Read the packet data.
        let mut packet_bytes = vec![0; packet_length];
        self.socket.read_exact(&mut packet_bytes).await?;

        Ok(packet_bytes)
    }
}

impl RfWriter {
    /// Create a new RF writer from an `AsyncWrite` implementation.
    pub fn new(socket: impl AsyncWrite + 'static) -> Self {
        RfWriter { socket: Box::pin(socket) }
    }

    /// Write a single RF packet to the writer.
    /// RF packets are framed with the byte size encoded as little-endian u16.
    async fn write(&mut self, packet: &[u8]) -> Result<()> {
        let packet_length: u16 = packet.len().try_into()?;
        let header_bytes = packet_length.to_le_bytes();

        // Write the header bytes.
        self.socket.write_all(&header_bytes).await?;

        // Write the packet data.
        self.socket.write_all(packet).await?;

        Ok(())
    }
}

/// Represent a generic NFC device interacting on the RF transport.
/// Devices communicate together through the RF mpsc channel.
/// NFCCs are an instance of Device.
pub struct Device {
    // Unique identifier associated with the device.
    // The identifier is assured never to be reused in the lifetime of
    // the emulator.
    id: u16,
    // Async task running the controller main loop.
    task: Pin<Box<dyn Future<Output = Result<()>>>>,
    // Channel for injecting RF data packets into the controller instance.
    rf_tx: mpsc::UnboundedSender<rf::RfPacket>,
}

impl Device {
    fn nci(
        id: Id,
        nci_rx: impl AsyncRead + 'static,
        nci_tx: impl AsyncWrite + 'static,
        controller_rf_tx: mpsc::UnboundedSender<rf::RfPacket>,
    ) -> Device {
        let (rf_tx, rf_rx) = mpsc::unbounded_channel();
        Device {
            id,
            rf_tx,
            task: Box::pin(async move {
                Controller::run(
                    id,
                    pin!(nci::Reader::new(nci_rx).into_stream()),
                    nci::Writer::new(nci_tx),
                    rf_rx,
                    controller_rf_tx,
                )
                .await
            }),
        }
    }

    fn rf(
        id: Id,
        socket_rx: impl AsyncRead + 'static,
        socket_tx: impl AsyncWrite + 'static,
        controller_rf_tx: mpsc::UnboundedSender<rf::RfPacket>,
    ) -> Device {
        let (rf_tx, mut rf_rx) = mpsc::unbounded_channel();
        Device {
            id,
            rf_tx,
            task: Box::pin(async move {
                let mut rf_reader = RfReader::new(socket_rx);
                let mut rf_writer = RfWriter::new(socket_tx);

                let result: Result<((), ())> = futures::future::try_join(
                    async {
                        loop {
                            // Replace the sender identifier in the packet
                            // with the assigned number for the RF connection.
                            // TODO: currently the generated API does not allow
                            // modifying the parsed fields so the change needs to be
                            // applied to the unparsed packet.
                            let mut packet_bytes = rf_reader.read().await?;
                            packet_bytes[0..2].copy_from_slice(&id.to_le_bytes());

                            // Parse the input packet.
                            let packet = rf::RfPacket::decode_full(&packet_bytes)?;

                            // Forward the packet to other devices.
                            controller_rf_tx.send(packet)?;
                        }
                    },
                    async {
                        loop {
                            // Forward the packet to the socket connection.
                            use pdl_runtime::Packet;
                            let packet = rf_rx
                                .recv()
                                .await
                                .ok_or(anyhow::anyhow!("rf_rx channel closed"))?;
                            rf_writer.write(&packet.encode_to_vec()?).await?;
                        }
                    },
                )
                .await;

                result?;
                Ok(())
            }),
        }
    }
}

struct Scene {
    next_id: u16,
    waker: Option<std::task::Waker>,
    devices: [Option<Device>; MAX_DEVICES],
}

impl Default for Scene {
    fn default() -> Self {
        const NONE: Option<Device> = None;
        Scene { next_id: 0, waker: None, devices: [NONE; MAX_DEVICES] }
    }
}

impl Scene {
    fn new() -> Scene {
        Default::default()
    }

    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake()
        }
    }

    fn add_device(&mut self, builder: impl FnOnce(Id) -> Device) -> Result<Id> {
        for n in 0..MAX_DEVICES {
            if self.devices[n].is_none() {
                self.devices[n] = Some(builder(self.next_id));
                self.next_id += 1;
                self.wake();
                return Ok(n as Id);
            }
        }
        Err(anyhow::anyhow!("max number of connections reached"))
    }

    fn disconnect(&mut self, n: usize) {
        let id = self.devices[n].as_ref().unwrap().id;
        self.devices[n] = None;
        for other_n in 0..MAX_DEVICES {
            let Some(ref device) = self.devices[other_n] else { continue };
            assert!(n != other_n);
            device
                .rf_tx
                .send(
                    rf::DeactivateNotification {
                        type_: rf::DeactivateType::Discovery,
                        reason: rf::DeactivateReason::RfLinkLoss,
                        sender: id,
                        receiver: device.id,
                        bitrate: rf::BitRate::BitRate106KbitS,
                        power_level: 255,
                        technology: rf::Technology::NfcA,
                        protocol: rf::Protocol::Undetermined,
                    }
                    .try_into()
                    .expect("failed to serialize notification"),
                )
                .expect("failed to send deactive notification")
        }
    }

    fn send(&self, packet: &rf::RfPacket) -> Result<()> {
        for n in 0..MAX_DEVICES {
            let Some(ref device) = self.devices[n] else { continue };
            if packet.sender() != device.id
                && (packet.receiver() == u16::MAX || packet.receiver() == device.id)
            {
                device.rf_tx.send(packet.to_owned())?;
            }
        }

        Ok(())
    }
}

impl Future for Scene {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        for n in 0..MAX_DEVICES {
            let dropped = match self.devices[n] {
                Some(ref mut device) => match device.task.as_mut().poll(cx) {
                    Poll::Ready(Ok(_)) => unreachable!(),
                    Poll::Ready(Err(err)) => {
                        warn!("dropping device {n}: {err}");
                        true
                    }
                    Poll::Pending => false,
                },
                None => false,
            };
            if dropped {
                self.disconnect(n)
            }
        }
        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

#[derive(FromArgs, Debug)]
/// Nfc emulator.
struct Opt {
    #[argh(option)]
    /// configure the TCP port for the NCI server.
    nci_port: Option<u16>,
    #[argh(option)]
    /// configure a preexisting unix server fd for the NCI server.
    nci_unix_fd: Option<i32>,
    #[argh(option)]
    /// configure the TCP port for the RF server.
    rf_port: Option<u16>,
    #[argh(option)]
    /// configure a preexisting unix server fd for the RF server.
    rf_unix_fd: Option<i32>,
}

/// Abstraction between different server sources
enum Listener {
    Tcp(TcpListener),
    #[allow(unused)]
    Unix(UnixListener),
}

impl Listener {
    async fn accept_split(
        &self,
    ) -> Result<(Pin<Box<dyn AsyncRead>>, Pin<Box<dyn AsyncWrite>>, String)> {
        match self {
            Listener::Tcp(tcp) => {
                let (socket, addr) = tcp.accept().await?;
                let (rx, tx) = socket.into_split();
                Ok((Box::pin(rx), Box::pin(tx), format!("{addr}")))
            }
            Listener::Unix(unix) => {
                let (socket, addr) = unix.accept().await?;
                let (rx, tx) = socket.into_split();
                Ok((Box::pin(rx), Box::pin(tx), format!("{addr:?}")))
            }
        }
    }
}

#[tokio::main]
async fn run() -> Result<()> {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "debug"),
    );

    let opt: Opt = argh::from_env();

    let nci_listener = match (opt.nci_port, opt.nci_unix_fd) {
        (None, Some(unix_fd)) => {
            let owned_fd = inherited_fd::take_fd_ownership(unix_fd)?;
            let nci_listener = std::os::unix::net::UnixListener::from(owned_fd);
            nci_listener.set_nonblocking(true)?;
            let nci_listener = UnixListener::from_std(nci_listener)?;
            info!("Listening for NCI connections on fd {unix_fd}");
            Listener::Unix(nci_listener)
        }
        (port, None) => {
            let port = port.unwrap_or(7000);
            let nci_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);
            let nci_listener = TcpListener::bind(nci_addr).await?;
            info!("Listening for NCI connections at address {nci_addr}");
            Listener::Tcp(nci_listener)
        }
        _ => anyhow::bail!("Specify at most one of `--nci-port` and `--nci-unix-fd`."),
    };

    let rf_listener = match (opt.rf_port, opt.rf_unix_fd) {
        (None, Some(unix_fd)) => {
            let owned_fd = inherited_fd::take_fd_ownership(unix_fd)?;
            let nci_listener = std::os::unix::net::UnixListener::from(owned_fd);
            nci_listener.set_nonblocking(true)?;
            let nci_listener = UnixListener::from_std(nci_listener)?;
            info!("Listening for RF connections on fd {unix_fd}");
            Listener::Unix(nci_listener)
        }
        (port, None) => {
            let port = port.unwrap_or(7001);
            let rf_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);
            let rf_listener = TcpListener::bind(rf_addr).await?;
            info!("Listening for RF connections at address {rf_addr}");
            Listener::Tcp(rf_listener)
        }
        _ => anyhow::bail!("Specify at most one of `--rf-port` and `--rf-unix-fd`"),
    };

    let (rf_tx, mut rf_rx) = mpsc::unbounded_channel();
    let mut scene = Scene::new();
    loop {
        select! {
            result = nci_listener.accept_split() => {
                let (socket_rx, socket_tx, addr) = result?;
                info!("Incoming NCI connection from {addr}");
                match scene.add_device(|id| Device::nci(id, socket_rx, socket_tx, rf_tx.clone())) {
                    Ok(id) => info!("Accepted NCI connection from {addr} in slot {id}"),
                    Err(err) => error!("Failed to accept NCI connection from {addr}: {err}")
                }
            },
            result = rf_listener.accept_split() => {
                let (socket_rx, socket_tx, addr) = result?;
                info!("Incoming RF connection from {addr}");
                match scene.add_device(|id| Device::rf(id, socket_rx, socket_tx, rf_tx.clone())) {
                    Ok(id) => info!("Accepted RF connection from {addr} in slot {id}"),
                    Err(err) => error!("Failed to accept RF connection from {addr}: {err}")
                }
            },
            _ = &mut scene => (),
            result = rf_rx.recv() => {
                let packet = result.ok_or(anyhow::anyhow!("rf_rx channel closed"))?;
                scene.send(&packet)?
            }
        }
    }
}

fn main() -> Result<()> {
    // Safety: First function call in the `main` function, before any other library calls
    unsafe { inherited_fd::init_once()? };
    run()
}
