// Copyright 2026, The Android Open Source Project
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

//! Casimir NFC Simulator Library.

#![cfg_attr(
    test,
    allow(
        clippy::indexing_slicing,
        clippy::panic,
        clippy::panic_in_result_fn,
        clippy::unwrap_used
    )
)]

use anyhow::Result;
use log::warn;
use pdl_runtime::Packet;
use std::future::Future;
use std::pin::{pin, Pin};
use std::task::Context;
use std::task::Poll;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

pub mod controller;
pub mod crc;
pub mod packets;

use controller::Controller;
use packets::{nci, rf};

/// The maximum number of devices supported by the emulator.
pub const MAX_DEVICES: usize = 128;
/// Device identifier.
pub type Id = u16;

/// Read RF Control and Data packets received on the RF transport.
/// Performs recombination of the segmented packets.
pub struct RfReader {
    socket: Pin<Box<dyn AsyncRead + Send>>,
}

/// Write RF Control and Data packets received to the RF transport.
/// Performs segmentation of the packets.
pub struct RfWriter {
    socket: Pin<Box<dyn AsyncWrite + Send>>,
}

impl RfReader {
    /// Create a new RF reader from an `AsyncRead` implementation.
    pub fn new(socket: impl AsyncRead + Send + 'static) -> Self {
        RfReader { socket: Box::pin(socket) }
    }

    /// Read a single RF packet from the reader.
    /// RF packets are framed with the byte size encoded as little-endian u16.
    pub async fn read(&mut self) -> Result<Vec<u8>> {
        const HEADER_SIZE: usize = 2;
        let mut header_bytes = [0; HEADER_SIZE];

        // Read the header bytes.
        self.socket.read_exact(&mut header_bytes).await?;
        let packet_length = u16::from_le_bytes(header_bytes) as usize;

        // Read the packet data.
        let mut packet_bytes = vec![0; packet_length];
        self.socket.read_exact(&mut packet_bytes).await?;

        Ok(packet_bytes)
    }
}

impl RfWriter {
    /// Create a new RF writer from an `AsyncWrite` implementation.
    pub fn new(socket: impl AsyncWrite + Send + 'static) -> Self {
        RfWriter { socket: Box::pin(socket) }
    }

    /// Write a single RF packet to the writer.
    /// RF packets are framed with the byte size encoded as little-endian u16.
    pub async fn write(&mut self, packet: &[u8]) -> Result<()> {
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
    /// Unique identifier associated with the device.
    /// The identifier is assured never to be reused in the lifetime of
    /// the emulator.
    pub id: u16,
    /// Async task running the controller main loop.
    pub task: Pin<Box<dyn Future<Output = Result<()>> + Send>>,
    /// Channel for injecting RF data packets into the controller instance.
    pub rf_tx: mpsc::UnboundedSender<rf::RfPacket>,
}

impl Device {
    /// Create a new NCI device.
    pub fn nci(
        id: Id,
        nci_rx: impl AsyncRead + Send + 'static,
        nci_tx: impl AsyncWrite + Send + 'static,
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

    /// Create a new RF device.
    pub fn rf(
        id: Id,
        socket_rx: impl AsyncRead + Send + 'static,
        socket_tx: impl AsyncWrite + Send + 'static,
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
                            let Some(sender_bytes) = packet_bytes.first_chunk_mut::<2>() else {
                                anyhow::bail!("RF packet shorter than 2-byte sender header");
                            };
                            *sender_bytes = id.to_le_bytes();

                            // Parse the input packet.
                            let (packet, _) = rf::RfPacket::decode(&packet_bytes)?;

                            // Forward the packet to other devices.
                            controller_rf_tx.send(packet)?;
                        }
                    },
                    async {
                        loop {
                            // Forward the packet to the socket connection.
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

/// Represent the emulation scene containing all connected devices.
pub struct Scene {
    next_id: u16,
    waker: Option<std::task::Waker>,
    /// List of devices in the scene.
    pub devices: [Option<Device>; MAX_DEVICES],
    rf_tx: mpsc::UnboundedSender<rf::RfPacket>,
    rf_rx: Option<mpsc::UnboundedReceiver<rf::RfPacket>>,
}

impl Default for Scene {
    fn default() -> Self {
        const NONE: Option<Device> = None;
        let (rf_tx, rf_rx) = mpsc::unbounded_channel();
        Scene { next_id: 0, waker: None, devices: [NONE; MAX_DEVICES], rf_tx, rf_rx: Some(rf_rx) }
    }
}

impl Scene {
    /// Create a new empty scene.
    pub fn new() -> Scene {
        Default::default()
    }

    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake()
        }
    }

    /// Add a device to the scene.
    pub fn add_device(
        &mut self,
        builder: impl FnOnce(Id, mpsc::UnboundedSender<rf::RfPacket>) -> Device,
    ) -> Result<Id> {
        for (n, slot) in self.devices.iter_mut().enumerate() {
            if slot.is_none() {
                *slot = Some(builder(self.next_id, self.rf_tx.clone()));
                self.next_id += 1;
                self.wake();
                return Ok(n as Id);
            }
        }
        Err(anyhow::anyhow!("max number of connections reached"))
    }

    /// Disconnect a device from the scene.
    pub fn disconnect(&mut self, n: usize) {
        let Some(id) = self.devices.get_mut(n).and_then(Option::take).map(|d| d.id) else {
            return;
        };
        for device in self.devices.iter().flatten() {
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
                .expect("failed to send deactive notification");
        }
    }

    /// Send a packet to all other devices in the scene.
    fn send(&self, packet: &rf::RfPacket) -> Result<()> {
        for device in self.devices.iter().flatten() {
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
        if let Some(mut rx) = self.rf_rx.take() {
            while let Poll::Ready(Some(packet)) = rx.poll_recv(cx) {
                if let Err(err) = self.send(&packet) {
                    warn!("failed to forward RF packet: {err}");
                }
            }
            self.rf_rx = Some(rx);
        }

        for n in 0..MAX_DEVICES {
            let dropped = match self.devices.get_mut(n).and_then(Option::as_mut) {
                Some(device) => match device.task.as_mut().poll(cx) {
                    Poll::Ready(Ok(())) => {
                        warn!("device {n} task finished unexpectedly");
                        true
                    }
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
