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
use casimir::{Device, Scene};
use command_fds::inherited::{init_inherited_fds, take_fd_ownership};
use log::{error, info};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, UnixListener};
use tokio::select;
use tokio::sync::mpsc;

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
            let owned_fd = take_fd_ownership(unix_fd)?;
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
            let owned_fd = take_fd_ownership(unix_fd)?;
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
    unsafe { init_inherited_fds()? };
    run()
}
