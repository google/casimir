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
use std::collections::HashMap;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::pin::Pin;
use std::task::Poll;
use tokio::net::TcpListener;
use tokio::select;
use tokio::sync::mpsc;

mod proto;

use ::casimir::packets::rf;
use ::casimir::Device as CasimirDevice;
use ::casimir::Id;
use ::casimir::MAX_DEVICES;
use proto::{casimir, casimir_grpc};

/// Identify the device type.
pub enum DeviceType {
    Nci,
    Rf,
}

/// Represent shared contextual information.
pub struct DeviceInformation {
    id: Id,
    position: u32,
    r#type: DeviceType,
}

struct Scene {
    next_id: u16,
    waker: Option<std::task::Waker>,
    devices: [Option<CasimirDevice>; MAX_DEVICES],
    context: std::sync::Arc<std::sync::Mutex<HashMap<Id, DeviceInformation>>>,
}

impl Scene {
    fn new() -> Scene {
        const NONE: Option<CasimirDevice> = None;
        Scene {
            next_id: 0,
            waker: None,
            devices: [NONE; MAX_DEVICES],
            context: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake()
        }
    }

    fn add_device(&mut self, builder: impl FnOnce(Id) -> CasimirDevice) -> Result<Id> {
        for n in 0..MAX_DEVICES {
            if self.devices[n].is_none() {
                let id = self.next_id;
                self.devices[n] = Some(builder(id));
                self.next_id += 1;
                self.wake();
                return Ok(id);
            }
        }
        Err(anyhow::anyhow!("max number of connections reached"))
    }

    fn disconnect(&mut self, n: usize) {
        let id = self.devices[n].as_ref().unwrap().id;
        self.devices[n] = None;
        self.context.lock().unwrap().remove(&id);
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
        let context = self.context.lock().unwrap();
        for n in 0..MAX_DEVICES {
            let Some(ref device) = self.devices[n] else { continue };
            if packet.sender() != device.id
                && (packet.receiver() == u16::MAX || packet.receiver() == device.id)
                && context.get(&device.id).map(|info| info.position)
                    == context.get(&packet.sender()).map(|info| info.position)
            {
                device.rf_tx.send(packet.to_owned())?;
            }
        }

        Ok(())
    }
}

impl Future for Scene {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<()> {
        for n in 0..MAX_DEVICES {
            let dropped = match self.devices[n] {
                Some(ref mut device) => match device.task.as_mut().poll(cx) {
                    Poll::Ready(Ok(_)) => unreachable!(),
                    Poll::Ready(Err(err)) => {
                        warn!("dropping device {}: {}", n, err);
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

#[derive(Clone)]
struct Service {
    context: std::sync::Arc<std::sync::Mutex<HashMap<Id, DeviceInformation>>>,
}

impl From<&DeviceInformation> for casimir::Device {
    fn from(info: &DeviceInformation) -> Self {
        let mut device = casimir::Device::new();
        device.set_device_id(info.id as u32);
        device.set_position(info.position);
        match info.r#type {
            DeviceType::Nci => device.set_nci(Default::default()),
            DeviceType::Rf => device.set_rf(Default::default()),
        }
        device
    }
}

impl casimir_grpc::Casimir for Service {
    fn list_devices(
        &mut self,
        _ctx: grpcio::RpcContext<'_>,
        _req: casimir::ListDevicesRequest,
        sink: grpcio::UnarySink<casimir::ListDevicesResponse>,
    ) {
        let mut response = casimir::ListDevicesResponse::new();
        response.set_device(
            self.context
                .lock()
                .unwrap()
                .values()
                .map(casimir::Device::from)
                .collect::<Vec<_>>()
                .into(),
        );
        sink.success(response);
    }

    fn get_device(
        &mut self,
        _ctx: grpcio::RpcContext<'_>,
        req: casimir::GetDeviceRequest,
        sink: grpcio::UnarySink<casimir::GetDeviceResponse>,
    ) {
        match self.context.lock().unwrap().get(&(req.get_device_id() as u16)) {
            Some(info) => {
                let mut response = casimir::GetDeviceResponse::new();
                response.set_device(info.into());
                sink.success(response)
            }
            None => sink.fail(grpcio::RpcStatus::with_message(
                grpcio::RpcStatusCode::INVALID_ARGUMENT,
                format!("device_id {} not found", req.get_device_id()),
            )),
        };
    }

    fn move_device(
        &mut self,
        _ctx: grpcio::RpcContext<'_>,
        req: casimir::MoveDeviceRequest,
        sink: grpcio::UnarySink<casimir::MoveDeviceResponse>,
    ) {
        match self.context.lock().unwrap().get_mut(&(req.get_device_id() as u16)) {
            Some(info) => {
                info.position = req.get_position();
                sink.success(Default::default())
            }
            None => sink.fail(grpcio::RpcStatus::with_message(
                grpcio::RpcStatusCode::INVALID_ARGUMENT,
                format!("device_id {} not found", req.get_device_id()),
            )),
        };
    }
}

#[derive(FromArgs, Debug)]
/// Nfc emulator.
struct Opt {
    #[argh(option, default = "7000")]
    /// configure the TCP port for the NCI server.
    nci_port: u16,
    #[argh(option, default = "7001")]
    /// configure the TCP port for the RF server.
    rf_port: u16,
    #[argh(option, default = "50051")]
    /// configure the gRPC port.
    grpc_port: u16,
}

async fn run() -> Result<()> {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "debug"),
    );

    let opt: Opt = argh::from_env();
    let nci_listener =
        TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, opt.nci_port)).await?;
    let rf_listener =
        TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, opt.rf_port)).await?;
    let (rf_tx, mut rf_rx) = mpsc::unbounded_channel();
    let mut scene = Scene::new();

    info!("Listening for NCI connections at address 127.0.0.1:{}", opt.nci_port);
    info!("Listening for RF connections at address 127.0.0.1:{}", opt.rf_port);
    info!("Listening for gRPC connections at address 127.0.0.1:{}", opt.grpc_port);

    let env = std::sync::Arc::new(grpcio::Environment::new(1));
    let service = casimir_grpc::create_casimir(Service { context: scene.context.clone() });
    let quota = grpcio::ResourceQuota::new(Some("CasimirQuota")).resize_memory(1024 * 1024);
    let channel_builder = grpcio::ChannelBuilder::new(env.clone()).set_resource_quota(quota);

    let mut server = grpcio::ServerBuilder::new(env)
        .register_service(service)
        .channel_args(channel_builder.build_args())
        .build()
        .unwrap();
    server
        .add_listening_port(
            format!("127.0.0.1:{}", opt.grpc_port),
            grpcio::ServerCredentials::insecure(),
        )
        .unwrap();
    server.start();

    loop {
        select! {
            result = nci_listener.accept() => {
                let (socket, addr) = result?;
                info!("Incoming NCI connection from {}", addr);
                let (socket_rx, socket_tx) = socket.into_split();
                match scene.add_device(|id| CasimirDevice::nci(id, socket_rx, socket_tx, rf_tx.clone())) {
                    Ok(id) => {
                        scene.context.lock().unwrap().insert(id, DeviceInformation {
                            id, position: id as u32, r#type: DeviceType::Nci
                        });
                        info!("Accepted NCI connection from {} with id {}", addr, id)
                    }
                    Err(err) => error!("Failed to accept NCI connection from {}: {}", addr, err)
                }
            },
            result = rf_listener.accept() => {
                let (socket, addr) = result?;
                info!("Incoming RF connection from {}", addr);
                let (socket_rx, socket_tx) = socket.into_split();
                match scene.add_device(|id| CasimirDevice::rf(id, socket_rx, socket_tx, rf_tx.clone())) {
                    Ok(id) => {
                        scene.context.lock().unwrap().insert(id, DeviceInformation {
                            id, position: id as u32, r#type: DeviceType::Rf
                        });
                        info!("Accepted RF connection from {} with id {}", addr, id)
                    }
                    Err(err) => error!("Failed to accept RF connection from {}: {}", addr, err)
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

#[tokio::main]
async fn main() -> Result<()> {
    run().await
}
