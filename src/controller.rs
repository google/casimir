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

//! Implementation of the NFCC.

use crate::packets::nci;
use crate::packets::nci::Packet;
use crate::NciReader;
use crate::NciWriter;
use anyhow::Result;
use core::time::Duration;
use std::collections::HashMap;
use std::convert::TryFrom;
use tokio::select;
use tokio::sync::mpsc;
use tokio::time;

const NCI_VERSION: nci::NciVersion = nci::NciVersion::Version11;
const MAX_LOGICAL_CONNECTIONS: u8 = 2;
const MAX_ROUTING_TABLE_SIZE: u16 = 512;
const MAX_CONTROL_PACKET_PAYLOAD_SIZE: u8 = 255;
const MAX_DATA_PACKET_PAYLOAD_SIZE: u8 = 255;
const NUMBER_OF_CREDITS: u8 = 0;
const MAX_NFCV_RF_FRAME_SIZE: u16 = 512;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum LogicalConnection {
    RemoteNfcEndpoint { rf_discovery_id: u8, rf_protocol_type: nci::RfProtocolType },
}

/// State of an NFCC instance.
pub struct Controller {
    #[allow(dead_code)]
    id: usize,
    nci_reader: NciReader,
    nci_writer: NciWriter,
    rf_rx: mpsc::Receiver<Vec<u8>>,
    #[allow(dead_code)]
    rf_tx: mpsc::Sender<(usize, Vec<u8>)>,
    config_parameters: HashMap<nci::ConfigParameterId, Vec<u8>>,
    logical_connections: [Option<LogicalConnection>; MAX_LOGICAL_CONNECTIONS as usize],
}

impl Controller {
    /// Create a new NFCC instance with default configuration.
    pub fn new(
        id: usize,
        nci_reader: NciReader,
        nci_writer: NciWriter,
        rf_rx: mpsc::Receiver<Vec<u8>>,
        rf_tx: mpsc::Sender<(usize, Vec<u8>)>,
    ) -> Controller {
        Controller {
            id,
            nci_reader,
            nci_writer,
            rf_rx,
            rf_tx,
            config_parameters: HashMap::new(),
            logical_connections: [None; MAX_LOGICAL_CONNECTIONS as usize],
        }
    }

    async fn send_control(&mut self, packet: impl Into<nci::ControlPacket>) -> Result<()> {
        self.nci_writer.write(&packet.into().to_vec()).await
    }

    #[allow(dead_code)]
    async fn send_data(&mut self, packet: impl Into<nci::DataPacket>) -> Result<()> {
        self.nci_writer.write(&packet.into().to_vec()).await
    }

    #[allow(dead_code)]
    async fn send_rf(&mut self, packet: Vec<u8>) -> Result<()> {
        self.rf_tx.send((self.id, packet)).await?;
        Ok(())
    }

    async fn core_reset(&mut self, cmd: nci::CoreResetCommand) -> Result<()> {
        println!("+ core_reset_cmd({:?})", cmd.get_reset_type());

        self.send_control(nci::CoreResetResponseBuilder { status: nci::Status::Ok }).await?;

        self.send_control(nci::CoreResetNotificationBuilder {
            trigger: nci::ResetTrigger::ResetCommand,
            config_status: match cmd.get_reset_type() {
                nci::ResetType::KeepConfig => nci::ConfigStatus::ConfigKept,
                nci::ResetType::ResetConfig => nci::ConfigStatus::ConfigReset,
            },
            nci_version: NCI_VERSION,
            manufacturer_id: 0,
            manufacturer_specific_information: vec![],
        })
        .await?;

        Ok(())
    }

    async fn core_init(&mut self, _cmd: nci::CoreInitCommand) -> Result<()> {
        println!("+ core_init_cmd()");

        self.send_control(nci::CoreInitResponseBuilder {
            status: nci::Status::Ok,
            nfcc_features: nci::NfccFeatures {
                discovery_frequency_configuration: nci::FeatureFlag::Disabled,
                discovery_configuration_mode: nci::DiscoveryConfigurationMode::DhOnly,
                hci_network_support: nci::FeatureFlag::Disabled,
                active_communication_mode: nci::FeatureFlag::Disabled,
                technology_based_routing: nci::FeatureFlag::Disabled,
                protocol_based_routing: nci::FeatureFlag::Disabled,
                aid_based_routing: nci::FeatureFlag::Disabled,
                system_code_based_routing: nci::FeatureFlag::Disabled,
                apdu_pattern_based_routing: nci::FeatureFlag::Disabled,
                forced_nfcee_routing: nci::FeatureFlag::Disabled,
                battery_off_state: nci::FeatureFlag::Disabled,
                switched_off_state: nci::FeatureFlag::Disabled,
                switched_on_substates: nci::FeatureFlag::Disabled,
                rf_configuration_in_switched_off_state: nci::FeatureFlag::Disabled,
                proprietary_capabilities: 0,
            },
            max_logical_connections: MAX_LOGICAL_CONNECTIONS,
            max_routing_table_size: MAX_ROUTING_TABLE_SIZE,
            max_control_packet_payload_size: MAX_CONTROL_PACKET_PAYLOAD_SIZE,
            max_data_packet_payload_size: MAX_DATA_PACKET_PAYLOAD_SIZE,
            number_of_credits: NUMBER_OF_CREDITS,
            max_nfcv_rf_frame_size: MAX_NFCV_RF_FRAME_SIZE,
            supported_rf_interfaces: vec![nci::RfInterface {
                interface: nci::RfInterfaceType::NfcDep,
                extensions: vec![],
            }],
        })
        .await?;

        Ok(())
    }

    async fn core_set_config(&mut self, cmd: nci::CoreSetConfigCommand) -> Result<()> {
        println!("+ core_set_config_cmd()");

        let mut invalid_parameters = vec![];
        for parameter in cmd.get_parameters().iter() {
            match parameter.id {
                nci::ConfigParameterId::Rfu(_) => invalid_parameters.push(parameter.id),
                _ => {
                    self.config_parameters.insert(parameter.id, parameter.value.clone());
                }
            }
        }

        self.send_control(nci::CoreSetConfigResponseBuilder {
            status: if invalid_parameters.is_empty() {
                // A Status of STATUS_OK SHALL indicate that all configuration parameters
                // have been set to these new values in the NFCC.
                nci::Status::Ok
            } else {
                // If the DH tries to set a parameter that is not applicable for the NFCC,
                // the NFCC SHALL respond with a CORE_SET_CONFIG_RSP with a Status field
                // of STATUS_INVALID_PARAM and including one or more invalid Parameter ID(s).
                // All other configuration parameters SHALL have been set to the new values
                // in the NFCC.
                println!(
                    " > rejecting unknown configuration parameter ids: {:?}",
                    invalid_parameters
                );
                nci::Status::InvalidParam
            },
            parameters: invalid_parameters,
        })
        .await?;

        Ok(())
    }

    async fn core_get_config(&mut self, cmd: nci::CoreGetConfigCommand) -> Result<()> {
        println!("+ core_get_config_cmd()");

        let mut valid_parameters = vec![];
        let mut invalid_parameters = vec![];
        for id in cmd.get_parameters() {
            match self.config_parameters.get(id) {
                Some(value) => {
                    valid_parameters.push(nci::ConfigParameter { id: *id, value: value.clone() })
                }
                None => invalid_parameters.push(nci::ConfigParameter { id: *id, value: vec![] }),
            }
        }

        self.send_control(if invalid_parameters.is_empty() {
            // If the NFCC is able to respond with all requested parameters, the
            // NFCC SHALL respond with the CORE_GET_CONFIG_RSP with a Status
            // of STATUS_OK.
            nci::CoreGetConfigResponseBuilder {
                status: nci::Status::Ok,
                parameters: valid_parameters,
            }
        } else {
            // If the DH tries to retrieve any parameter(s) that are not available
            // in the NFCC, the NFCC SHALL respond with a CORE_GET_CONFIG_RSP with
            // a Status field of STATUS_INVALID_PARAM, containing each unavailable
            // Parameter ID with a Parameter Len field of value zero.
            nci::CoreGetConfigResponseBuilder {
                status: nci::Status::InvalidParam,
                parameters: invalid_parameters,
            }
        })
        .await?;

        Ok(())
    }

    async fn core_conn_create(&mut self, cmd: nci::CoreConnCreateCommand) -> Result<()> {
        println!("+ core_conn_create()");

        let result: std::result::Result<u8, nci::Status> = (|| {
            // Retrieve an unused connection ID for the logical connection.
            let conn_id = (0..MAX_LOGICAL_CONNECTIONS)
                .find(|conn_id| self.logical_connections[*conn_id as usize].is_none())
                .ok_or(nci::Status::Rejected)?;

            // Check that the selected destination type is supported and validate
            // the destination specific parameters.
            let logical_connection = match cmd.get_destination_type() {
                // If the value of Destination Type is that of a Remote NFC
                // Endpoint (0x02), then only the Destination-specific Parameter
                // with Type 0x00 or proprietary parameters (as defined in Table 16)
                // SHALL be present.
                nci::DestinationType::RemoteNfcEndpoint => {
                    let mut rf_discovery_id: Option<u8> = None;
                    let mut rf_protocol_type: Option<nci::RfProtocolType> = None;

                    for parameter in cmd.get_parameters() {
                        match parameter.id {
                            nci::DestinationSpecificParameterId::RfDiscovery => {
                                rf_discovery_id = parameter.value.first().cloned();
                                rf_protocol_type = parameter
                                    .value
                                    .get(1)
                                    .and_then(|t| nci::RfProtocolType::try_from(*t).ok());
                            }
                            _ => return Err(nci::Status::Rejected),
                        }
                    }

                    LogicalConnection::RemoteNfcEndpoint {
                        rf_discovery_id: rf_discovery_id.ok_or(nci::Status::Rejected)?,
                        rf_protocol_type: rf_protocol_type.ok_or(nci::Status::Rejected)?,
                    }
                }
                nci::DestinationType::NfccLoopback | nci::DestinationType::Nfcee => {
                    return Err(nci::Status::Rejected)
                }
            };

            // The combination of Destination Type and Destination Specific
            // Parameters SHALL uniquely identify a single destination for the
            // Logical Connection.
            if self.logical_connections.iter().any(|c| c.as_ref() == Some(&logical_connection)) {
                return Err(nci::Status::Rejected);
            }

            // Create the connection.
            self.logical_connections[conn_id as usize] = Some(logical_connection);

            Ok(conn_id)
        })();

        self.send_control(match result {
            Ok(conn_id) => nci::CoreConnCreateResponseBuilder {
                status: nci::Status::Ok,
                max_data_packet_payload_size: MAX_DATA_PACKET_PAYLOAD_SIZE,
                initial_number_of_credits: 0xff,
                conn_id,
            },
            Err(status) => nci::CoreConnCreateResponseBuilder {
                status,
                max_data_packet_payload_size: 0,
                initial_number_of_credits: 0xff,
                conn_id: 0,
            },
        })
        .await?;

        Ok(())
    }

    async fn core_conn_close(&mut self, cmd: nci::CoreConnCloseCommand) -> Result<()> {
        println!("+ core_conn_close({})", cmd.get_conn_id());

        let conn_id = cmd.get_conn_id();
        let status = if conn_id >= MAX_LOGICAL_CONNECTIONS
            || self.logical_connections[conn_id as usize].is_none()
        {
            // If there is no connection associated to the Conn ID in the CORE_CONN_CLOSE_CMD, the
            // NFCC SHALL reject the connection closure request by sending a CORE_CONN_CLOSE_RSP
            // with a Status of STATUS_REJECTED.
            nci::Status::Rejected
        } else {
            // When it receives a CORE_CONN_CLOSE_CMD for an existing connection, the NFCC SHALL
            // accept the connection closure request by sending a CORE_CONN_CLOSE_RSP with a Status of
            // STATUS_OK, and the Logical Connection is closed.
            self.logical_connections[conn_id as usize] = None;
            nci::Status::Ok
        };

        self.send_control(nci::CoreConnCloseResponseBuilder { status }).await?;

        Ok(())
    }

    async fn core_set_power_sub_state(
        &mut self,
        cmd: nci::CoreSetPowerSubStateCommand,
    ) -> Result<()> {
        println!("+ core_set_power_sub_state({:?})", cmd.get_power_state());

        self.send_control(nci::CoreSetPowerSubStateResponseBuilder { status: nci::Status::Ok })
            .await?;

        Ok(())
    }

    async fn rf_discover_map(&mut self, _cmd: nci::RfDiscoverMapCommand) -> Result<()> {
        println!("+ rf_discover_map()");

        self.send_control(nci::RfDiscoverMapResponseBuilder { status: nci::Status::Ok }).await?;

        Ok(())
    }

    async fn rf_set_listen_mode_routing(
        &mut self,
        _cmd: nci::RfSetListenModeRoutingCommand,
    ) -> Result<()> {
        println!("+ rf_set_listen_mode_routing()");

        self.send_control(nci::RfSetListenModeRoutingResponseBuilder { status: nci::Status::Ok })
            .await?;

        Ok(())
    }

    async fn rf_get_listen_mode_routing(
        &mut self,
        _cmd: nci::RfGetListenModeRoutingCommand,
    ) -> Result<()> {
        println!("+ rf_get_listen_mode_routing()");

        self.send_control(nci::RfGetListenModeRoutingResponseBuilder {
            status: nci::Status::Ok,
            more_to_follow: 0,
            routing_entries: vec![],
        })
        .await?;

        Ok(())
    }

    async fn rf_discover(&mut self, _cmd: nci::RfDiscoverCommand) -> Result<()> {
        println!("+ rf_discover()");

        self.send_control(nci::RfDiscoverResponseBuilder { status: nci::Status::Ok }).await?;

        Ok(())
    }

    async fn rf_deactivate(&mut self, cmd: nci::RfDeactivateCommand) -> Result<()> {
        println!("+ rf_deactivate({:?})", cmd.get_deactivation_type());

        self.send_control(nci::RfDeactivateResponseBuilder { status: nci::Status::Ok }).await?;

        self.send_control(nci::RfDeactivateNotificationBuilder {
            deactivation_type: cmd.get_deactivation_type(),
            deactivation_reason: nci::DeactivationReason::DhRequest,
        })
        .await?;

        Ok(())
    }

    async fn nfcee_discover(&mut self, _cmd: nci::NfceeDiscoverCommand) -> Result<()> {
        println!("+ nfcee_discover()");

        self.send_control(nci::NfceeDiscoverResponseBuilder {
            status: nci::Status::Ok,
            number_of_nfcees: 0,
        })
        .await?;

        Ok(())
    }

    async fn receive_command(&mut self, packet: nci::ControlPacket) -> Result<()> {
        use nci::ControlPacketChild::*;
        use nci::CorePacketChild::*;
        use nci::NfceePacketChild::*;
        use nci::RfPacketChild::*;

        match packet.specialize() {
            CorePacket(packet) => match packet.specialize() {
                CoreResetCommand(cmd) => self.core_reset(cmd).await,
                CoreInitCommand(cmd) => self.core_init(cmd).await,
                CoreSetConfigCommand(cmd) => self.core_set_config(cmd).await,
                CoreGetConfigCommand(cmd) => self.core_get_config(cmd).await,
                CoreConnCreateCommand(cmd) => self.core_conn_create(cmd).await,
                CoreConnCloseCommand(cmd) => self.core_conn_close(cmd).await,
                CoreSetPowerSubStateCommand(cmd) => self.core_set_power_sub_state(cmd).await,
                _ => unimplemented!("unsupported core oid {:?}", packet.get_oid()),
            },
            RfPacket(packet) => match packet.specialize() {
                RfDiscoverMapCommand(cmd) => self.rf_discover_map(cmd).await,
                RfSetListenModeRoutingCommand(cmd) => self.rf_set_listen_mode_routing(cmd).await,
                RfGetListenModeRoutingCommand(cmd) => self.rf_get_listen_mode_routing(cmd).await,
                RfDiscoverCommand(cmd) => self.rf_discover(cmd).await,
                RfDeactivateCommand(cmd) => self.rf_deactivate(cmd).await,
                _ => unimplemented!("unsupported rf oid {:?}", packet.get_oid()),
            },
            NfceePacket(packet) => match packet.specialize() {
                NfceeDiscoverCommand(cmd) => self.nfcee_discover(cmd).await,
                _ => unimplemented!("unsupported nfcee oid {:?}", packet.get_oid()),
            },
            _ => unimplemented!("unsupported gid {:?}", packet.get_gid()),
        }
    }

    async fn receive_data(&mut self, _packet: nci::DataPacket) {
        todo!()
    }

    async fn receive_rf(&mut self, _packet: Vec<u8>) {
        todo!()
    }

    /// Timer handler method. This function is invoked at regular interval
    /// on the NFCC instance and is used to drive internal timers.
    pub async fn tick(&mut self) {}

    /// Main NFCC instance routine.
    pub async fn run(&mut self) -> Result<()> {
        // Send a Reset notification on controller creation corresponding
        // to a power on.
        self.send_control(nci::CoreResetNotificationBuilder {
            trigger: nci::ResetTrigger::PowerOn,
            config_status: nci::ConfigStatus::ConfigReset,
            nci_version: NCI_VERSION,
            manufacturer_id: 0,
            manufacturer_specific_information: vec![],
        })
        .await?;

        // Timer for tick events.
        let mut timer = time::interval(Duration::from_millis(5));

        // Loop waiting for commands or external events.
        loop {
            select! {
                result = self.nci_reader.read() => {
                    let packet = result?;
                    let header = nci::PacketHeader::parse(&packet[0..3])?;
                    match header.get_mt() {
                        nci::MessageType::Data =>
                            self.receive_data(nci::DataPacket::parse(&packet)?).await,
                        nci::MessageType::Command =>
                            self.receive_command(nci::ControlPacket::parse(&packet)?).await?,
                        mt => return Err(anyhow::anyhow!("unexpected message type {:?} in received NCI packet", mt))
                    }
                },
                result = self.rf_rx.recv() => {
                    self.receive_rf(result.ok_or(anyhow::anyhow!("rf_rx channel closed"))?).await
                },
                _ = timer.tick() => self.tick().await
            }
        }
    }
}
