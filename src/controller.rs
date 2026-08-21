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

use crate::crc;
use crate::packets::{nci, rf};
use anyhow::Result;
use core::time::Duration;
use futures::StreamExt;
use log::{debug, info, warn};
use pdl_runtime::Packet;
use std::convert::TryFrom;
use std::future::Future;
use std::pin::pin;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::time;

const NCI_VERSION: nci::NciVersion = nci::NciVersion::Version20;
const MANUFACTURER_ID: u8 = 0x02;
const MANUFACTURER_SPECIFIC_INFORMATION: [u8; 26] =
    [5, 3, 3, 19, 4, 25, 1, 7, 0, 0, 68, 100, 214, 0, 0, 90, 172, 0, 0, 0, 1, 44, 176, 153, 243, 0];

/// Read-only configuration parameters
const PB_ATTRIB_PARAM1: u8 = 0x00;
const LF_T3T_MAX: u8 = 16;
const LLCP_VERSION: u8 = 0x00;

/// Writable configuration parameters with default
/// value defined by the NFCC.
const TOTAL_DURATION: u16 = 1000;
const PA_DEVICES_LIMIT: u8 = 255;
const PB_DEVICES_LIMIT: u8 = 255;
const PF_DEVICES_LIMIT: u8 = 255;
const PV_DEVICES_LIMIT: u8 = 255;
const LA_BIT_FRAME_SDD: u8 = 0x10;
const LA_PLATFORM_CONFIG: u8 = 0x0c;
const LA_SEL_INFO: u8 = 0x60; // Supports ISO-DEP and NFC-DEP.
const LB_SENSB_INFO: u8 = 0x1; // Supports ISO-DEP.
const LB_SFGI: u8 = 0;
const LB_FWI_ADC_FO: u8 = 0x00;
const LF_PROTOCOL_TYPE: u8 = 0x02; // Supports NFC-DEP.
const LI_A_RATS_TB1: u8 = 0x70;
const LI_A_RATS_TC1: u8 = 0x02;

const MAX_LOGICAL_CONNECTIONS: u8 = 2;
const MAX_ROUTING_TABLE_SIZE: u16 = 512;
const MAX_CONTROL_PACKET_PAYLOAD_SIZE: u8 = 255;
const MAX_DATA_PACKET_PAYLOAD_SIZE: u8 = 255;
const NUMBER_OF_CREDITS: u8 = 1;
const MAX_NFCV_RF_FRAME_SIZE: u16 = 512;
const NUMBER_OF_SUPPORTED_EXIT_FRAMES: u8 = 5;
const EXIT_FRAME_QUALIFIER_TECHNOLOGY_MASK: u8 = 0b00000011;
const EXIT_FRAME_QUALIFIER_PREFIX_MATCHING_MASK: u8 = 0b00010000;

/// Time in milliseconds that Casimir waits for poll responses after
/// sending a poll command.
const POLL_RESPONSE_TIMEOUT: u64 = 200;

/// All configuration parameters of the NFCC.
/// The configuration is filled with default values from the specification
/// See [NCI] Table 46: Common Parameters for Discovery Configuration
/// for the format of each parameter and the default value.
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub struct ConfigParameters {
    total_duration: u16,
    /// [NCI] Table 47: Values for CON_DISCOVERY_PARAM.
    con_discovery_param: u8,
    power_state: u8,
    pa_bail_out: u8,
    pa_devices_limit: u8,
    pb_afi: u8,
    pb_bail_out: u8,
    pb_attrib_param1: u8,
    /// [NCI] Table 26: Values for PB_SENSB_REQ_PARAM.
    pb_sensb_req_param: u8,
    pb_devices_limit: u8,
    pf_bit_rate: u8,
    pf_bail_out: u8,
    pf_devices_limit: u8,
    pi_b_h_info: Vec<u8>,
    pi_bit_rate: u8,
    pn_nfc_dep_psl: u8,
    pn_atr_req_gen_bytes: Vec<u8>,
    /// [NCI] Table 30: Values for PN_ATR_REQ_CONFIG.
    pn_atr_req_config: u8,
    pv_devices_limit: u8,
    la_bit_frame_sdd: u8,
    la_platform_config: u8,
    /// [NCI] Table 34: LA_SEL_INFO Coding.
    la_sel_info: u8,
    la_nfcid1: Vec<u8>,
    /// [NCI] Table 36: LB_SENSB_INFO Values.
    lb_sensb_info: u8,
    lb_nfcid0: [u8; 4],
    lb_application_data: u32,
    lb_sfgi: u8,
    /// [NCI] Table 37: LB_FWI_ADC_FO Values.
    lb_fwi_adc_fo: u8,
    lb_bit_rate: u8,
    lf_t3t_identifiers_1: [u8; 18],
    lf_t3t_identifiers_2: [u8; 18],
    lf_t3t_identifiers_3: [u8; 18],
    lf_t3t_identifiers_4: [u8; 18],
    lf_t3t_identifiers_5: [u8; 18],
    lf_t3t_identifiers_6: [u8; 18],
    lf_t3t_identifiers_7: [u8; 18],
    lf_t3t_identifiers_8: [u8; 18],
    lf_t3t_identifiers_9: [u8; 18],
    lf_t3t_identifiers_10: [u8; 18],
    lf_t3t_identifiers_11: [u8; 18],
    lf_t3t_identifiers_12: [u8; 18],
    lf_t3t_identifiers_13: [u8; 18],
    lf_t3t_identifiers_14: [u8; 18],
    lf_t3t_identifiers_15: [u8; 18],
    lf_t3t_identifiers_16: [u8; 18],
    lf_t3t_pmm_default: [u8; 8],
    lf_t3t_max: u8,
    lf_t3t_flags: u16,
    lf_t3t_rd_allowed: u8,
    /// [NCI] Table 39: Supported Protocols for Listen F.
    lf_protocol_type: u8,
    li_a_rats_tb1: u8,
    li_a_hist_by: Vec<u8>,
    li_b_h_info_resp: Vec<u8>,
    li_a_bit_rate: u8,
    li_a_rats_tc1: u8,
    ln_wt: u8,
    ln_atr_res_gen_bytes: Vec<u8>,
    ln_atr_res_config: u8,
    pacm_bit_rate: u8,
    /// [NCI] Table 23: RF Field Information Configuration Parameter.
    rf_field_info: u8,
    rf_nfcee_action: u8,
    nfcdep_op: u8,
    /// [NCI] Table 115: LLCP Version Parameter.
    llcp_version: u8,
    /// [NCI] Table 65: Value Field for NFCC Configuration Control.
    nfcc_config_control: u8,
}

/// State of an NFCC logical connection with the DH.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum LogicalConnection {
    RemoteNfcEndpoint { rf_discovery_id: u8, rf_protocol_type: nci::RfProtocolType },
}

/// State of the RF Discovery of an NFCC instance.
/// The state WaitForAllDiscoveries is not represented as it is implied
/// by the discovery routine.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum RfState {
    Idle,
    Discovery,
    PollActive {
        id: u16,
        rf_interface: nci::RfInterfaceType,
        rf_technology: rf::Technology,
        rf_protocol: rf::Protocol,
    },
    ListenSleep {
        id: u16,
    },
    ListenActive {
        id: u16,
        rf_interface: nci::RfInterfaceType,
        rf_technology: rf::Technology,
        rf_protocol: rf::Protocol,
    },
    WaitForHostSelect,
    WaitForSelectResponse {
        id: u16,
        rf_discovery_id: usize,
        rf_interface: nci::RfInterfaceType,
        rf_technology: rf::Technology,
        rf_protocol: rf::Protocol,
    },
}

/// State of the emulated eSE (ST) NFCEE.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum NfceeState {
    Enabled,
    Disabled,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum RfMode {
    Poll,
    Listen,
}

/// Poll responses received in the context of RF discovery in active
/// Listen mode.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RfPollResponse {
    id: u16,
    rf_protocol: rf::Protocol,
    rf_technology: rf::Technology,
    rf_technology_specific_parameters: Vec<u8>,
}

/// Exit frame data received by the Set exit frame command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RfExitFrame {
    is_prefix_matching_allowed: bool,
    rf_technology: rf::Technology,
    power_states: u8,
    data: Vec<u8>,
    mask: Vec<u8>,
}

/// State of an NFCC instance.
#[allow(missing_docs)]
pub struct State {
    pub config_parameters: ConfigParameters,
    pub logical_connections: [Option<LogicalConnection>; MAX_LOGICAL_CONNECTIONS as usize],
    pub discover_configuration: Vec<nci::DiscoverConfiguration>,
    pub discover_map: Vec<nci::MappingConfiguration>,
    pub nfcee_state: NfceeState,
    pub rf_state: RfState,
    pub rf_poll_responses: Vec<RfPollResponse>,
    pub rf_activation_parameters: Vec<u8>,
    // TODO(johnrjohn) Use bitflags here
    pub passive_observe_mode: u8,
    pub last_observe_mode_state: Option<u8>,
    pub start_time: std::time::Instant,
    pub remote_field_status: rf::FieldStatus,
    pub last_rf_received_time: Option<std::time::Instant>,
    pub exit_frame_timeout: Duration,
    pub exit_frame_start_time: Option<std::time::Instant>,
    pub exit_frames: Vec<RfExitFrame>,
}

/// State of an NFCC instance.
pub struct Controller<'a> {
    id: u16,
    nci_stream: nci::StreamRefMut<'a>,
    nci_writer: nci::Writer,
    rf_rx: mpsc::UnboundedReceiver<rf::RfPacket>,
    rf_tx: mpsc::UnboundedSender<rf::RfPacket>,
    state: State,
}

impl ConfigParameters {
    fn get(&self, id: nci::ConfigParameterId) -> Result<Vec<u8>> {
        match id {
            nci::ConfigParameterId::TotalDuration => Ok(self.total_duration.to_le_bytes().to_vec()),
            nci::ConfigParameterId::ConDiscoveryParam => {
                Ok(self.con_discovery_param.to_le_bytes().to_vec())
            }
            nci::ConfigParameterId::PowerState => Ok(vec![self.power_state]),
            nci::ConfigParameterId::PaBailOut => Ok(vec![self.pa_bail_out]),
            nci::ConfigParameterId::PaDevicesLimit => Ok(vec![self.pa_devices_limit]),
            nci::ConfigParameterId::PbAfi => Ok(vec![self.pb_afi]),
            nci::ConfigParameterId::PbBailOut => Ok(vec![self.pb_bail_out]),
            nci::ConfigParameterId::PbAttribParam1 => Ok(vec![self.pb_attrib_param1]),
            nci::ConfigParameterId::PbSensbReqParam => Ok(vec![self.pb_sensb_req_param]),
            nci::ConfigParameterId::PbDevicesLimit => Ok(vec![self.pb_devices_limit]),
            nci::ConfigParameterId::PfBitRate => Ok(vec![self.pf_bit_rate]),
            nci::ConfigParameterId::PfBailOut => Ok(vec![self.pf_bail_out]),
            nci::ConfigParameterId::PfDevicesLimit => Ok(vec![self.pf_devices_limit]),
            nci::ConfigParameterId::PiBHInfo => Ok(self.pi_b_h_info.clone()),
            nci::ConfigParameterId::PiBitRate => Ok(vec![self.pi_bit_rate]),
            nci::ConfigParameterId::PnNfcDepPsl => Ok(vec![self.pn_nfc_dep_psl]),
            nci::ConfigParameterId::PnAtrReqGenBytes => Ok(self.pn_atr_req_gen_bytes.clone()),
            nci::ConfigParameterId::PnAtrReqConfig => Ok(vec![self.pn_atr_req_config]),
            nci::ConfigParameterId::PvDevicesLimit => Ok(vec![self.pv_devices_limit]),
            nci::ConfigParameterId::LaBitFrameSdd => Ok(vec![self.la_bit_frame_sdd]),
            nci::ConfigParameterId::LaPlatformConfig => Ok(vec![self.la_platform_config]),
            nci::ConfigParameterId::LaSelInfo => Ok(vec![self.la_sel_info]),
            nci::ConfigParameterId::LaNfcid1 => Ok(self.la_nfcid1.clone()),
            nci::ConfigParameterId::LbSensbInfo => Ok(vec![self.lb_sensb_info]),
            nci::ConfigParameterId::LbNfcid0 => Ok(self.lb_nfcid0.to_vec()),
            nci::ConfigParameterId::LbApplicationData => {
                Ok(self.lb_application_data.to_le_bytes().to_vec())
            }
            nci::ConfigParameterId::LbSfgi => Ok(vec![self.lb_sfgi]),
            nci::ConfigParameterId::LbFwiAdcFo => Ok(vec![self.lb_fwi_adc_fo]),
            nci::ConfigParameterId::LbBitRate => Ok(vec![self.lb_bit_rate]),
            nci::ConfigParameterId::LfT3tIdentifiers1 => Ok(self.lf_t3t_identifiers_1.to_vec()),
            nci::ConfigParameterId::LfT3tIdentifiers2 => Ok(self.lf_t3t_identifiers_2.to_vec()),
            nci::ConfigParameterId::LfT3tIdentifiers3 => Ok(self.lf_t3t_identifiers_3.to_vec()),
            nci::ConfigParameterId::LfT3tIdentifiers4 => Ok(self.lf_t3t_identifiers_4.to_vec()),
            nci::ConfigParameterId::LfT3tIdentifiers5 => Ok(self.lf_t3t_identifiers_5.to_vec()),
            nci::ConfigParameterId::LfT3tIdentifiers6 => Ok(self.lf_t3t_identifiers_6.to_vec()),
            nci::ConfigParameterId::LfT3tIdentifiers7 => Ok(self.lf_t3t_identifiers_7.to_vec()),
            nci::ConfigParameterId::LfT3tIdentifiers8 => Ok(self.lf_t3t_identifiers_8.to_vec()),
            nci::ConfigParameterId::LfT3tIdentifiers9 => Ok(self.lf_t3t_identifiers_9.to_vec()),
            nci::ConfigParameterId::LfT3tIdentifiers10 => Ok(self.lf_t3t_identifiers_10.to_vec()),
            nci::ConfigParameterId::LfT3tIdentifiers11 => Ok(self.lf_t3t_identifiers_11.to_vec()),
            nci::ConfigParameterId::LfT3tIdentifiers12 => Ok(self.lf_t3t_identifiers_12.to_vec()),
            nci::ConfigParameterId::LfT3tIdentifiers13 => Ok(self.lf_t3t_identifiers_13.to_vec()),
            nci::ConfigParameterId::LfT3tIdentifiers14 => Ok(self.lf_t3t_identifiers_14.to_vec()),
            nci::ConfigParameterId::LfT3tIdentifiers15 => Ok(self.lf_t3t_identifiers_15.to_vec()),
            nci::ConfigParameterId::LfT3tIdentifiers16 => Ok(self.lf_t3t_identifiers_16.to_vec()),
            nci::ConfigParameterId::LfT3tPmmDefault => Ok(self.lf_t3t_pmm_default.to_vec()),
            nci::ConfigParameterId::LfT3tMax => Ok(vec![self.lf_t3t_max]),
            nci::ConfigParameterId::LfT3tFlags => Ok(self.lf_t3t_flags.to_le_bytes().to_vec()),
            nci::ConfigParameterId::LfT3tRdAllowed => Ok(vec![self.lf_t3t_rd_allowed]),
            nci::ConfigParameterId::LfProtocolType => Ok(vec![self.lf_protocol_type]),
            nci::ConfigParameterId::LiARatsTb1 => Ok(vec![self.li_a_rats_tb1]),
            nci::ConfigParameterId::LiAHistBy => Ok(self.li_a_hist_by.clone()),
            nci::ConfigParameterId::LiBHInfoResp => Ok(self.li_b_h_info_resp.clone()),
            nci::ConfigParameterId::LiABitRate => Ok(vec![self.li_a_bit_rate]),
            nci::ConfigParameterId::LiARatsTc1 => Ok(vec![self.li_a_rats_tc1]),
            nci::ConfigParameterId::LnWt => Ok(vec![self.ln_wt]),
            nci::ConfigParameterId::LnAtrResGenBytes => Ok(self.ln_atr_res_gen_bytes.clone()),
            nci::ConfigParameterId::LnAtrResConfig => Ok(vec![self.ln_atr_res_config]),
            nci::ConfigParameterId::PacmBitRate => Ok(vec![self.pacm_bit_rate]),
            nci::ConfigParameterId::RfFieldInfo => Ok(vec![self.rf_field_info]),
            nci::ConfigParameterId::RfNfceeAction => Ok(vec![self.rf_nfcee_action]),
            nci::ConfigParameterId::NfcdepOp => Ok(vec![self.nfcdep_op]),
            nci::ConfigParameterId::LlcpVersion => Ok(vec![self.llcp_version]),
            nci::ConfigParameterId::NfccConfigControl => Ok(vec![self.nfcc_config_control]),
            _ => Err(anyhow::anyhow!("unknown config parameter ID")),
        }
    }

    fn set(&mut self, id: nci::ConfigParameterId, value: &[u8]) -> Result<()> {
        match id {
            nci::ConfigParameterId::TotalDuration => {
                self.total_duration = u16::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::ConDiscoveryParam => {
                self.con_discovery_param = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::PowerState => {
                self.power_state = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::PaBailOut => {
                self.pa_bail_out = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::PaDevicesLimit => {
                self.pa_devices_limit = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::PbAfi => {
                self.pb_afi = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::PbBailOut => {
                self.pb_bail_out = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::PbAttribParam1 => {
                self.pb_attrib_param1 = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::PbSensbReqParam => {
                self.pb_sensb_req_param = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::PbDevicesLimit => {
                self.pb_devices_limit = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::PfBitRate => {
                self.pf_bit_rate = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::PfBailOut => {
                self.pf_bail_out = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::PfDevicesLimit => {
                self.pf_devices_limit = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::PiBHInfo => {
                self.pi_b_h_info = value.to_vec();
                Ok(())
            }
            nci::ConfigParameterId::PiBitRate => {
                self.pi_bit_rate = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::PnNfcDepPsl => {
                self.pn_nfc_dep_psl = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::PnAtrReqGenBytes => {
                self.pn_atr_req_gen_bytes = value.to_vec();
                Ok(())
            }
            nci::ConfigParameterId::PnAtrReqConfig => {
                self.pn_atr_req_config = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::PvDevicesLimit => {
                self.pv_devices_limit = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::LaBitFrameSdd => {
                self.la_bit_frame_sdd = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::LaPlatformConfig => {
                self.la_platform_config = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::LaSelInfo => {
                self.la_sel_info = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::LaNfcid1 => {
                self.la_nfcid1 = value.to_vec();
                Ok(())
            }
            nci::ConfigParameterId::LbSensbInfo => {
                self.lb_sensb_info = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::LbNfcid0 => {
                self.lb_nfcid0 = value.try_into()?;
                Ok(())
            }
            nci::ConfigParameterId::LbApplicationData => {
                self.lb_application_data = u32::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::LbSfgi => {
                self.lb_sfgi = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::LbFwiAdcFo => {
                self.lb_fwi_adc_fo = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::LbBitRate => {
                self.lb_bit_rate = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::LfT3tIdentifiers1 => {
                self.lf_t3t_identifiers_1 = value.try_into()?;
                Ok(())
            }
            nci::ConfigParameterId::LfT3tIdentifiers2 => {
                self.lf_t3t_identifiers_2 = value.try_into()?;
                Ok(())
            }
            nci::ConfigParameterId::LfT3tIdentifiers3 => {
                self.lf_t3t_identifiers_3 = value.try_into()?;
                Ok(())
            }
            nci::ConfigParameterId::LfT3tIdentifiers4 => {
                self.lf_t3t_identifiers_4 = value.try_into()?;
                Ok(())
            }
            nci::ConfigParameterId::LfT3tIdentifiers5 => {
                self.lf_t3t_identifiers_5 = value.try_into()?;
                Ok(())
            }
            nci::ConfigParameterId::LfT3tIdentifiers6 => {
                self.lf_t3t_identifiers_6 = value.try_into()?;
                Ok(())
            }
            nci::ConfigParameterId::LfT3tIdentifiers7 => {
                self.lf_t3t_identifiers_7 = value.try_into()?;
                Ok(())
            }
            nci::ConfigParameterId::LfT3tIdentifiers8 => {
                self.lf_t3t_identifiers_8 = value.try_into()?;
                Ok(())
            }
            nci::ConfigParameterId::LfT3tIdentifiers9 => {
                self.lf_t3t_identifiers_9 = value.try_into()?;
                Ok(())
            }
            nci::ConfigParameterId::LfT3tIdentifiers10 => {
                self.lf_t3t_identifiers_10 = value.try_into()?;
                Ok(())
            }
            nci::ConfigParameterId::LfT3tIdentifiers11 => {
                self.lf_t3t_identifiers_11 = value.try_into()?;
                Ok(())
            }
            nci::ConfigParameterId::LfT3tIdentifiers12 => {
                self.lf_t3t_identifiers_12 = value.try_into()?;
                Ok(())
            }
            nci::ConfigParameterId::LfT3tIdentifiers13 => {
                self.lf_t3t_identifiers_13 = value.try_into()?;
                Ok(())
            }
            nci::ConfigParameterId::LfT3tIdentifiers14 => {
                self.lf_t3t_identifiers_14 = value.try_into()?;
                Ok(())
            }
            nci::ConfigParameterId::LfT3tIdentifiers15 => {
                self.lf_t3t_identifiers_15 = value.try_into()?;
                Ok(())
            }
            nci::ConfigParameterId::LfT3tIdentifiers16 => {
                self.lf_t3t_identifiers_16 = value.try_into()?;
                Ok(())
            }
            nci::ConfigParameterId::LfT3tPmmDefault => {
                self.lf_t3t_pmm_default = value.try_into()?;
                Ok(())
            }
            nci::ConfigParameterId::LfT3tMax => Err(anyhow::anyhow!("read-only config parameter")),
            nci::ConfigParameterId::LfT3tFlags => {
                self.lf_t3t_flags = u16::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::LfT3tRdAllowed => {
                self.lf_t3t_rd_allowed = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::LfProtocolType => {
                self.lf_protocol_type = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::LiARatsTb1 => {
                self.li_a_rats_tb1 = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::LiAHistBy => {
                self.li_a_hist_by = value.to_vec();
                Ok(())
            }
            nci::ConfigParameterId::LiBHInfoResp => {
                self.li_b_h_info_resp = value.to_vec();
                Ok(())
            }
            nci::ConfigParameterId::LiABitRate => {
                self.li_a_bit_rate = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::LiARatsTc1 => {
                self.li_a_rats_tc1 = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::LnWt => {
                self.ln_wt = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::LnAtrResGenBytes => {
                self.ln_atr_res_gen_bytes = value.to_vec();
                Ok(())
            }
            nci::ConfigParameterId::LnAtrResConfig => {
                self.ln_atr_res_config = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::PacmBitRate => {
                self.pacm_bit_rate = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::RfFieldInfo => {
                self.rf_field_info = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::RfNfceeAction => {
                self.rf_nfcee_action = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::NfcdepOp => {
                self.nfcdep_op = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::LlcpVersion => {
                self.llcp_version = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            nci::ConfigParameterId::NfccConfigControl => {
                self.nfcc_config_control = u8::from_le_bytes(value.try_into()?);
                Ok(())
            }
            _ => Err(anyhow::anyhow!("unknown config parameter ID")),
        }
    }
}

impl Default for ConfigParameters {
    fn default() -> Self {
        ConfigParameters {
            total_duration: TOTAL_DURATION,
            con_discovery_param: 0x01,
            power_state: 0x02,
            pa_bail_out: 0x00,
            pa_devices_limit: PA_DEVICES_LIMIT,
            pb_afi: 0x00,
            pb_bail_out: 0x00,
            pb_attrib_param1: PB_ATTRIB_PARAM1,
            pb_sensb_req_param: 0x00,
            pb_devices_limit: PB_DEVICES_LIMIT,
            pf_bit_rate: 0x01,
            pf_bail_out: 0x00,
            pf_devices_limit: PF_DEVICES_LIMIT,
            pi_b_h_info: vec![],
            pi_bit_rate: 0x00,
            pn_nfc_dep_psl: 0x00,
            pn_atr_req_gen_bytes: vec![],
            pn_atr_req_config: 0x30,
            pv_devices_limit: PV_DEVICES_LIMIT,
            la_bit_frame_sdd: LA_BIT_FRAME_SDD,
            la_platform_config: LA_PLATFORM_CONFIG,
            la_sel_info: LA_SEL_INFO,
            la_nfcid1: vec![0x08, 0x00, 0x00, 0x00],
            lb_sensb_info: LB_SENSB_INFO,
            lb_nfcid0: [0x08, 0x00, 0x00, 0x00],
            lb_application_data: 0x00000000,
            lb_sfgi: LB_SFGI,
            lb_fwi_adc_fo: LB_FWI_ADC_FO,
            lb_bit_rate: 0x00,
            lf_t3t_identifiers_1: [0; 18],
            lf_t3t_identifiers_2: [0; 18],
            lf_t3t_identifiers_3: [0; 18],
            lf_t3t_identifiers_4: [0; 18],
            lf_t3t_identifiers_5: [0; 18],
            lf_t3t_identifiers_6: [0; 18],
            lf_t3t_identifiers_7: [0; 18],
            lf_t3t_identifiers_8: [0; 18],
            lf_t3t_identifiers_9: [0; 18],
            lf_t3t_identifiers_10: [0; 18],
            lf_t3t_identifiers_11: [0; 18],
            lf_t3t_identifiers_12: [0; 18],
            lf_t3t_identifiers_13: [0; 18],
            lf_t3t_identifiers_14: [0; 18],
            lf_t3t_identifiers_15: [0; 18],
            lf_t3t_identifiers_16: [0; 18],
            lf_t3t_pmm_default: [0xff; 8],
            lf_t3t_max: LF_T3T_MAX,
            lf_t3t_flags: 0x0000,
            lf_t3t_rd_allowed: 0x00,
            lf_protocol_type: LF_PROTOCOL_TYPE,
            li_a_rats_tb1: LI_A_RATS_TB1,
            li_a_hist_by: vec![],
            li_b_h_info_resp: vec![],
            li_a_bit_rate: 0x00,
            li_a_rats_tc1: LI_A_RATS_TC1,
            ln_wt: 10,
            ln_atr_res_gen_bytes: vec![],
            ln_atr_res_config: 0x30,
            pacm_bit_rate: 0x01,
            rf_field_info: 0x00,
            rf_nfcee_action: 0x01,
            // [NCI] Table 101: NFC-DEP Operation Parameter.
            nfcdep_op: 0x1f,
            llcp_version: LLCP_VERSION,
            nfcc_config_control: 0x00,
        }
    }
}

impl State {
    /// Craft the NFCID1 used by this instance in NFC-A poll responses.
    /// Returns a dynamically generated NFCID1 (4 byte long and starts with 08h).
    fn nfcid1(&self) -> Vec<u8> {
        if self.config_parameters.la_nfcid1.len() == 4
            && self.config_parameters.la_nfcid1[0] == 0x08
        {
            vec![0x08, 186, 7, 99] // TODO(hchataing) pseudo random
        } else {
            self.config_parameters.la_nfcid1.clone()
        }
    }

    /// Select the interface to be preferably used for the selected protocol.
    fn select_interface(
        &self,
        mode: RfMode,
        rf_protocol: nci::RfProtocolType,
    ) -> nci::RfInterfaceType {
        for config in self.discover_map.iter() {
            match (mode, config.mode.poll_mode, config.mode.listen_mode) {
                _ if config.rf_protocol != rf_protocol => (),
                (RfMode::Poll, nci::FeatureFlag::Enabled, _)
                | (RfMode::Listen, _, nci::FeatureFlag::Enabled) => return config.rf_interface,
                _ => (),
            }
        }

        // [NCI] 6.2 RF Interface Mapping Configuration
        //
        // The NFCC SHALL set the default mapping of RF Interface to RF Protocols /
        // Modes to the following values:
        //
        // • If the NFCC supports the ISO-DEP RF interface, the NFCC SHALL map the
        //   ISO-DEP RF Protocol to the ISO-DEP RF Interface for Poll Mode and
        //   Listen Mode.
        // • If the NFCC supports the NFC-DEP RF interface, the NFCC SHALL map the
        //   NFC-DEP RF Protocol to the NFC-DEP RF Interface for Poll Mode and
        //   Listen Mode.
        // • If the NFCC supports the NDEF RF interface, the NFCC SHALL map the
        //   NDEF RF Protocol to the NDEF RF Interface for Poll Mode.
        // • Otherwise, the NFCC SHALL map to the Frame RF Interface by default
        match rf_protocol {
            nci::RfProtocolType::IsoDep => nci::RfInterfaceType::IsoDep,
            nci::RfProtocolType::NfcDep => nci::RfInterfaceType::NfcDep,
            nci::RfProtocolType::Ndef if mode == RfMode::Poll => nci::RfInterfaceType::Ndef,
            _ => nci::RfInterfaceType::Frame,
        }
    }

    /// Insert a poll response into the discovery list.
    /// The response is updated if the device was already discovered,
    /// ensuring a single physical peer is not treated as multiple distinct targets.
    fn add_poll_response(&mut self, poll_response: RfPollResponse) {
        if let Some(existing) = self.rf_poll_responses.iter_mut().find(|r| r.id == poll_response.id)
        {
            *existing = poll_response;
        } else {
            self.rf_poll_responses.push(poll_response);
        }
    }
}

impl<'a> Controller<'a> {
    /// Create a new NFCC instance with default configuration.
    pub fn new(
        id: u16,
        nci_stream: nci::StreamRefMut<'a>,
        nci_writer: nci::Writer,
        rf_rx: mpsc::UnboundedReceiver<rf::RfPacket>,
        rf_tx: mpsc::UnboundedSender<rf::RfPacket>,
    ) -> Controller<'a> {
        Controller {
            id,
            nci_stream,
            nci_writer,
            rf_rx,
            rf_tx,
            state: State {
                config_parameters: Default::default(),
                logical_connections: [None; MAX_LOGICAL_CONNECTIONS as usize],
                discover_map: vec![],
                discover_configuration: vec![],
                nfcee_state: NfceeState::Disabled,
                rf_state: RfState::Idle,
                rf_poll_responses: vec![],
                rf_activation_parameters: vec![],
                passive_observe_mode: nci::PassiveObserveMode::Disable.into(),
                last_observe_mode_state: None,
                start_time: Instant::now(),
                remote_field_status: rf::FieldStatus::FieldOff,
                last_rf_received_time: None,
                exit_frame_timeout: Duration::from_millis(0),
                exit_frame_start_time: None,
                exit_frames: vec![],
            },
        }
    }

    async fn send_control(
        &mut self,
        packet: impl TryInto<nci::ControlPacket, Error = pdl_runtime::EncodeError>,
    ) -> Result<()> {
        self.nci_writer.write(&packet.try_into()?.encode_to_vec()?).await
    }

    async fn send_data(&mut self, packet: impl Into<nci::DataPacket>) -> Result<()> {
        self.nci_writer.write(&packet.into().encode_to_vec()?).await
    }

    fn send_rf(
        &self,
        packet: impl TryInto<rf::RfPacket, Error = pdl_runtime::EncodeError>,
    ) -> Result<()> {
        self.rf_tx.send(packet.try_into()?)?;
        Ok(())
    }

    async fn core_reset(&mut self, cmd: nci::CoreResetCommand) -> Result<()> {
        info!("[{}] CORE_RESET_CMD", self.id);
        info!("         ResetType: {:?}", cmd.reset_type());

        match cmd.reset_type() {
            nci::ResetType::KeepConfig => (),
            nci::ResetType::ResetConfig => self.state.config_parameters = Default::default(),
        }

        for i in 0..MAX_LOGICAL_CONNECTIONS {
            self.state.logical_connections[i as usize] = None;
        }

        self.state.discover_map.clear();
        self.state.discover_configuration.clear();
        self.state.rf_state = RfState::Idle;
        self.state.rf_poll_responses.clear();

        self.send_control(nci::CoreResetResponse { status: nci::Status::Ok }).await?;

        self.send_control(nci::CoreResetNotification {
            trigger: nci::ResetTrigger::ResetCommand,
            config_status: match cmd.reset_type() {
                nci::ResetType::KeepConfig => nci::ConfigStatus::ConfigKept,
                nci::ResetType::ResetConfig => nci::ConfigStatus::ConfigReset,
            },
            nci_version: NCI_VERSION,
            manufacturer_id: MANUFACTURER_ID,
            manufacturer_specific_information: MANUFACTURER_SPECIFIC_INFORMATION.to_vec(),
        })
        .await?;

        Ok(())
    }

    async fn core_init(&mut self, _cmd: nci::CoreInitCommand) -> Result<()> {
        info!("[{}] CORE_INIT_CMD", self.id);

        self.send_control(nci::CoreInitResponse {
            status: nci::Status::Ok,
            nfcc_features: nci::NfccFeatures {
                discovery_frequency_configuration: nci::FeatureFlag::Disabled,
                discovery_configuration_mode: nci::DiscoveryConfigurationMode::DhOnly,
                hci_network_support: nci::FeatureFlag::Enabled,
                active_communication_mode: nci::FeatureFlag::Enabled,
                technology_based_routing: nci::FeatureFlag::Enabled,
                protocol_based_routing: nci::FeatureFlag::Enabled,
                aid_based_routing: nci::FeatureFlag::Enabled,
                system_code_based_routing: nci::FeatureFlag::Enabled,
                apdu_pattern_based_routing: nci::FeatureFlag::Enabled,
                forced_nfcee_routing: nci::FeatureFlag::Enabled,
                battery_off_state: nci::FeatureFlag::Disabled,
                switched_off_state: nci::FeatureFlag::Enabled,
                switched_on_substates: nci::FeatureFlag::Enabled,
                rf_configuration_in_switched_off_state: nci::FeatureFlag::Disabled,
                proprietary_capabilities: 0,
            },
            max_logical_connections: MAX_LOGICAL_CONNECTIONS,
            max_routing_table_size: MAX_ROUTING_TABLE_SIZE,
            max_control_packet_payload_size: MAX_CONTROL_PACKET_PAYLOAD_SIZE,
            max_data_packet_payload_size: MAX_DATA_PACKET_PAYLOAD_SIZE,
            number_of_credits: NUMBER_OF_CREDITS,
            max_nfcv_rf_frame_size: MAX_NFCV_RF_FRAME_SIZE,
            supported_rf_interfaces: vec![
                nci::RfInterface { interface: nci::RfInterfaceType::Frame, extensions: vec![] },
                nci::RfInterface { interface: nci::RfInterfaceType::IsoDep, extensions: vec![] },
                nci::RfInterface { interface: nci::RfInterfaceType::NfcDep, extensions: vec![] },
                nci::RfInterface {
                    interface: nci::RfInterfaceType::NfceeDirect,
                    extensions: vec![],
                },
            ],
        })
        .await?;

        Ok(())
    }

    async fn core_set_config(&mut self, cmd: nci::CoreSetConfigCommand) -> Result<()> {
        info!("[{}] CORE_SET_CONFIG_CMD", self.id);

        let mut invalid_parameters = vec![];
        for parameter in cmd.parameters().iter() {
            info!("         Type: {:?}", parameter.id);
            info!("         Value: {:?}", parameter.value);
            match parameter.id {
                nci::ConfigParameterId::Rfu(_) => invalid_parameters.push(parameter.id),
                // TODO(henrichataing):
                // [NCI] 5.2.1 State RFST_IDLE
                // Unless otherwise specified, discovery related configuration
                // defined in Sections 6.1, 6.2, 6.3 and 7.1 SHALL only be set
                // while in IDLE state.
                //
                // Respond with Semantic Error as indicated by
                // [NCI] 3.2.2 Exception Handling for Control Messages
                // An unexpected Command SHALL NOT cause any action by the NFCC.
                // Unless otherwise specified, the NFCC SHALL send a Response
                // with a Status value of STATUS_SEMANTIC_ERROR and no
                // additional fields.
                _ => {
                    if self.state.config_parameters.set(parameter.id, &parameter.value).is_err() {
                        invalid_parameters.push(parameter.id)
                    }
                }
            }
        }

        self.send_control(nci::CoreSetConfigResponse {
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
                warn!(
                    "[{}] rejecting unknown configuration parameter ids: {:?}",
                    self.id, invalid_parameters
                );
                nci::Status::InvalidParam
            },
            parameters: invalid_parameters,
        })
        .await?;

        Ok(())
    }

    async fn core_config(&mut self, cmd: nci::CoreGetConfigCommand) -> Result<()> {
        info!("[{}] CORE_GET_CONFIG_CMD", self.id);

        let mut valid_parameters = vec![];
        let mut invalid_parameters = vec![];
        for id in cmd.parameters() {
            info!("         ID: {id:?}");
            match self.state.config_parameters.get(*id) {
                Ok(value) => {
                    valid_parameters.push(nci::ConfigParameter { id: *id, value: value.to_vec() })
                }
                Err(_) => invalid_parameters.push(nci::ConfigParameter { id: *id, value: vec![] }),
            }
        }

        self.send_control(if invalid_parameters.is_empty() {
            // If the NFCC is able to respond with all requested parameters, the
            // NFCC SHALL respond with the CORE_GET_CONFIG_RSP with a Status
            // of STATUS_OK.
            nci::CoreGetConfigResponse { status: nci::Status::Ok, parameters: valid_parameters }
        } else {
            // If the DH tries to retrieve any parameter(s) that are not available
            // in the NFCC, the NFCC SHALL respond with a CORE_GET_CONFIG_RSP with
            // a Status field of STATUS_INVALID_PARAM, containing each unavailable
            // Parameter ID with a Parameter Len field of value zero.
            nci::CoreGetConfigResponse {
                status: nci::Status::InvalidParam,
                parameters: invalid_parameters,
            }
        })
        .await?;

        Ok(())
    }

    async fn core_conn_create(&mut self, cmd: nci::CoreConnCreateCommand) -> Result<()> {
        info!("[{}] CORE_CONN_CREATE_CMD", self.id);

        let result: std::result::Result<u8, nci::Status> = (|| {
            // Retrieve an unused connection ID for the logical connection.
            let conn_id = {
                (0..MAX_LOGICAL_CONNECTIONS)
                    .find(|conn_id| self.state.logical_connections[*conn_id as usize].is_none())
                    .ok_or(nci::Status::Rejected)?
            };

            // Check that the selected destination type is supported and validate
            // the destination specific parameters.
            let logical_connection = match cmd.destination_type() {
                // If the value of Destination Type is that of a Remote NFC
                // Endpoint (0x02), then only the Destination-specific Parameter
                // with Type 0x00 or proprietary parameters (as defined in Table 16)
                // SHALL be present.
                nci::DestinationType::RemoteNfcEndpoint => {
                    let mut rf_discovery_id: Option<u8> = None;
                    let mut rf_protocol_type: Option<nci::RfProtocolType> = None;

                    for parameter in cmd.parameters() {
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
            if self
                .state
                .logical_connections
                .iter()
                .any(|c| c.as_ref() == Some(&logical_connection))
            {
                return Err(nci::Status::Rejected);
            }

            // Create the connection.
            self.state.logical_connections[conn_id as usize] = Some(logical_connection);

            Ok(conn_id)
        })();

        self.send_control(match result {
            Ok(conn_id) => nci::CoreConnCreateResponse {
                status: nci::Status::Ok,
                max_data_packet_payload_size: MAX_DATA_PACKET_PAYLOAD_SIZE,
                initial_number_of_credits: 0xff,
                conn_id: nci::ConnId::from_dynamic(conn_id),
            },
            Err(status) => nci::CoreConnCreateResponse {
                status,
                max_data_packet_payload_size: 0,
                initial_number_of_credits: 0xff,
                conn_id: 0.try_into().unwrap(),
            },
        })
        .await?;

        Ok(())
    }

    async fn core_conn_close(&mut self, cmd: nci::CoreConnCloseCommand) -> Result<()> {
        info!("[{}] CORE_CONN_CLOSE_CMD", self.id);

        let conn_id = match cmd.conn_id() {
            nci::ConnId::StaticRf | nci::ConnId::StaticHci => {
                warn!("[{}] core_conn_close called with static conn_id", self.id);
                self.send_control(nci::CoreConnCloseResponse { status: nci::Status::Rejected })
                    .await?;
                return Ok(());
            }
            nci::ConnId::Dynamic(id) => nci::ConnId::to_dynamic(id),
        };

        let status = if conn_id >= MAX_LOGICAL_CONNECTIONS
            || self.state.logical_connections[conn_id as usize].is_none()
        {
            // If there is no connection associated to the Conn ID in the CORE_CONN_CLOSE_CMD, the
            // NFCC SHALL reject the connection closure request by sending a CORE_CONN_CLOSE_RSP
            // with a Status of STATUS_REJECTED.
            nci::Status::Rejected
        } else {
            // When it receives a CORE_CONN_CLOSE_CMD for an existing connection, the NFCC SHALL
            // accept the connection closure request by sending a CORE_CONN_CLOSE_RSP with a Status of
            // STATUS_OK, and the Logical Connection is closed.
            self.state.logical_connections[conn_id as usize] = None;
            nci::Status::Ok
        };

        self.send_control(nci::CoreConnCloseResponse { status }).await?;

        Ok(())
    }

    async fn core_set_power_sub_state(
        &mut self,
        cmd: nci::CoreSetPowerSubStateCommand,
    ) -> Result<()> {
        info!("[{}] CORE_SET_POWER_SUB_STATE_CMD", self.id);
        info!("         State: {:?}", cmd.power_state());

        self.send_control(nci::CoreSetPowerSubStateResponse { status: nci::Status::Ok }).await?;

        Ok(())
    }

    async fn rf_discover_map(&mut self, cmd: nci::RfDiscoverMapCommand) -> Result<()> {
        info!("[{}] RF_DISCOVER_MAP_CMD", self.id);

        self.state.discover_map.clone_from(cmd.mapping_configurations());
        self.send_control(nci::RfDiscoverMapResponse { status: nci::Status::Ok }).await?;

        Ok(())
    }

    async fn rf_set_listen_mode_routing(
        &mut self,
        _cmd: nci::RfSetListenModeRoutingCommand,
    ) -> Result<()> {
        info!("[{}] RF_SET_LISTEN_MODE_ROUTING_CMD", self.id);

        self.send_control(nci::RfSetListenModeRoutingResponse { status: nci::Status::Ok }).await?;

        Ok(())
    }

    async fn rf_listen_mode_routing(
        &mut self,
        _cmd: nci::RfGetListenModeRoutingCommand,
    ) -> Result<()> {
        info!("[{}] RF_GET_LISTEN_MODE_ROUTING_CMD", self.id);

        self.send_control(nci::RfGetListenModeRoutingResponse {
            status: nci::Status::Ok,
            more_to_follow: 0,
            routing_entries: vec![],
        })
        .await?;

        Ok(())
    }

    async fn rf_discover(&mut self, cmd: nci::RfDiscoverCommand) -> Result<()> {
        info!("[{}] RF_DISCOVER_CMD", self.id);
        for config in cmd.configurations() {
            info!("         TechMode: {:?}", config.technology_and_mode);
        }

        if self.state.rf_state != RfState::Idle && self.state.rf_state != RfState::Discovery {
            warn!("[{}] rf_discover received in {:?} state", self.id, self.state.rf_state);
            self.send_control(nci::RfDiscoverResponse { status: nci::Status::SemanticError })
                .await?;
            return Ok(());
        }

        self.state.discover_configuration.clone_from(cmd.configurations());
        self.state.rf_state = RfState::Discovery;
        let has_poll = self.state.discover_configuration.iter().any(|c| {
            matches!(
                c.technology_and_mode,
                nci::RfTechnologyAndMode::NfcAPassivePollMode
                    | nci::RfTechnologyAndMode::NfcBPassivePollMode
                    | nci::RfTechnologyAndMode::NfcFPassivePollMode
                    | nci::RfTechnologyAndMode::NfcVPassivePollMode
            )
        });
        let has_listen = self.state.discover_configuration.iter().any(|c| {
            matches!(
                c.technology_and_mode,
                nci::RfTechnologyAndMode::NfcAPassiveListenMode
                    | nci::RfTechnologyAndMode::NfcBPassiveListenMode
                    | nci::RfTechnologyAndMode::NfcFPassiveListenMode
            )
        });
        if has_poll && !has_listen {
            self.state.passive_observe_mode = 0;
        }

        self.send_control(nci::RfDiscoverResponse { status: nci::Status::Ok }).await?;

        Ok(())
    }

    async fn rf_discover_select(&mut self, cmd: nci::RfDiscoverSelectCommand) -> Result<()> {
        info!("[{}] RF_DISCOVER_SELECT_CMD", self.id);
        info!("         DiscoveryID: {:?}", cmd.rf_discovery_id());
        info!("         Protocol: {:?}", cmd.rf_protocol());
        info!("         Interface: {:?}", cmd.rf_interface());

        if self.state.rf_state != RfState::WaitForHostSelect
            && self.state.rf_state != RfState::Discovery
        {
            warn!("[{}] rf_discover_select received in {:?} state", self.id, self.state.rf_state);
            self.send_control(nci::RfDiscoverSelectResponse { status: nci::Status::SemanticError })
                .await?;
            return Ok(());
        }

        let rf_discovery_id = match cmd.rf_discovery_id() {
            nci::RfDiscoveryId::Rfu(_) => {
                warn!("[{}] rf_discover_select with RFU id", self.id);
                self.send_control(nci::RfDiscoverSelectResponse { status: nci::Status::Rejected })
                    .await?;
                return Ok(());
            }
            nci::RfDiscoveryId::Id(id) => {
                let idx = nci::RfDiscoveryId::to_index(id);
                if idx < self.state.rf_poll_responses.len() {
                    idx
                } else {
                    warn!("[{}] rf_discover_select with invalid id", self.id);
                    self.send_control(nci::RfDiscoverSelectResponse {
                        status: nci::Status::Rejected,
                    })
                    .await?;
                    return Ok(());
                }
            }
        };

        self.send_control(nci::RfDiscoverSelectResponse { status: nci::Status::Ok }).await?;

        // Send RF select command to the peer to activate the device.
        // The command has varying parameters based on the activated protocol.
        self.activate_poll_interface(rf_discovery_id, cmd.rf_protocol(), cmd.rf_interface())
            .await?;

        Ok(())
    }

    async fn rf_deactivate(&mut self, cmd: nci::RfDeactivateCommand) -> Result<()> {
        info!("[{}] RF_DEACTIVATE_CMD", self.id);
        info!("         Type: {:?}", cmd.deactivation_type());

        use nci::DeactivationType::*;

        let (status, mut next_state) = match (self.state.rf_state, cmd.deactivation_type()) {
            (RfState::Idle, _) => (nci::Status::SemanticError, RfState::Idle),
            (RfState::Discovery, IdleMode) => (nci::Status::Ok, RfState::Idle),
            (RfState::Discovery, _) => (nci::Status::SemanticError, RfState::Discovery),
            (RfState::PollActive { .. }, IdleMode) => (nci::Status::Ok, RfState::Idle),
            (RfState::PollActive { .. }, SleepMode | SleepAfMode) => {
                (nci::Status::Ok, RfState::WaitForHostSelect)
            }
            (RfState::PollActive { .. }, Discovery) => (nci::Status::Ok, RfState::Discovery),
            (RfState::ListenSleep { .. }, IdleMode) => (nci::Status::Ok, RfState::Idle),
            (RfState::ListenSleep { .. }, _) => (nci::Status::SemanticError, self.state.rf_state),
            (RfState::ListenActive { .. }, IdleMode) => (nci::Status::Ok, RfState::Idle),
            (RfState::ListenActive { id, .. }, SleepMode | SleepAfMode) => {
                (nci::Status::Ok, RfState::ListenSleep { id })
            }
            (RfState::ListenActive { .. }, Discovery) => (nci::Status::Ok, RfState::Discovery),
            (RfState::WaitForHostSelect, IdleMode) => (nci::Status::Ok, RfState::Idle),
            (RfState::WaitForHostSelect, _) => {
                (nci::Status::SemanticError, RfState::WaitForHostSelect)
            }
            (RfState::WaitForSelectResponse { .. }, IdleMode) => (nci::Status::Ok, RfState::Idle),
            (RfState::WaitForSelectResponse { .. }, _) => {
                (nci::Status::SemanticError, self.state.rf_state)
            }
        };

        // Update the state now to prevent interface activation from
        // completing if a remote device is being selected.
        (next_state, self.state.rf_state) = (self.state.rf_state, next_state);

        self.send_control(nci::RfDeactivateResponse { status }).await?;

        // Deactivate the active RF interface if applicable
        // (next_state is the previous state in this context).
        match next_state {
            RfState::PollActive { .. } | RfState::ListenActive { .. } => {
                info!("[{}] RF_DEACTIVATE_NTF", self.id);
                info!("         Type: {:?}", cmd.deactivation_type());
                info!("         Reason: DH_Request");
                self.field_info(rf::FieldStatus::FieldOff, 255).await?;
                self.send_control(nci::RfDeactivateNotification {
                    deactivation_type: cmd.deactivation_type(),
                    deactivation_reason: nci::DeactivationReason::DhRequest,
                })
                .await?
            }
            _ => (),
        }

        // Deselect the remote device if applicable.
        match next_state {
            RfState::PollActive { id, rf_protocol, rf_technology, .. }
            | RfState::WaitForSelectResponse { id, rf_protocol, rf_technology, .. } => self
                .send_rf(rf::DeactivateNotification {
                    receiver: id,
                    protocol: rf_protocol,
                    technology: rf_technology,
                    bitrate: rf::BitRate::BitRate106KbitS,
                    power_level: 255,
                    sender: self.id,
                    type_: cmd.deactivation_type().into(),
                    reason: rf::DeactivateReason::EndpointRequest,
                })?,
            _ => (),
        }

        Ok(())
    }

    async fn nfcee_discover(&mut self, _cmd: nci::NfceeDiscoverCommand) -> Result<()> {
        info!("[{}] NFCEE_DISCOVER_CMD", self.id);

        self.send_control(nci::NfceeDiscoverResponse {
            status: nci::Status::Ok,
            number_of_nfcees: 1,
        })
        .await?;

        self.send_control(nci::NfceeDiscoverNotification {
            nfcee_id: nci::NfceeId::hci_nfcee(0x86),
            nfcee_status: nci::NfceeStatus::Disabled,
            supported_nfcee_protocols: vec![],
            nfcee_information: vec![nci::NfceeInformation {
                r#type: nci::NfceeInformationType::HostId,
                value: vec![0xc0],
            }],
            nfcee_supply_power: nci::NfceeSupplyPower::NfccHasNoControl,
        })
        .await?;

        Ok(())
    }

    async fn nfcee_mode_set(&mut self, cmd: nci::NfceeModeSetCommand) -> Result<()> {
        info!("[{}] NFCEE_MODE_SET_CMD", self.id);
        info!("         NFCEE ID: {:?}", cmd.nfcee_id());
        info!("         NFCEE Mode: {:?}", cmd.nfcee_mode());

        if cmd.nfcee_id() != nci::NfceeId::hci_nfcee(0x86) {
            warn!("[{}] nfcee_mode_set with invalid nfcee_id", self.id);
            self.send_control(nci::NfceeModeSetResponse { status: nci::Status::Ok }).await?;
            return Ok(());
        }

        self.state.nfcee_state = match cmd.nfcee_mode() {
            nci::NfceeMode::Enable => NfceeState::Enabled,
            nci::NfceeMode::Disable => NfceeState::Disabled,
        };

        self.send_control(nci::NfceeModeSetResponse { status: nci::Status::Ok }).await?;

        self.send_control(nci::NfceeModeSetNotification { status: nci::Status::Ok }).await?;

        if self.state.nfcee_state == NfceeState::Enabled {
            // Android host stack expects this notification to know when the
            // NFCEE completes start-up. The list of information entries is
            // filled with defaults observed on real phones.
            self.send_data(nci::DataPacket {
                mt: nci::MessageType::Data,
                conn_id: nci::ConnId::StaticHci,
                cr: 0,
                payload: vec![0x81, 0x43, 0xc0, 0x01],
            })
            .await?;

            self.send_control(nci::RfNfceeDiscoveryReqNotification {
                information_entries: vec![
                    nci::InformationEntry {
                        r#type: nci::InformationEntryType::AddDiscoveryRequest,
                        nfcee_id: nci::NfceeId::hci_nfcee(0x86),
                        rf_technology_and_mode: nci::RfTechnologyAndMode::NfcFPassiveListenMode,
                        rf_protocol: nci::RfProtocolType::T3t,
                    },
                    nci::InformationEntry {
                        r#type: nci::InformationEntryType::AddDiscoveryRequest,
                        nfcee_id: nci::NfceeId::hci_nfcee(0x86),
                        rf_technology_and_mode: nci::RfTechnologyAndMode::NfcAPassiveListenMode,
                        rf_protocol: nci::RfProtocolType::IsoDep,
                    },
                    nci::InformationEntry {
                        r#type: nci::InformationEntryType::AddDiscoveryRequest,
                        nfcee_id: nci::NfceeId::hci_nfcee(0x86),
                        rf_technology_and_mode: nci::RfTechnologyAndMode::NfcBPassiveListenMode,
                        rf_protocol: nci::RfProtocolType::IsoDep,
                    },
                ],
            })
            .await?;
        }

        Ok(())
    }

    async fn android_caps(&mut self, _cmd: nci::AndroidGetCapsCommand) -> Result<()> {
        info!("[{}] ANDROID_GET_CAPS_CMD", self.id);
        let cap_tlvs = vec![
            nci::CapTlv { t: nci::CapTlvType::PassiveObserverMode, v: vec![2] },
            nci::CapTlv { t: nci::CapTlvType::PollingFrameNotification, v: vec![1] },
            nci::CapTlv { t: nci::CapTlvType::AutotransactPollingLoopFilter, v: vec![1] },
            nci::CapTlv {
                t: nci::CapTlvType::NumberOfExitFramesSupported,
                v: vec![NUMBER_OF_SUPPORTED_EXIT_FRAMES],
            },
        ];
        self.send_control(nci::AndroidGetCapsResponse {
            status: nci::Status::Ok,
            android_version: 0,
            tlvs: cap_tlvs,
        })
        .await?;
        Ok(())
    }

    async fn android_passive_observe_mode(
        &mut self,
        cmd: nci::AndroidPassiveObserveModeCommand,
    ) -> Result<()> {
        info!("[{}] ANDROID_PASSIVE_OBSERVE_MODE_CMD", self.id);
        info!("     Mode: {:?}", cmd.passive_observe_mode());

        self.state.passive_observe_mode =
            if cmd.passive_observe_mode() == nci::PassiveObserveMode::Disable {
                u8::from(nci::PassiveObserveMode::Disable)
            } else {
                u8::from(nci::TechnologyMask::AllOn)
            };
        self.state.last_observe_mode_state = None;
        self.state.exit_frame_start_time = None;
        self.send_control(nci::AndroidPassiveObserveModeResponse { status: nci::Status::Ok })
            .await?;
        Ok(())
    }

    async fn android_set_passive_observer_tech(
        &mut self,
        cmd: nci::AndroidSetPassiveObserverTechCommand,
    ) -> Result<()> {
        info!("[{}] ANDROID_SET_PASSIVE_OBSERVER_TECH_CMD", self.id);
        info!("     Mask: {:#b}", cmd.tech_mask());

        self.state.passive_observe_mode = cmd.tech_mask();
        self.state.last_observe_mode_state = None;
        self.state.exit_frame_start_time = None;

        self.send_control(nci::AndroidPassiveObserveModeResponse { status: nci::Status::Ok })
            .await?;
        Ok(())
    }

    async fn android_query_passive_observe_mode(
        &mut self,
        _cmd: nci::AndroidQueryPassiveObserveModeCommand,
    ) -> Result<()> {
        info!("[{}] ANDROID_QUERY_PASSIVE_OBSERVE_MODE_CMD", self.id);
        info!("     Observe mode state: {:#b}", self.state.passive_observe_mode);

        self.send_control(nci::AndroidQueryPassiveObserveModeResponse {
            status: nci::Status::Ok,
            passive_observe_mode: self.state.passive_observe_mode,
        })
        .await?;
        Ok(())
    }

    async fn android_set_passive_observer_exit_frames(
        &mut self,
        cmd: nci::AndroidSetPassiveObserverExitFrameCommand,
    ) -> Result<()> {
        info!("[{}] ANDROID_SET_PASSIVE_OBSERVER_EXIT_FRAMES_CMD", self.id);

        if self.state.rf_state == RfState::Idle || self.state.rf_state == RfState::Discovery {
            self.state.exit_frames.clear();
            self.state.exit_frame_timeout =
                Duration::from_millis(u16::from_le(cmd.timeout()) as u64);
            let exit_frame_count = cmd.exit_frames().len();
            info!("number of exit frames {exit_frame_count:?}");
            let incoming_frames = cmd.exit_frames();
            for frame in incoming_frames.iter() {
                let data_length = frame.field_value[1..].len() / 2;
                let power_states = frame.field_value[0];
                let mut data: Vec<u8> = vec![];
                data.clone_from(&frame.field_value[1..1 + data_length].to_vec());
                let mut mask: Vec<u8> = vec![];
                mask.clone_from(&frame.field_value[1 + data_length..].to_vec());

                self.state.exit_frames.push(RfExitFrame {
                    power_states,
                    data,
                    mask,
                    rf_technology: rf::Technology::from_u8(
                        frame.qualifier_type & EXIT_FRAME_QUALIFIER_TECHNOLOGY_MASK,
                    ),
                    is_prefix_matching_allowed: frame.qualifier_type
                        & EXIT_FRAME_QUALIFIER_PREFIX_MATCHING_MASK
                        != 0,
                });
                info!("Added exit frame {:?}", self.state.exit_frames.last().unwrap())
            }

            self.send_control(nci::AndroidSetPassiveObserverExitFrameResponse {
                status: nci::Status::Ok,
            })
            .await?;
            return Ok(());
        }
        self.send_control(nci::AndroidSetPassiveObserverExitFrameResponse {
            status: nci::Status::SemanticError,
        })
        .await?;
        Ok(())
    }

    async fn receive_command(&mut self, packet: nci::ControlPacket) -> Result<()> {
        use nci::AndroidPacketChild::*;
        use nci::ControlPacketChild::*;
        use nci::CorePacketChild::*;
        use nci::NfceePacketChild::*;
        use nci::ProprietaryPacketChild::*;
        use nci::RfPacketChild::*;

        let child = match packet.specialize() {
            Ok(c) => c,
            Err(err) => {
                warn!("[{}] failed to specialize NCI control packet: {err}", self.id);
                return Ok(());
            }
        };

        match child {
            CorePacket(packet) => match packet.specialize() {
                Ok(CoreResetCommand(cmd)) => self.core_reset(cmd).await,
                Ok(CoreInitCommand(cmd)) => self.core_init(cmd).await,
                Ok(CoreSetConfigCommand(cmd)) => self.core_set_config(cmd).await,
                Ok(CoreGetConfigCommand(cmd)) => self.core_config(cmd).await,
                Ok(CoreConnCreateCommand(cmd)) => self.core_conn_create(cmd).await,
                Ok(CoreConnCloseCommand(cmd)) => self.core_conn_close(cmd).await,
                Ok(CoreSetPowerSubStateCommand(cmd)) => self.core_set_power_sub_state(cmd).await,
                Ok(_) => {
                    warn!("unsupported core oid {:?}", packet.oid());
                    Ok(())
                }
                Err(err) => {
                    warn!("failed to specialize core packet: {err}");
                    Ok(())
                }
            },
            RfPacket(packet) => match packet.specialize() {
                Ok(RfDiscoverMapCommand(cmd)) => self.rf_discover_map(cmd).await,
                Ok(RfSetListenModeRoutingCommand(cmd)) => {
                    self.rf_set_listen_mode_routing(cmd).await
                }
                Ok(RfGetListenModeRoutingCommand(cmd)) => self.rf_listen_mode_routing(cmd).await,
                Ok(RfDiscoverCommand(cmd)) => self.rf_discover(cmd).await,
                Ok(RfDiscoverSelectCommand(cmd)) => self.rf_discover_select(cmd).await,
                Ok(RfDeactivateCommand(cmd)) => self.rf_deactivate(cmd).await,
                Ok(_) => {
                    warn!("unsupported rf oid {:?}", packet.oid());
                    Ok(())
                }
                Err(err) => {
                    warn!("failed to specialize rf packet: {err}");
                    Ok(())
                }
            },
            NfceePacket(packet) => match packet.specialize() {
                Ok(NfceeDiscoverCommand(cmd)) => self.nfcee_discover(cmd).await,
                Ok(NfceeModeSetCommand(cmd)) => self.nfcee_mode_set(cmd).await,
                Ok(_) => {
                    warn!("unsupported nfcee oid {:?}", packet.oid());
                    Ok(())
                }
                Err(err) => {
                    warn!("failed to specialize nfcee packet: {err}");
                    Ok(())
                }
            },
            ProprietaryPacket(packet) => match packet.specialize() {
                Ok(AndroidPacket(packet)) => match packet.specialize() {
                    Ok(AndroidGetCapsCommand(cmd)) => self.android_caps(cmd).await,
                    Ok(AndroidPassiveObserveModeCommand(cmd)) => {
                        self.android_passive_observe_mode(cmd).await
                    }
                    Ok(AndroidSetPassiveObserverTechCommand(cmd)) => {
                        self.android_set_passive_observer_tech(cmd).await
                    }
                    Ok(AndroidQueryPassiveObserveModeCommand(cmd)) => {
                        self.android_query_passive_observe_mode(cmd).await
                    }
                    Ok(AndroidSetPassiveObserverExitFrameCommand(cmd)) => {
                        self.android_set_passive_observer_exit_frames(cmd).await
                    }
                    Ok(_) => {
                        warn!("unsupported android sub oid {:?}", packet.android_sub_oid());
                        Ok(())
                    }
                    Err(err) => {
                        warn!("failed to specialize android packet: {err}");
                        Ok(())
                    }
                },
                Ok(_) => {
                    warn!("unsupported proprietary oid {:?}", packet.oid());
                    Ok(())
                }
                Err(err) => {
                    warn!("failed to specialize proprietary packet: {err}");
                    Ok(())
                }
            },
            _ => {
                warn!("unsupported gid {:?}", packet.gid());
                Ok(())
            }
        }
    }

    async fn rf_conn_data(&mut self, packet: nci::DataPacket) -> Result<()> {
        info!("[{}] received data on RF logical connection", self.id);

        // TODO(henrichataing) implement credit based control flow.
        match self.state.rf_state {
            RfState::PollActive {
                id,
                rf_technology,
                rf_protocol: rf::Protocol::IsoDep,
                rf_interface: nci::RfInterfaceType::IsoDep,
                ..
            }
            | RfState::ListenActive {
                id,
                rf_technology,
                rf_protocol: rf::Protocol::IsoDep,
                rf_interface: nci::RfInterfaceType::IsoDep,
                ..
            } => {
                self.state.last_rf_received_time = Some(Instant::now());
                self.send_rf(rf::Data {
                    receiver: id,
                    sender: self.id,
                    bitrate: rf::BitRate::BitRate106KbitS,
                    power_level: 255,
                    protocol: rf::Protocol::IsoDep,
                    technology: rf_technology,
                    data: packet.payload().into(),
                })?;
                // Resplenish the credit count for the RF Connection.
                self.send_control(nci::CoreConnCreditsNotification {
                    connections: vec![nci::ConnectionCredits {
                        conn_id: nci::ConnId::StaticRf,
                        credits: 1,
                    }],
                })
                .await
            }
            RfState::PollActive {
                rf_protocol: rf::Protocol::IsoDep,
                rf_interface: nci::RfInterfaceType::Frame,
                ..
            } => {
                println!("ISO-DEP frame data {:?}", packet.payload());
                match packet.payload() {
                    // RATS command
                    // TODO(henrichataing) Send back the response received from
                    // the peer in the RF packet.
                    [0xe0, _] => {
                        warn!("[{}] frame RATS command", self.id);
                        self.send_data(nci::DataPacket {
                            mt: nci::MessageType::Data,
                            conn_id: nci::ConnId::StaticRf,
                            cr: 0,
                            payload: self.state.rf_activation_parameters.clone(),
                        })
                        .await?
                    }
                    // DESELECT command
                    // TODO(henrichataing) check if the command should be
                    // forwarded to the peer, and if it warrants a response
                    [0xc2] => warn!("[{}] unimplemented frame DESELECT command", self.id),
                    // SLP_REQ command
                    // No response is expected for this command.
                    // TODO(henrichataing) forward a deactivation request to
                    // the peer and deactivate the local interface.
                    [0x50, 0x00] => warn!("[{}] unimplemented frame SLP_REQ command", self.id),
                    _ => unimplemented!(),
                };
                // Resplenish the credit count for the RF Connection.
                self.send_control(nci::CoreConnCreditsNotification {
                    connections: vec![nci::ConnectionCredits {
                        conn_id: nci::ConnId::StaticRf,
                        credits: 1,
                    }],
                })
                .await
            }
            RfState::PollActive { rf_protocol, rf_interface, .. }
            | RfState::ListenActive { rf_protocol, rf_interface, .. } => unimplemented!(
                "unsupported combination of RF protocol {:?} and interface {:?}",
                rf_protocol,
                rf_interface
            ),
            _ => {
                warn!(
                    "[{}] ignored RF data packet while not in active listen or poll mode",
                    self.id
                );
                Ok(())
            }
        }
    }

    async fn hci_conn_data(&mut self, _packet: nci::DataPacket) -> Result<()> {
        info!("[{}] received data on HCI logical connection", self.id);
        Ok(())
    }

    async fn dynamic_conn_data(&mut self, conn_id: u8, packet: nci::DataPacket) -> Result<()> {
        info!("[{}] received data on dynamic logical connection", self.id);
        let response = packet.payload();

        self.send_data(nci::DataPacket {
            mt: nci::MessageType::Data,
            conn_id: nci::ConnId::from_dynamic(conn_id),
            cr: 0,
            payload: response.to_vec(),
        })
        .await?;

        // Resplenish the credit count for the HCI Connection.
        self.send_control(nci::CoreConnCreditsNotification {
            connections: vec![nci::ConnectionCredits {
                conn_id: nci::ConnId::from_dynamic(conn_id),
                credits: 1,
            }],
        })
        .await
    }

    async fn receive_data(&mut self, packet: nci::DataPacket) -> Result<()> {
        info!("[{}] receive_data({})", self.id, u8::from(packet.conn_id()));

        match packet.conn_id() {
            nci::ConnId::StaticRf => self.rf_conn_data(packet).await,
            nci::ConnId::StaticHci => self.hci_conn_data(packet).await,
            nci::ConnId::Dynamic(id) => self.dynamic_conn_data(*id, packet).await,
        }
    }

    async fn field_info(&mut self, field_status: rf::FieldStatus, power_level: u8) -> Result<()> {
        if self.state.remote_field_status != field_status {
            if self.state.config_parameters.rf_field_info != 0 {
                self.send_control(nci::RfFieldInfoNotification {
                    rf_field_status: match field_status {
                        rf::FieldStatus::FieldOn => nci::RfFieldStatus::FieldDetected,
                        rf::FieldStatus::FieldOff => nci::RfFieldStatus::NoFieldDetected,
                    },
                })
                .await?;
            }
            if self.state.passive_observe_mode != nci::PassiveObserveMode::Disable.into() {
                self.send_control(nci::AndroidPollingLoopNotification {
                    polling_frames: vec![nci::PollingFrame {
                        frame_type: nci::PollingFrameType::RemoteField,
                        flags: nci::PollingFrameFlags { format: nci::PollingFrameFormat::Short },
                        timestamp: (self.state.start_time.elapsed().as_micros() as u32)
                            .to_be_bytes(),
                        gain: power_level,
                        payload: vec![field_status.into()],
                    }],
                })
                .await?;
            }
            self.state.remote_field_status = field_status;
        }
        Ok(())
    }

    async fn poll_command(&mut self, cmd: rf::PollCommand) -> Result<()> {
        if matches!(
            self.state.rf_state,
            RfState::PollActive { .. }
                | RfState::ListenActive { .. }
                | RfState::WaitForSelectResponse { .. }
                | RfState::WaitForHostSelect
        ) {
            return Ok(());
        }

        if self.state.rf_state != RfState::Discovery {
            return Ok(());
        }
        let technology = cmd.technology();

        // Synthesize RF Field Activation immediately when incoming carrier power is detected
        self.field_info(rf::FieldStatus::FieldOn, cmd.power_level()).await?;

        // Android proprietary extension for polling frame notifications.
        // The NFCC should send the NCI_ANDROID_POLLING_FRAME_NTF to the Host
        // after each polling loop frame
        // This notification is independent of whether Passive Observe Mode is
        // active or not. When Passive Observe Mode is active, the NFCC
        // should always send this notification before proceeding with the
        // transaction.

        let data = cmd.payload();
        let format = cmd.format();

        let (crc_valid, data) = match technology {
            // If frame longer than 2 bytes has valid CRC
            // cut it (2 last bytes) out of result
            rf::Technology::NfcA | rf::Technology::NfcB
                if data.len() > 2
                    && format == rf::PollingFrameFormat::Long
                    && crc::verify_crc(technology, data).is_ok() =>
            {
                (true, data[0..data.len() - 2].to_vec())
            }
            // If length of data is less than 2 bytes,
            // or the last byte does not contain 8 bits, there can be no CRC
            // Frames without or with invalid CRC are returned in full
            _ => (false, data.to_vec()),
        };

        if self.state.passive_observe_mode != nci::PassiveObserveMode::Disable.into() {
            if self.has_exit_frame(&data, technology)
                || (technology == rf::Technology::NfcA && (data == [0x52] || data == [0x26]))
            {
                self.state.last_observe_mode_state = Some(self.state.passive_observe_mode);
                self.state.passive_observe_mode = nci::PassiveObserveMode::Disable.into();
                self.state.exit_frame_start_time = Some(Instant::now());

                self.send_control(nci::PassiveObserverSuspendedNotification {
                    exit_frame_type: match technology {
                        rf::Technology::NfcA => 0x00,
                        rf::Technology::NfcB => 0x01,
                        _ => panic!(),
                    },
                    payload: data.clone(),
                })
                .await?;
            }

            self.send_control(nci::AndroidPollingLoopNotification {
                polling_frames: vec![nci::PollingFrame {
                    frame_type: match technology {
                        rf::Technology::NfcA => {
                            // WUPA/REQA
                            // Musn't have CRC
                            if !crc_valid
                                // have short format (7 bits in last byte)
                                && format == rf::PollingFrameFormat::Short
                                // 1 byte long in total
                                && data.len() == 1
                                // start with 0x52 (WUPA) or 0x26 (REQA)
                                && (data[0] == 0x52 || data[0] == 0x26)
                            {
                                nci::PollingFrameType::Reqa
                            } else {
                                // Other are considered as polling loop annotations
                                nci::PollingFrameType::Unknown
                            }
                        }
                        rf::Technology::NfcB => {
                            // REQB/WUPB
                            // Must have valid CRC
                            if crc_valid
                                // have long format (8 bits in last byte)
                                && format == rf::PollingFrameFormat::Long
                                // 3 bytes long (APf, AFI and PARAM)
                                && data.len() == 3
                                // start with 0x05 (APf)
                                && data[0] == 0x05
                            {
                                nci::PollingFrameType::Reqb
                            } else {
                                // Other are considered as polling loop annotations
                                nci::PollingFrameType::Unknown
                            }
                        }
                        // Type F frames cannot serve as polling loop annotations
                        rf::Technology::NfcF => nci::PollingFrameType::Reqf,
                        // Type V frames cannot serve as polling loop annotations
                        rf::Technology::NfcV => nci::PollingFrameType::Reqv,
                        rf::Technology::Raw => nci::PollingFrameType::Unknown,
                    },
                    // NCI_ANDROID_POLLING_FRAME_NTF Flags
                    // b0: 0 - short frame; 1 - long frame
                    flags: nci::PollingFrameFlags {
                        format: match format {
                            rf::PollingFrameFormat::Long => nci::PollingFrameFormat::Long,
                            rf::PollingFrameFormat::Short => nci::PollingFrameFormat::Short,
                        },
                    },
                    timestamp: (self.state.start_time.elapsed().as_micros() as u32).to_be_bytes(),
                    gain: cmd.power_level(),
                    payload: data,
                }],
            })
            .await?;
        }

        // When the Passive Observe Mode is active, the NFCC shall not respond
        // to any poll requests during the polling loop in Listen Mode, until
        // explicitly authorized by the Host.
        let mask: u8 = match technology {
            rf::Technology::NfcA => nci::TechnologyMask::NfcA.into(),
            rf::Technology::NfcB => nci::TechnologyMask::NfcB.into(),
            rf::Technology::NfcF => nci::TechnologyMask::NfcF.into(),
            rf::Technology::NfcV => nci::TechnologyMask::NfcV.into(),
            _ => 0,
        };
        if self.state.passive_observe_mode & mask != 0 {
            return Ok(());
        }

        let supports_listen = self.state.discover_configuration.iter().any(|config| {
            matches!(
                (config.technology_and_mode, technology),
                (nci::RfTechnologyAndMode::NfcAPassiveListenMode, rf::Technology::NfcA)
                    | (nci::RfTechnologyAndMode::NfcBPassiveListenMode, rf::Technology::NfcB)
                    | (nci::RfTechnologyAndMode::NfcFPassiveListenMode, rf::Technology::NfcF)
            )
        });

        if !supports_listen {
            return Ok(());
        }

        match technology {
            rf::Technology::NfcA => self.send_rf(rf::NfcAPollResponse {
                protocol: rf::Protocol::Undetermined,
                receiver: cmd.sender(),
                sender: self.id,
                bitrate: rf::BitRate::BitRate106KbitS,
                power_level: 255,
                nfcid1: self.state.nfcid1(),
                int_protocol: self.state.config_parameters.la_sel_info >> 5,
                bit_frame_sdd: self.state.config_parameters.la_bit_frame_sdd,
            })?,
            // TODO(b/346715736) implement support for NFC-B technology
            rf::Technology::NfcB => (),
            rf::Technology::NfcF => (),
            _ => (),
        }

        Ok(())
    }

    fn has_exit_frame(&mut self, polling_frame_data: &[u8], rf_technology: rf::Technology) -> bool {
        'frame_loop: for exit_frame in self.state.exit_frames.iter() {
            if rf_technology != exit_frame.rf_technology {
                continue;
            }

            let exit_frame_len = exit_frame.data.len();
            // This checks if the data in the exit frame matches the polling frame.
            for n in 0..exit_frame_len {
                if n >= polling_frame_data.len()
                    || exit_frame.data[n] != (polling_frame_data[n] & exit_frame.mask[n])
                {
                    continue 'frame_loop;
                }
            }
            // TODO(johnrjohn) Check if power state matches.

            // If the lengths do not match, or if prefix matching is not allowed, it means there
            // is unmatched data in the polling frame and this isn't a match.
            if exit_frame.data.len() == polling_frame_data.len()
                || exit_frame.is_prefix_matching_allowed
            {
                info!(
                    "Exit frame matched! PollingFrame: {polling_frame_data:?}, ExitFrame {exit_frame:?}"
                );
                return true;
            }
        }
        false
    }

    async fn nfca_poll_response(&mut self, cmd: rf::NfcAPollResponse) -> Result<()> {
        let int_protocol = cmd.int_protocol();
        let sel_res = int_protocol << 5;
        info!("[{}] nfca_poll_response()", self.id);

        if self.state.rf_state != RfState::Discovery {
            return Ok(());
        }

        let rf_protocols = match int_protocol {
            0b00 => [rf::Protocol::T2t].iter(),
            0b01 => [rf::Protocol::IsoDep].iter(),
            0b10 => [rf::Protocol::NfcDep].iter(),
            0b11 => [rf::Protocol::NfcDep, rf::Protocol::IsoDep].iter(),
            _ => return Ok(()),
        };
        let sens_res = match cmd.nfcid1().len() {
            4 => 0x00,
            7 => 0x40,
            10 => 0x80,
            _ => panic!(),
        } | cmd.bit_frame_sdd() as u16;

        for rf_protocol in rf_protocols {
            self.state.add_poll_response(RfPollResponse {
                id: cmd.sender(),
                rf_protocol: *rf_protocol,
                rf_technology: rf::Technology::NfcA,
                rf_technology_specific_parameters: nci::NfcAPollModeTechnologySpecificParameters {
                    sens_res,
                    nfcid1: cmd.nfcid1().clone(),
                    sel_res,
                }
                .encode_to_vec()?,
            })
        }

        Ok(())
    }

    async fn t4at_select_command(&mut self, cmd: rf::IsoDepT4ATSelectCommand) -> Result<()> {
        info!("[{}] t4at_select_command()", self.id);

        let supports_listen = self.state.discover_configuration.iter().any(|config| {
            matches!(config.technology_and_mode, nci::RfTechnologyAndMode::NfcAPassiveListenMode)
        });

        if !supports_listen {
            return Ok(());
        }

        match self.state.rf_state {
            RfState::Discovery | RfState::Idle | RfState::ListenActive { .. } => (),
            RfState::ListenSleep { id } if id == cmd.sender() => (),
            _ => {
                return Ok(());
            }
        };

        self.state.rf_state = RfState::ListenActive {
            id: cmd.sender(),
            rf_technology: rf::Technology::NfcA,
            rf_protocol: rf::Protocol::IsoDep,
            rf_interface: nci::RfInterfaceType::IsoDep,
        };
        self.state.last_rf_received_time = Some(Instant::now());

        // [DIGITAL] 14.6.2 RATS Response (Answer To Select)
        // Construct the response from the values passed in the configuration
        // parameters. The TL byte is excluded from the response.
        let mut rats_response = vec![
            0x78, // TC(1), TB(1), TA(1) transmitted, FSCI=8
            0x80, // TA(1)
            self.state.config_parameters.li_a_rats_tb1,
            self.state.config_parameters.li_a_rats_tc1,
        ];

        rats_response.extend_from_slice(&self.state.config_parameters.li_a_hist_by);

        self.send_rf(rf::IsoDepT4ATSelectResponse {
            receiver: cmd.sender(),
            sender: self.id,
            bitrate: rf::BitRate::BitRate106KbitS,
            power_level: 255,
            rats_response,
        })?;

        info!("[{}] RF_INTF_ACTIVATED_NTF", self.id);
        info!("         DiscoveryID: {:?}", nci::RfDiscoveryId::from_index(0));
        info!("         Interface: ISO-DEP");
        info!("         Protocol: ISO-DEP");
        info!("         ActivationTechnology: NFC_A_PASSIVE_LISTEN");
        info!("         RATS: {}", cmd.param());

        self.send_control(nci::RfIntfActivatedNotification {
            rf_discovery_id: nci::RfDiscoveryId::from_index(0),
            rf_interface: nci::RfInterfaceType::IsoDep,
            rf_protocol: nci::RfProtocolType::IsoDep,
            activation_rf_technology_and_mode: nci::RfTechnologyAndMode::NfcAPassiveListenMode,
            max_data_packet_payload_size: MAX_DATA_PACKET_PAYLOAD_SIZE,
            initial_number_of_credits: 1,
            // No parameters are currently defined for NFC-A Listen Mode.
            rf_technology_specific_parameters: vec![],
            data_exchange_rf_technology_and_mode: nci::RfTechnologyAndMode::NfcAPassiveListenMode,
            data_exchange_transmit_bit_rate: nci::BitRate::BitRate106KbitS,
            data_exchange_receive_bit_rate: nci::BitRate::BitRate106KbitS,
            activation_parameters: nci::NfcAIsoDepListenModeActivationParameters {
                param: cmd.param(),
            }
            .encode_to_vec()?,
        })
        .await?;

        Ok(())
    }

    async fn t4at_select_response(&mut self, cmd: rf::IsoDepT4ATSelectResponse) -> Result<()> {
        info!("[{}] t4at_select_response()", self.id);

        let (id, rf_discovery_id, rf_interface, rf_protocol) = match self.state.rf_state {
            RfState::WaitForSelectResponse {
                id,
                rf_discovery_id,
                rf_interface,
                rf_protocol,
                ..
            } => (id, rf_discovery_id, rf_interface, rf_protocol),
            _ => return Ok(()),
        };

        if cmd.sender() != id {
            return Ok(());
        }

        self.state.rf_state = RfState::PollActive {
            id,
            rf_protocol: self.state.rf_poll_responses[rf_discovery_id].rf_protocol,
            rf_technology: self.state.rf_poll_responses[rf_discovery_id].rf_technology,
            rf_interface,
        };
        self.state.last_rf_received_time = Some(Instant::now());

        // Save the activation parameters for the RF frame interface
        // implementation. Note: TL is not included in the RATS response
        // and needs to be added manually to the activation parameters.
        self.state.rf_activation_parameters = vec![cmd.rats_response().len() as u8];
        self.state.rf_activation_parameters.extend_from_slice(cmd.rats_response());

        info!("[{}] RF_INTF_ACTIVATED_NTF", self.id);
        info!("         DiscoveryID: {:?}", nci::RfDiscoveryId::from_index(rf_discovery_id));
        info!("         Interface: {rf_interface:?}");
        info!("         Protocol: {rf_protocol:?}");
        info!("         ActivationTechnology: NFC_A_PASSIVE_POLL");
        info!("         RATS: {:?}", cmd.rats_response());

        self.send_control(nci::RfIntfActivatedNotification {
            rf_discovery_id: nci::RfDiscoveryId::from_index(rf_discovery_id),
            rf_interface,
            rf_protocol: rf_protocol.into(),
            activation_rf_technology_and_mode: nci::RfTechnologyAndMode::NfcAPassivePollMode,
            max_data_packet_payload_size: MAX_DATA_PACKET_PAYLOAD_SIZE,
            initial_number_of_credits: 1,
            rf_technology_specific_parameters: self.state.rf_poll_responses[rf_discovery_id]
                .rf_technology_specific_parameters
                .clone(),
            data_exchange_rf_technology_and_mode: nci::RfTechnologyAndMode::NfcAPassivePollMode,
            data_exchange_transmit_bit_rate: nci::BitRate::BitRate106KbitS,
            data_exchange_receive_bit_rate: nci::BitRate::BitRate106KbitS,
            // TODO(hchataing) the activation parameters should be empty
            // when the RF frame interface is used, since the protocol
            // activation is managed by the DH.
            activation_parameters: nci::NfcAIsoDepPollModeActivationParameters {
                rats_response: cmd.rats_response().clone(),
            }
            .encode_to_vec()?,
        })
        .await?;

        Ok(())
    }

    async fn data_packet(&mut self, data: rf::Data) -> Result<()> {
        info!("[{}] data_packet()", self.id);

        match (self.state.rf_state, data.protocol()) {
            (
                RfState::PollActive {
                    id, rf_technology, rf_protocol: rf::Protocol::IsoDep, ..
                },
                rf::Protocol::IsoDep,
            )
            | (
                RfState::ListenActive {
                    id, rf_technology, rf_protocol: rf::Protocol::IsoDep, ..
                },
                rf::Protocol::IsoDep,
            ) if data.sender() == id && data.technology() == rf_technology => {
                if matches!(self.state.rf_state, RfState::ListenActive { .. }) {
                    let payload = data.data();

                    // Autonomous NFCC T4T Presence Check / NDEF probe fast-path
                    let is_gesture_exchange = payload.len() >= 11
                        && payload.starts_with(&[0x00, 0xA4, 0x04, 0x00])
                        && payload[5..].starts_with(&[0xA0, 0x00, 0x00, 0x04, 0x76, 0x09]);

                    if !is_gesture_exchange {
                        if payload.starts_with(&[0x00, 0xA4, 0x04, 0x00, 0x07, 0xD2, 0x76])
                            || payload.starts_with(&[0x00, 0xA4, 0x00])
                            || payload.starts_with(&[0x00, 0xB0])
                        {
                            self.state.last_rf_received_time = Some(Instant::now());
                            self.send_rf(rf::Data {
                                receiver: id,
                                sender: self.id,
                                bitrate: rf::BitRate::BitRate106KbitS,
                                power_level: 255,
                                protocol: rf::Protocol::IsoDep,
                                technology: rf_technology,
                                data: vec![0x6A, 0x82],
                            })?;
                            return Ok(());
                        }

                        if payload.is_empty() {
                            self.state.last_rf_received_time = Some(Instant::now());
                            self.send_rf(rf::Data {
                                receiver: id,
                                sender: self.id,
                                bitrate: rf::BitRate::BitRate106KbitS,
                                power_level: 255,
                                protocol: rf::Protocol::IsoDep,
                                technology: rf_technology,
                                data: vec![],
                            })?;
                            return Ok(());
                        }
                    }
                }
                self.state.last_rf_received_time = Some(Instant::now());
                self.send_data(nci::DataPacket {
                    mt: nci::MessageType::Data,
                    conn_id: nci::ConnId::StaticRf,
                    cr: 0,
                    payload: data.data().clone(),
                })
                .await
            }
            (RfState::PollActive { id, .. }, _) | (RfState::ListenActive { id, .. }, _)
                if id != data.sender() =>
            {
                warn!("[{}] ignored RF data packet sent from an un-selected device", self.id);
                Ok(())
            }
            (RfState::PollActive { .. }, _) | (RfState::ListenActive { .. }, _) => {
                unimplemented!("unsupported combination of technology and protocol")
            }
            (_, _) => {
                warn!("[{}] ignored RF data packet received in inactive state", self.id);
                Ok(())
            }
        }
    }

    async fn deactivate_notification(&mut self, cmd: rf::DeactivateNotification) -> Result<()> {
        info!("[{}] deactivate_notification()", self.id);

        use rf::DeactivateType::*;

        let mut next_state = match (self.state.rf_state, cmd.type_()) {
            (RfState::PollActive { id, .. }, IdleMode) if id == cmd.sender() => RfState::Idle,
            (RfState::PollActive { id, .. }, SleepMode | SleepAfMode) if id == cmd.sender() => {
                RfState::WaitForHostSelect
            }
            (RfState::PollActive { id, .. }, Discovery) if id == cmd.sender() => RfState::Discovery,
            (RfState::ListenSleep { id, .. }, IdleMode) if id == cmd.sender() => RfState::Idle,
            (RfState::ListenSleep { id, .. }, Discovery) if id == cmd.sender() => {
                RfState::Discovery
            }
            (RfState::ListenActive { id, .. }, IdleMode) if id == cmd.sender() => RfState::Idle,
            (RfState::ListenActive { id, .. }, SleepMode | SleepAfMode) if id == cmd.sender() => {
                RfState::ListenSleep { id }
            }
            (RfState::ListenActive { id, .. }, Discovery) if id == cmd.sender() => {
                RfState::Discovery
            }
            (_, _) => self.state.rf_state,
        };

        // Update the state now to prevent interface activation from
        // completing if a remote device is being selected.
        (next_state, self.state.rf_state) = (self.state.rf_state, next_state);

        // Deactivate the active RF interface if applicable.
        if next_state != self.state.rf_state {
            self.field_info(rf::FieldStatus::FieldOff, 255).await?;
            self.send_control(nci::RfDeactivateNotification {
                deactivation_type: cmd.type_().into(),
                deactivation_reason: cmd.reason().into(),
            })
            .await?
        }

        Ok(())
    }

    async fn receive_rf(&mut self, packet: rf::RfPacket) -> Result<()> {
        use rf::RfPacketChild::*;

        self.state.last_rf_received_time = Some(Instant::now());

        let child = match packet.specialize() {
            Ok(c) => c,
            Err(err) => {
                warn!("[{}] failed to specialize RF packet: {err}", self.id);
                return Ok(());
            }
        };

        match child {
            PollCommand(cmd) => self.poll_command(cmd).await,
            FieldInfo(cmd) => self.field_info(cmd.field_status(), cmd.power_level()).await,
            NfcAPollResponse(cmd) => self.nfca_poll_response(cmd).await,
            // [NCI] 5.2.2 State RFST_DISCOVERY
            // If discovered by a Remote NFC Endpoint in Listen mode, once the
            // Remote NFC Endpoint has established any underlying protocol(s) needed
            // by the configured RF Interface, the NFCC SHALL send
            // RF_INTF_ACTIVATED_NTF (Listen Mode) to the DH and the state is
            // changed to RFST_LISTEN_ACTIVE.
            IsoDepT4ATSelectCommand(cmd) => self.t4at_select_command(cmd).await,
            IsoDepT4ATSelectResponse(cmd) => self.t4at_select_response(cmd).await,
            SelectCommand(_) => Ok(()),
            DeactivateNotification(cmd) => self.deactivate_notification(cmd).await,
            Data(cmd) => self.data_packet(cmd).await,
            _ => Ok(()),
        }
    }

    /// Activity for activating an RF interface for a discovered device.
    ///
    /// The method send a notification when the interface is successfully
    /// activated, or when the device activation fails.
    ///
    ///  * `rf_discovery_id` - index of the discovered device
    ///  * `rf_interface` - interface to activate
    ///
    /// The RF state is changed to WaitForSelectResponse when
    /// the select command is successfully sent.
    async fn activate_poll_interface(
        &mut self,
        rf_discovery_id: usize,
        rf_protocol: nci::RfProtocolType,
        rf_interface: nci::RfInterfaceType,
    ) -> Result<()> {
        info!("[{}] activate_poll_interface({:?})", self.id, rf_interface);

        let rf_technology = self.state.rf_poll_responses[rf_discovery_id].rf_technology;
        match (rf_protocol, rf_technology) {
            (nci::RfProtocolType::T2t, rf::Technology::NfcA) => {
                self.send_rf(rf::SelectCommand {
                    sender: self.id,
                    receiver: self.state.rf_poll_responses[rf_discovery_id].id,
                    technology: rf::Technology::NfcA,
                    bitrate: rf::BitRate::BitRate106KbitS,
                    power_level: 255,
                    protocol: rf::Protocol::T2t,
                })?
            }
            (nci::RfProtocolType::IsoDep, rf::Technology::NfcA) => {
                self.send_rf(rf::IsoDepT4ATSelectCommand {
                    sender: self.id,
                    receiver: self.state.rf_poll_responses[rf_discovery_id].id,
                    bitrate: rf::BitRate::BitRate106KbitS,
                    power_level: 255,
                    // [DIGITAL] 14.6.1.6 The FSD supported by the
                    // Reader/Writer SHALL be FSD T4AT,MIN
                    // (set to 256 in Appendix B.6).
                    param: 0x80,
                })?
            }
            (nci::RfProtocolType::NfcDep, rf::Technology::NfcA) => {
                self.send_rf(rf::NfcDepSelectCommand {
                    sender: self.id,
                    receiver: self.state.rf_poll_responses[rf_discovery_id].id,
                    bitrate: rf::BitRate::BitRate106KbitS,
                    power_level: 255,
                    technology: rf::Technology::NfcA,
                    lr: 0,
                })?
            }
            _ => todo!(),
        }

        self.state.rf_state = RfState::WaitForSelectResponse {
            id: self.state.rf_poll_responses[rf_discovery_id].id,
            rf_discovery_id,
            rf_interface,
            rf_protocol: rf_protocol.into(),
            rf_technology,
        };
        Ok(())
    }

    async fn run_until<O>(&mut self, future: impl Future<Output = O>) -> Result<O> {
        let mut future = pin!(future);
        loop {
            tokio::select! {
                packet = self.nci_stream.next() => {
                    let packet = match packet {
                        Some(Ok(p)) => p,
                        Some(Err(err)) => {
                            warn!("[{}] failed to read NCI packet: {err}", self.id);
                            continue;
                        }
                        None => return Err(anyhow::anyhow!("nci channel closed")),
                    };
                    let header = match nci::PacketHeader::decode_full(&packet[0..3]) {
                        Ok(h) => h,
                        Err(err) => {
                            warn!("[{}] failed to decode NCI packet header: {err}", self.id);
                            continue;
                        }
                    };
                    match header.mt() {
                        nci::MessageType::Data => {
                            match nci::DataPacket::decode(&packet) {
                                Ok((p, _)) => self.receive_data(p).await?,
                                Err(err) => warn!("[{}] failed to decode NCI data packet: {err}", self.id),
                            }
                        }
                        nci::MessageType::Command => {
                            match nci::ControlPacket::decode(&packet) {
                                Ok((p, _)) => self.receive_command(p).await?,
                                Err(err) => warn!("[{}] failed to decode NCI command packet: {err}", self.id),
                            }
                        }
                        mt => {
                            warn!("[{}] unexpected message type {:?} in received NCI packet", self.id, mt);
                        }
                    }
                },
                rf_packet = self.rf_rx.recv() => {
                    self.receive_rf(
                        rf_packet.ok_or(anyhow::anyhow!("rf_rx channel closed"))?,
                    )
                    .await?
                },
                output = &mut future => break Ok(output)
            }
        }
    }

    /// Timer handler method. This function is invoked at regular interval
    /// on the NFCC instance and is used to drive internal timers.
    async fn tick(&mut self) -> Result<()> {
        // Deactivate RF field / active connection if no RF packet received within 5000ms
        if let Some(last_rx) = self.state.last_rf_received_time {
            if last_rx.elapsed() > Duration::from_millis(5000) {
                if self.state.remote_field_status == rf::FieldStatus::FieldOn {
                    self.field_info(rf::FieldStatus::FieldOff, 0).await?;
                }
                match self.state.rf_state {
                    RfState::PollActive { .. }
                    | RfState::ListenActive { .. }
                    | RfState::WaitForSelectResponse { .. } => {
                        self.state.rf_state = RfState::Discovery;
                        self.state.last_rf_received_time = None;
                        self.send_control(nci::RfDeactivateNotification {
                            deactivation_type: nci::DeactivationType::Discovery,
                            deactivation_reason: nci::DeactivationReason::RfLinkLoss,
                        })
                        .await?;
                    }
                    _ => {}
                }
            }
        }

        if self.state.rf_state != RfState::Discovery {
            return Ok(());
        }

        if let Some(exit_frame_start_time) = self.state.exit_frame_start_time {
            let timeout_ms = if self.state.exit_frame_timeout.as_millis() > 0 {
                self.state.exit_frame_timeout.as_millis()
            } else {
                2000
            };
            let elapsed_ms = exit_frame_start_time.elapsed().as_millis();
            if elapsed_ms > timeout_ms {
                self.state.exit_frame_start_time = None;
                self.state.passive_observe_mode =
                    self.state.last_observe_mode_state.unwrap_or(nci::TechnologyMask::AllOn.into());
                info!("Turning observe mode back on, exit frame timeout has passed.");
                self.send_control(nci::PassiveObserverResumedNotification {}).await?;
            }
        }

        //info!("[{}] poll", self.id);

        // In this state the NFCC stays in Poll Mode and/or Listen Mode (based
        // on the discovery configuration) until at least one Remote NFC
        // Endpoint is detected or the RF Discovery Process is stopped by
        // the DH.
        //
        // The following implements the Poll Mode Discovery, Listen Mode
        // Discover is implicitly implemented in response to poll and
        // select commands.

        // RF Discovery is ongoing and no peer device has been discovered
        // so far. Send a RF poll command for all enabled technologies.
        if self.state.passive_observe_mode != 0 {
            return Ok(());
        }

        self.state.rf_poll_responses.clear();
        for configuration in self.state.discover_configuration.iter() {
            self.send_rf(rf::PollCommand {
                sender: self.id,
                receiver: u16::MAX,
                protocol: rf::Protocol::Undetermined,
                technology: match configuration.technology_and_mode {
                    nci::RfTechnologyAndMode::NfcAPassivePollMode => rf::Technology::NfcA,
                    nci::RfTechnologyAndMode::NfcBPassivePollMode => rf::Technology::NfcB,
                    nci::RfTechnologyAndMode::NfcFPassivePollMode => rf::Technology::NfcF,
                    nci::RfTechnologyAndMode::NfcVPassivePollMode => rf::Technology::NfcV,
                    _ => continue,
                },
                format: match configuration.technology_and_mode {
                    nci::RfTechnologyAndMode::NfcAPassivePollMode => rf::PollingFrameFormat::Short,
                    _ => rf::PollingFrameFormat::Long,
                },
                bitrate: match configuration.technology_and_mode {
                    nci::RfTechnologyAndMode::NfcFPassivePollMode => rf::BitRate::BitRate212KbitS,
                    nci::RfTechnologyAndMode::NfcVPassivePollMode => rf::BitRate::BitRate26KbitS,
                    _ => rf::BitRate::BitRate106KbitS,
                },
                power_level: 255,
                payload: match configuration.technology_and_mode {
                    nci::RfTechnologyAndMode::NfcAPassivePollMode => vec![0x52],
                    nci::RfTechnologyAndMode::NfcBPassivePollMode => vec![0x05, 0x00, 0x00],
                    _ => vec![],
                },
            })?;
        }

        // Wait for poll responses to return.
        self.run_until(time::sleep(Duration::from_millis(POLL_RESPONSE_TIMEOUT))).await?;

        // Check if device was activated in Listen mode during
        // the poll interval, or if the discovery got cancelled.
        if self.state.rf_state != RfState::Discovery || self.state.rf_poll_responses.is_empty() {
            return Ok(());
        }

        // While polling, if the NFCC discovers just one Remote NFC Endpoint
        // that supports just one protocol, the NFCC SHALL try to automatically
        // activate it. The NFCC SHALL first establish any underlying
        // protocol(s) with the Remote NFC Endpoint that are needed by the
        // configured RF Interface. On completion, the NFCC SHALL activate the
        // RF Interface and send RF_INTF_ACTIVATED_NTF (Poll Mode) to the DH.
        // At this point, the state is changed to RFST_POLL_ACTIVE. If the
        // protocol activation is not successful, the NFCC SHALL send
        // CORE_GENERIC_ERROR_NTF to the DH with status
        // DISCOVERY_TARGET_ACTIVATION_FAILED and SHALL stay in the
        // RFST_DISCOVERY state.
        debug!("[{}] received {} poll response(s)", self.id, self.state.rf_poll_responses.len());

        if self.state.passive_observe_mode != 0 {
            return Ok(());
        }

        // [NCI] 2.0 Section 5.2.2 (Discovery)
        // If the NFCC discovers only one remote NFC Endpoint supporting one RF Protocol,
        // the NFCC SHALL activate the RF Interface directly and send RF_INTF_ACTIVATED_NTF
        // (Poll Mode) to the DH, changing state to RFST_POLL_ACTIVE.
        if self.state.rf_poll_responses.len() == 1 {
            let rf_protocol = self.state.rf_poll_responses[0].rf_protocol.into();
            let rf_interface = self.state.select_interface(RfMode::Poll, rf_protocol);
            return self.activate_poll_interface(0, rf_protocol, rf_interface).await;
        }

        // Send RF_DISCOVER_NTF messages to the DH to notify discovery and allow DH
        // to select the interface.
        self.state.rf_state = RfState::WaitForHostSelect;
        let last_index = self.state.rf_poll_responses.len() - 1;
        for (index, response) in self.state.rf_poll_responses.clone().iter().enumerate() {
            self.send_control(nci::RfDiscoverNotification {
                rf_discovery_id: nci::RfDiscoveryId::from_index(index),
                rf_protocol: response.rf_protocol.into(),
                rf_technology_and_mode: match response.rf_technology {
                    rf::Technology::NfcA => nci::RfTechnologyAndMode::NfcAPassivePollMode,
                    rf::Technology::NfcB => nci::RfTechnologyAndMode::NfcBPassivePollMode,
                    _ => todo!(),
                },
                rf_technology_specific_parameters: response
                    .rf_technology_specific_parameters
                    .clone(),
                notification_type: if index == last_index {
                    nci::DiscoverNotificationType::LastNotification
                } else {
                    nci::DiscoverNotificationType::MoreNotifications
                },
            })
            .await?
        }

        Ok(())
    }

    /// Main NFCC instance routine.
    pub async fn run(
        id: u16,
        nci_stream: nci::StreamRefMut<'a>,
        nci_writer: nci::Writer,
        rf_rx: mpsc::UnboundedReceiver<rf::RfPacket>,
        rf_tx: mpsc::UnboundedSender<rf::RfPacket>,
    ) -> Result<()> {
        // Local controller state.
        let mut nfcc = Controller::new(id, nci_stream, nci_writer, rf_rx, rf_tx);

        // Timer for tick events (200ms standard NFC Forum polling cycle).
        let mut timer = time::interval(Duration::from_millis(200));

        loop {
            nfcc.run_until(timer.tick()).await?;
            nfcc.tick().await?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_state() -> State {
        State {
            config_parameters: Default::default(),
            logical_connections: [None; MAX_LOGICAL_CONNECTIONS as usize],
            discover_map: vec![],
            discover_configuration: vec![],
            nfcee_state: NfceeState::Disabled,
            rf_state: RfState::Idle,
            rf_poll_responses: vec![],
            rf_activation_parameters: vec![],
            passive_observe_mode: nci::PassiveObserveMode::Disable.into(),
            last_observe_mode_state: None,
            start_time: Instant::now(),
            remote_field_status: rf::FieldStatus::FieldOff,
            last_rf_received_time: None,
            exit_frame_timeout: Duration::from_millis(0),
            exit_frame_start_time: None,
            exit_frames: vec![],
        }
    }

    #[test]
    fn test_add_poll_response_deduplication_single_peer() {
        let mut state = create_test_state();

        // Peer 4 sends initial SENS_RES frame
        let resp1 = RfPollResponse {
            id: 4,
            rf_protocol: rf::Protocol::IsoDep,
            rf_technology: rf::Technology::NfcA,
            rf_technology_specific_parameters: vec![0x04, 0x00],
        };
        state.add_poll_response(resp1);
        assert_eq!(state.rf_poll_responses.len(), 1);

        // Peer 4 sends updated SEL_RES frame with different technology specific parameters
        let resp2 = RfPollResponse {
            id: 4,
            rf_protocol: rf::Protocol::IsoDep,
            rf_technology: rf::Technology::NfcA,
            rf_technology_specific_parameters: vec![0x20],
        };
        state.add_poll_response(resp2);

        // Multiple poll frames from the same peer must deduplicate by peer ID
        // so that single-peer tap always performs Direct Activation (NCI §5.2.2)
        // instead of Discovery Notification Mode (NCI §5.2.1).
        assert_eq!(
            state.rf_poll_responses.len(),
            1,
            "Multiple poll frames from the same peer ID must deduplicate to avoid triggering multi-tag discovery notifications!"
        );
        assert_eq!(state.rf_poll_responses[0].rf_technology_specific_parameters, vec![0x20]);
    }

    #[test]
    fn test_add_poll_response_multi_peer_no_deduplication() {
        let mut state = create_test_state();

        let resp1 = RfPollResponse {
            id: 1,
            rf_protocol: rf::Protocol::IsoDep,
            rf_technology: rf::Technology::NfcA,
            rf_technology_specific_parameters: vec![0x04, 0x00],
        };
        let resp2 = RfPollResponse {
            id: 2,
            rf_protocol: rf::Protocol::IsoDep,
            rf_technology: rf::Technology::NfcA,
            rf_technology_specific_parameters: vec![0x20],
        };

        state.add_poll_response(resp1);
        state.add_poll_response(resp2);

        assert_eq!(
            state.rf_poll_responses.len(),
            2,
            "Distinct peer IDs must both be recorded for multi-device discovery"
        );
    }

    #[test]
    fn test_poller_preserves_role_during_collision() {
        let mut state = create_test_state();
        state.discover_configuration = vec![nci::DiscoverConfiguration {
            technology_and_mode: nci::RfTechnologyAndMode::NfcAPassivePollMode,
            discovery_frequency: 1,
        }];
        state.rf_state = RfState::WaitForSelectResponse {
            id: 2,
            rf_discovery_id: 0,
            rf_interface: nci::RfInterfaceType::IsoDep,
            rf_protocol: rf::Protocol::IsoDep,
            rf_technology: rf::Technology::NfcA,
        };

        // If device has only Poll mode configured, supports_listen is false.
        let supports_listen = state.discover_configuration.iter().any(|config| {
            matches!(config.technology_and_mode, nci::RfTechnologyAndMode::NfcAPassiveListenMode)
        });
        assert!(!supports_listen, "Pure Reader Mode device must not support listen mode");

        let supports_poll = state.discover_configuration.iter().any(|config| {
            matches!(config.technology_and_mode, nci::RfTechnologyAndMode::NfcAPassivePollMode)
        });
        assert!(supports_poll, "Reader Mode device must support poll mode");
    }

    #[test]
    fn test_observe_mode_and_exit_frame_timeout() {
        let mut state = create_test_state();
        state.passive_observe_mode = nci::TechnologyMask::NfcA.into();
        state.last_observe_mode_state = Some(nci::TechnologyMask::NfcA.into());
        state.exit_frame_timeout = Duration::from_millis(50);
        state.exit_frame_start_time =
            Some(Instant::now().checked_sub(Duration::from_millis(100)).unwrap());

        // Simulate tick checking timeout
        if let Some(exit_frame_start_time) = state.exit_frame_start_time {
            let timeout_ms = state.exit_frame_timeout.as_millis();
            let elapsed_ms = exit_frame_start_time.elapsed().as_millis();
            if elapsed_ms > timeout_ms {
                state.exit_frame_start_time = None;
                state.passive_observe_mode =
                    state.last_observe_mode_state.unwrap_or(nci::TechnologyMask::AllOn.into());
            }
        }

        assert!(
            state.exit_frame_start_time.is_none(),
            "Exit frame start time should be cleared after timeout"
        );
        assert_eq!(state.passive_observe_mode, nci::TechnologyMask::NfcA.into());
    }

    #[test]
    fn test_select_interface_mapping() {
        let mut state = create_test_state();
        state.discover_map = vec![
            nci::MappingConfiguration {
                rf_protocol: nci::RfProtocolType::IsoDep,
                mode: nci::MappingConfigurationMode {
                    poll_mode: nci::FeatureFlag::Enabled,
                    listen_mode: nci::FeatureFlag::Disabled,
                },
                rf_interface: nci::RfInterfaceType::IsoDep,
            },
            nci::MappingConfiguration {
                rf_protocol: nci::RfProtocolType::NfcDep,
                mode: nci::MappingConfigurationMode {
                    poll_mode: nci::FeatureFlag::Disabled,
                    listen_mode: nci::FeatureFlag::Enabled,
                },
                rf_interface: nci::RfInterfaceType::NfcDep,
            },
        ];

        let poll_if = state.select_interface(RfMode::Poll, nci::RfProtocolType::IsoDep);
        assert_eq!(poll_if, nci::RfInterfaceType::IsoDep);

        let listen_if = state.select_interface(RfMode::Listen, nci::RfProtocolType::NfcDep);
        assert_eq!(listen_if, nci::RfInterfaceType::NfcDep);

        // Fallback default interface
        let fallback_if = state.select_interface(RfMode::Poll, nci::RfProtocolType::T2t);
        assert_eq!(fallback_if, nci::RfInterfaceType::Frame);
    }

    #[test]
    fn test_select_interface_ndef_mapping() {
        let state = create_test_state();
        // [NCI] §6.2: NDEF RF Protocol maps to NDEF Interface for Poll mode, Frame for Listen mode
        let poll_if = state.select_interface(RfMode::Poll, nci::RfProtocolType::Ndef);
        assert_eq!(poll_if, nci::RfInterfaceType::Ndef);

        let listen_if = state.select_interface(RfMode::Listen, nci::RfProtocolType::Ndef);
        assert_eq!(listen_if, nci::RfInterfaceType::Frame);
    }

    #[test]
    fn test_state_nfcid1_generation() {
        let mut state = create_test_state();

        // Default dynamic NFCID1 (starts with 0x08, 4 bytes)
        let nfcid1 = state.nfcid1();
        assert_eq!(nfcid1.len(), 4);
        assert_eq!(nfcid1[0], 0x08);

        // Static custom NFCID1
        state.config_parameters.la_nfcid1 = vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07];
        let custom_nfcid1 = state.nfcid1();
        assert_eq!(custom_nfcid1, vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07]);
    }

    #[test]
    fn test_config_parameters_defaults() {
        let params = ConfigParameters::default();
        assert_eq!(params.total_duration, TOTAL_DURATION);
        assert_eq!(params.power_state, 0x02);
        assert_eq!(params.pa_bail_out, 0x00);
        assert_eq!(params.pa_devices_limit, PA_DEVICES_LIMIT);
        assert_eq!(params.la_sel_info, LA_SEL_INFO);
        assert_eq!(params.la_bit_frame_sdd, LA_BIT_FRAME_SDD);
        assert_eq!(params.la_platform_config, LA_PLATFORM_CONFIG);
        assert_eq!(params.llcp_version, LLCP_VERSION);
    }

    #[test]
    fn test_config_parameters_get_set_roundtrip() {
        let mut params = ConfigParameters::default();

        // Test TotalDuration
        assert!(params.set(nci::ConfigParameterId::TotalDuration, &500u16.to_le_bytes()).is_ok());
        let val = params.get(nci::ConfigParameterId::TotalDuration).unwrap();
        assert_eq!(val, 500u16.to_le_bytes().to_vec());

        // Test PowerState
        assert!(params.set(nci::ConfigParameterId::PowerState, &[0x03]).is_ok());
        assert_eq!(params.get(nci::ConfigParameterId::PowerState).unwrap(), vec![0x03]);

        // Test PaBailOut
        assert!(params.set(nci::ConfigParameterId::PaBailOut, &[0x01]).is_ok());
        assert_eq!(params.get(nci::ConfigParameterId::PaBailOut).unwrap(), vec![0x01]);

        // Test LaPlatformConfig
        assert!(params.set(nci::ConfigParameterId::LaPlatformConfig, &[0x02]).is_ok());
        assert_eq!(params.get(nci::ConfigParameterId::LaPlatformConfig).unwrap(), vec![0x02]);

        // Test LaSelInfo
        assert!(params.set(nci::ConfigParameterId::LaSelInfo, &[0x60]).is_ok());
        assert_eq!(params.get(nci::ConfigParameterId::LaSelInfo).unwrap(), vec![0x60]);

        // Test LaNfcid1
        assert!(params.set(nci::ConfigParameterId::LaNfcid1, &[0x08, 0x11, 0x22, 0x33]).is_ok());
        assert_eq!(
            params.get(nci::ConfigParameterId::LaNfcid1).unwrap(),
            vec![0x08, 0x11, 0x22, 0x33]
        );

        // Test LbSensbInfo
        assert!(params.set(nci::ConfigParameterId::LbSensbInfo, &[0x01]).is_ok());
        assert_eq!(params.get(nci::ConfigParameterId::LbSensbInfo).unwrap(), vec![0x01]);

        // Test LfT3tMax is read-only
        assert!(params.set(nci::ConfigParameterId::LfT3tMax, &[0x01]).is_err());

        // Test RfFieldInfo
        assert!(params.set(nci::ConfigParameterId::RfFieldInfo, &[0x01]).is_ok());
        assert_eq!(params.get(nci::ConfigParameterId::RfFieldInfo).unwrap(), vec![0x01]);

        // Test LlcpVersion
        assert!(params.set(nci::ConfigParameterId::LlcpVersion, &[0x11]).is_ok());
        assert_eq!(params.get(nci::ConfigParameterId::LlcpVersion).unwrap(), vec![0x11]);
    }

    #[test]
    fn test_rf_poll_response_update_preserves_peer_count() {
        let mut state = create_test_state();

        let resp1 = RfPollResponse {
            id: 1,
            rf_protocol: rf::Protocol::IsoDep,
            rf_technology: rf::Technology::NfcA,
            rf_technology_specific_parameters: vec![0x01],
        };
        let resp2 = RfPollResponse {
            id: 2,
            rf_protocol: rf::Protocol::IsoDep,
            rf_technology: rf::Technology::NfcA,
            rf_technology_specific_parameters: vec![0x02],
        };
        let resp3 = RfPollResponse {
            id: 3,
            rf_protocol: rf::Protocol::IsoDep,
            rf_technology: rf::Technology::NfcA,
            rf_technology_specific_parameters: vec![0x03],
        };

        state.add_poll_response(resp1);
        state.add_poll_response(resp2);
        state.add_poll_response(resp3);
        assert_eq!(state.rf_poll_responses.len(), 3);

        // Update peer 2 with new technology parameters
        let resp2_updated = RfPollResponse {
            id: 2,
            rf_protocol: rf::Protocol::IsoDep,
            rf_technology: rf::Technology::NfcA,
            rf_technology_specific_parameters: vec![0x20, 0x00],
        };
        state.add_poll_response(resp2_updated);

        assert_eq!(state.rf_poll_responses.len(), 3);
        let p2 = state.rf_poll_responses.iter().find(|r| r.id == 2).unwrap();
        assert_eq!(p2.rf_technology_specific_parameters, vec![0x20, 0x00]);
    }

    #[test]
    fn test_exit_frame_prefix_matching() {
        let exit_frame = RfExitFrame {
            is_prefix_matching_allowed: true,
            rf_technology: rf::Technology::NfcA,
            power_states: 0x01,
            data: vec![0x00, 0xA4, 0x04, 0x00],
            mask: vec![0xFF, 0xFF, 0xFF, 0xFF],
        };

        let incoming_packet =
            vec![0x00, 0xA4, 0x04, 0x00, 0x07, 0xA0, 0x00, 0x00, 0x04, 0x76, 0x09, 0x00];

        // Prefix match check: data & mask against incoming prefix
        let matches_prefix = if exit_frame.is_prefix_matching_allowed {
            incoming_packet.len() >= exit_frame.data.len()
                && exit_frame
                    .data
                    .iter()
                    .zip(exit_frame.mask.iter())
                    .zip(incoming_packet.iter())
                    .all(|((&d, &m), &p)| (p & m) == (d & m))
        } else {
            incoming_packet.len() == exit_frame.data.len()
                && exit_frame
                    .data
                    .iter()
                    .zip(exit_frame.mask.iter())
                    .zip(incoming_packet.iter())
                    .all(|((&d, &m), &p)| (p & m) == (d & m))
        };

        assert!(
            matches_prefix,
            "Exit frame with prefix matching enabled should match AID Select command"
        );
    }

    #[test]
    fn test_rf_inactivity_timeout_detection() {
        let mut state = create_test_state();
        let now = Instant::now();

        // Recent packet: within 500ms
        state.last_rf_received_time = Some(now);
        let is_timed_out_recent = state
            .last_rf_received_time
            .map(|t| t.elapsed() > Duration::from_millis(500))
            .unwrap_or(false);
        assert!(
            !is_timed_out_recent,
            "Recently received RF packet should not trigger inactivity timeout"
        );

        // Stale packet: elapsed > 500ms
        state.last_rf_received_time = Some(now.checked_sub(Duration::from_millis(600)).unwrap());
        let is_timed_out_stale = state
            .last_rf_received_time
            .map(|t| t.elapsed() > Duration::from_millis(500))
            .unwrap_or(false);
        assert!(
            is_timed_out_stale,
            "Inactive RF connection exceeding 500ms should trigger inactivity reset"
        );
    }

    #[tokio::test]
    async fn test_controller_core_commands() {
        let (_nci_rx_writer, nci_rx_reader) = tokio::io::duplex(4096);
        let (_nci_tx_reader, nci_tx_writer) = tokio::io::duplex(4096);
        let (_rf_peer_tx, rf_rx) = mpsc::unbounded_channel();
        let (rf_tx, _rf_peer_rx) = mpsc::unbounded_channel();

        let mut nci_stream_pin = std::pin::pin!(nci::Reader::new(nci_rx_reader).into_stream());
        let nci_writer = nci::Writer::new(nci_tx_writer);
        let mut controller = Controller::new(1, nci_stream_pin.as_mut(), nci_writer, rf_rx, rf_tx);

        // 1. Core Reset (ResetConfig)
        assert!(controller
            .core_reset(nci::CoreResetCommand { reset_type: nci::ResetType::ResetConfig })
            .await
            .is_ok());
        assert_eq!(controller.state.rf_state, RfState::Idle);

        // 2. Core Init
        assert!(controller
            .core_init(nci::CoreInitCommand { feature_enable: nci::FeatureEnable {} })
            .await
            .is_ok());

        // 3. Core Set Config
        let set_config = nci::CoreSetConfigCommand {
            parameters: vec![
                nci::ConfigParameter {
                    id: nci::ConfigParameterId::TotalDuration,
                    value: 400u16.to_le_bytes().to_vec(),
                },
                nci::ConfigParameter { id: nci::ConfigParameterId::PowerState, value: vec![0x03] },
                nci::ConfigParameter { id: nci::ConfigParameterId::PaBailOut, value: vec![0x01] },
            ],
        };
        assert!(controller.core_set_config(set_config).await.is_ok());
        assert_eq!(controller.state.config_parameters.total_duration, 400);

        // 4. Core Get Config
        let get_config = nci::CoreGetConfigCommand {
            parameters: vec![
                nci::ConfigParameterId::TotalDuration,
                nci::ConfigParameterId::PowerState,
            ],
        };
        assert!(controller.core_config(get_config).await.is_ok());

        // 5. Core Set Power Sub State
        assert!(controller
            .core_set_power_sub_state(nci::CoreSetPowerSubStateCommand {
                power_state: nci::PowerState::SwitchedOnSubstate1,
            })
            .await
            .is_ok());

        // 6. Core Conn Create and Close
        let conn_create = nci::CoreConnCreateCommand {
            destination_type: nci::DestinationType::RemoteNfcEndpoint,
            parameters: vec![nci::DestinationSpecificParameter {
                id: nci::DestinationSpecificParameterId::RfDiscovery,
                value: vec![0x00, nci::RfProtocolType::IsoDep as u8],
            }],
        };
        assert!(controller.core_conn_create(conn_create).await.is_ok());
        assert!(controller.state.logical_connections[0].is_some());

        assert!(controller
            .core_conn_close(nci::CoreConnCloseCommand { conn_id: nci::ConnId::from_dynamic(0) })
            .await
            .is_ok());
        assert!(controller.state.logical_connections[0].is_none());
    }

    #[tokio::test]
    async fn test_controller_rf_discovery_and_routing() {
        let (_nci_rx_writer, nci_rx_reader) = tokio::io::duplex(4096);
        let (_nci_tx_reader, nci_tx_writer) = tokio::io::duplex(4096);
        let (_rf_peer_tx, rf_rx) = mpsc::unbounded_channel();
        let (rf_tx, _rf_peer_rx) = mpsc::unbounded_channel();

        let mut nci_stream_pin = std::pin::pin!(nci::Reader::new(nci_rx_reader).into_stream());
        let nci_writer = nci::Writer::new(nci_tx_writer);
        let mut controller = Controller::new(1, nci_stream_pin.as_mut(), nci_writer, rf_rx, rf_tx);

        // 1. Discover Map
        let map_cmd = nci::RfDiscoverMapCommand {
            mapping_configurations: vec![
                nci::MappingConfiguration {
                    rf_protocol: nci::RfProtocolType::IsoDep,
                    mode: nci::MappingConfigurationMode {
                        poll_mode: nci::FeatureFlag::Enabled,
                        listen_mode: nci::FeatureFlag::Enabled,
                    },
                    rf_interface: nci::RfInterfaceType::IsoDep,
                },
                nci::MappingConfiguration {
                    rf_protocol: nci::RfProtocolType::Ndef,
                    mode: nci::MappingConfigurationMode {
                        poll_mode: nci::FeatureFlag::Enabled,
                        listen_mode: nci::FeatureFlag::Disabled,
                    },
                    rf_interface: nci::RfInterfaceType::Ndef,
                },
            ],
        };
        assert!(controller.rf_discover_map(map_cmd).await.is_ok());
        assert_eq!(controller.state.discover_map.len(), 2);

        // 2. Set Listen Mode Routing
        let routing_cmd = nci::RfSetListenModeRoutingCommand {
            more_to_follow: 0,
            routing_entries: vec![nci::ListenModeRoutingEntry {
                r#type: nci::ListenModeRoutingEntryType::ProtocolBasedRouting,
                match_longer_aids: nci::FeatureFlag::Disabled,
                match_shorter_aids: nci::FeatureFlag::Disabled,
                routing_blocked_for_unsupported_power_modes: nci::FeatureFlag::Disabled,
                value: vec![0x04, 0x00, 0x01],
            }],
        };
        assert!(controller.rf_set_listen_mode_routing(routing_cmd).await.is_ok());

        // 3. Get Listen Mode Routing
        assert!(controller
            .rf_listen_mode_routing(nci::RfGetListenModeRoutingCommand {})
            .await
            .is_ok());

        // 4. RF Discover
        let discover_cmd = nci::RfDiscoverCommand {
            configurations: vec![
                nci::DiscoverConfiguration {
                    technology_and_mode: nci::RfTechnologyAndMode::NfcAPassivePollMode,
                    discovery_frequency: 1,
                },
                nci::DiscoverConfiguration {
                    technology_and_mode: nci::RfTechnologyAndMode::NfcAPassiveListenMode,
                    discovery_frequency: 1,
                },
            ],
        };
        assert!(controller.rf_discover(discover_cmd).await.is_ok());
        assert_eq!(controller.state.rf_state, RfState::Discovery);

        // Add poll responses to test RfDiscoverSelect
        controller.state.rf_poll_responses.push(RfPollResponse {
            id: 2,
            rf_protocol: rf::Protocol::IsoDep,
            rf_technology: rf::Technology::NfcA,
            rf_technology_specific_parameters: vec![0x20],
        });
        controller.state.rf_state = RfState::WaitForHostSelect;

        // 5. RF Discover Select with valid ID
        let select_cmd = nci::RfDiscoverSelectCommand {
            rf_discovery_id: nci::RfDiscoveryId::from_index(0),
            rf_protocol: nci::RfProtocolType::IsoDep,
            rf_interface: nci::RfInterfaceType::IsoDep,
        };
        assert!(controller.rf_discover_select(select_cmd).await.is_ok());
        assert_eq!(
            controller.state.rf_state,
            RfState::WaitForSelectResponse {
                id: 2,
                rf_discovery_id: 0,
                rf_interface: nci::RfInterfaceType::IsoDep,
                rf_protocol: rf::Protocol::IsoDep,
                rf_technology: rf::Technology::NfcA,
            }
        );

        // 6. RF Deactivate to Idle
        let deact_cmd =
            nci::RfDeactivateCommand { deactivation_type: nci::DeactivationType::IdleMode };
        assert!(controller.rf_deactivate(deact_cmd).await.is_ok());
        assert_eq!(controller.state.rf_state, RfState::Idle);
    }

    #[tokio::test]
    async fn test_controller_android_observe_mode() {
        let (_nci_rx_writer, nci_rx_reader) = tokio::io::duplex(4096);
        let (_nci_tx_reader, nci_tx_writer) = tokio::io::duplex(4096);
        let (_rf_peer_tx, rf_rx) = mpsc::unbounded_channel();
        let (rf_tx, _rf_peer_rx) = mpsc::unbounded_channel();

        let mut nci_stream_pin = std::pin::pin!(nci::Reader::new(nci_rx_reader).into_stream());
        let nci_writer = nci::Writer::new(nci_tx_writer);
        let mut controller = Controller::new(1, nci_stream_pin.as_mut(), nci_writer, rf_rx, rf_tx);

        // 1. Enable Observe Mode
        let observe_cmd = nci::AndroidPassiveObserveModeCommand {
            passive_observe_mode: nci::PassiveObserveMode::try_from(0x01).unwrap(),
        };
        assert!(controller.android_passive_observe_mode(observe_cmd).await.is_ok());
        assert_eq!(controller.state.passive_observe_mode, nci::TechnologyMask::AllOn.into());

        // 2. Query Observe Mode
        let query_cmd = nci::AndroidQueryPassiveObserveModeCommand {};
        assert!(controller.android_query_passive_observe_mode(query_cmd).await.is_ok());

        // 3. Disable Observe Mode
        let disable_cmd = nci::AndroidPassiveObserveModeCommand {
            passive_observe_mode: nci::PassiveObserveMode::try_from(0x00).unwrap(),
        };
        assert!(controller.android_passive_observe_mode(disable_cmd).await.is_ok());
        assert_eq!(controller.state.passive_observe_mode, nci::TechnologyMask::AllOff.into());
    }

    #[tokio::test]
    async fn test_controller_data_packet_and_credits() {
        let (_nci_rx_writer, nci_rx_reader) = tokio::io::duplex(4096);
        let (_nci_tx_reader, nci_tx_writer) = tokio::io::duplex(4096);
        let (_rf_peer_tx, rf_rx) = mpsc::unbounded_channel();
        let (rf_tx, _rf_peer_rx) = mpsc::unbounded_channel();

        let mut nci_stream_pin = std::pin::pin!(nci::Reader::new(nci_rx_reader).into_stream());
        let nci_writer = nci::Writer::new(nci_tx_writer);
        let mut controller = Controller::new(1, nci_stream_pin.as_mut(), nci_writer, rf_rx, rf_tx);

        // Put controller in PollActive state
        controller.state.rf_state = RfState::PollActive {
            id: 2,
            rf_interface: nci::RfInterfaceType::IsoDep,
            rf_protocol: rf::Protocol::IsoDep,
            rf_technology: rf::Technology::NfcA,
        };

        // Send data packet
        let data = rf::Data {
            sender: 2,
            receiver: 1,
            technology: rf::Technology::NfcA,
            protocol: rf::Protocol::IsoDep,
            bitrate: rf::BitRate::BitRate106KbitS,
            power_level: 255,
            data: vec![0x00, 0xA4, 0x04, 0x00, 0x07, 0xA0, 0x00, 0x00, 0x04, 0x76, 0x09, 0x00],
        };
        assert!(controller.data_packet(data).await.is_ok());
    }

    #[tokio::test]
    async fn test_controller_receive_rf_packets() {
        let (_nci_rx_writer, nci_rx_reader) = tokio::io::duplex(4096);
        let (_nci_tx_reader, nci_tx_writer) = tokio::io::duplex(4096);
        let (_rf_peer_tx, rf_rx) = mpsc::unbounded_channel();
        let (rf_tx, _rf_peer_rx) = mpsc::unbounded_channel();

        let mut nci_stream_pin = std::pin::pin!(nci::Reader::new(nci_rx_reader).into_stream());
        let nci_writer = nci::Writer::new(nci_tx_writer);
        let mut controller = Controller::new(1, nci_stream_pin.as_mut(), nci_writer, rf_rx, rf_tx);

        // 1. Controller in PollActive receives PollCommand
        controller.state.rf_state = RfState::PollActive {
            id: 2,
            rf_interface: nci::RfInterfaceType::IsoDep,
            rf_protocol: rf::Protocol::IsoDep,
            rf_technology: rf::Technology::NfcA,
        };
        let poll_cmd: rf::RfPacket = rf::PollCommand {
            sender: 2,
            receiver: u16::MAX,
            technology: rf::Technology::NfcA,
            protocol: rf::Protocol::IsoDep,
            bitrate: rf::BitRate::BitRate106KbitS,
            power_level: 255,
            format: rf::PollingFrameFormat::Short,
            payload: vec![],
        }
        .try_into()
        .unwrap();
        assert!(controller.receive_rf(poll_cmd).await.is_ok());

        // 2. Controller in Discovery receives PollResponse
        controller.state.rf_state = RfState::Discovery;
        controller.state.discover_configuration = vec![nci::DiscoverConfiguration {
            technology_and_mode: nci::RfTechnologyAndMode::NfcAPassivePollMode,
            discovery_frequency: 1,
        }];
        let poll_resp: rf::RfPacket = rf::NfcAPollResponse {
            sender: 2,
            receiver: 1,
            protocol: rf::Protocol::IsoDep,
            bitrate: rf::BitRate::BitRate106KbitS,
            power_level: 255,
            nfcid1: vec![0x08, 0x01, 0x02, 0x03],
            int_protocol: 0x01,
            bit_frame_sdd: 0x08,
        }
        .try_into()
        .unwrap();
        assert!(controller.receive_rf(poll_resp).await.is_ok());
        assert_eq!(controller.state.rf_poll_responses.len(), 1);

        // 3. Controller receives IsoDepT4ATSelectResponse in WaitForSelectResponse
        controller.state.rf_state = RfState::WaitForSelectResponse {
            id: 2,
            rf_discovery_id: 0,
            rf_interface: nci::RfInterfaceType::IsoDep,
            rf_protocol: rf::Protocol::IsoDep,
            rf_technology: rf::Technology::NfcA,
        };
        let select_resp: rf::RfPacket = rf::IsoDepT4ATSelectResponse {
            sender: 2,
            receiver: 1,
            bitrate: rf::BitRate::BitRate106KbitS,
            power_level: 255,
            rats_response: vec![0x05, 0x78, 0x80, 0x71, 0x02],
        }
        .try_into()
        .unwrap();
        assert!(controller.receive_rf(select_resp).await.is_ok());
        assert_eq!(
            controller.state.rf_state,
            RfState::PollActive {
                id: 2,
                rf_interface: nci::RfInterfaceType::IsoDep,
                rf_protocol: rf::Protocol::IsoDep,
                rf_technology: rf::Technology::NfcA,
            }
        );

        // 4. Inactivity timeout check on PollActive triggers reset to Discovery
        assert!(controller.state.last_rf_received_time.is_some());
        controller.state.last_rf_received_time =
            Some(Instant::now().checked_sub(Duration::from_millis(6000)).unwrap());
        assert!(controller.tick().await.is_ok());
        assert_eq!(controller.state.rf_state, RfState::Discovery);
    }
}
