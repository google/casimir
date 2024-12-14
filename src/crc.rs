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

//! NFC CRC calculation
//! Based on ISO/IEC_13239 (FCS 16 bit, CCITT)
//! ISO/IEC_15693-3 Annex C used for additional reference

use crate::rf;

/// Error describing why CRC verification failed
pub enum CrcVerificationError {
    /// Invalid input length (<= 2 bytes) for CRC check
    InvalidInputLength,
    /// Type F & V are not covered here yet (TODO)
    UnsupportedTechnology,
    /// CRC Verification failed (CRC invalid or missing)
    VerificationFailed,
}

fn crc16(data: &[u8], initial: u16, invert: bool) -> (u8, u8) {
    let mut w_crc: u16 = initial;

    for &byte in data {
        let mut temp: u16 = (byte as u16) ^ (w_crc & 0x00FF);
        temp = (temp ^ (temp << 4)) & 0xFF;
        w_crc = (w_crc >> 8) ^ (temp << 8) ^ (temp << 3) ^ (temp >> 4);
    }
    if invert {
        w_crc = !w_crc;
    }
    ((w_crc & 0xFF) as u8, ((w_crc >> 8) & 0xFF) as u8)
}

fn crc16a(data: &[u8]) -> (u8, u8) {
    // [DIGITAL]
    // The CRC_A MUST be calculated as defined in [ISO/IEC_13239],
    // but the initial register content MUST be 6363h
    // and the register content MUST not be inverted after calculation
    crc16(data, 0x6363, false)
}

fn crc16b(data: &[u8]) -> (u8, u8) {
    // [DIGITAL]
    // The CRC_B MUST be calculated as defined in [ISO/IEC_13239].
    // The initial register content MUST be all ones (FFFFh)
    crc16(data, 0xFFFF, true)
}

/// Verify CRC is valid for provided NFC technology
pub fn verify_crc(technology: rf::Technology, data: &[u8]) -> Result<(), CrcVerificationError> {
    if data.len() <= 2 {
        return Err(CrcVerificationError::InvalidInputLength);
    }

    let (crc_lo, crc_hi) = match technology {
        rf::Technology::NfcA => crc16a(&data[..data.len() - 2]),
        rf::Technology::NfcB => crc16b(&data[..data.len() - 2]),
        _ => return Err(CrcVerificationError::UnsupportedTechnology),
    };

    if crc_lo == data[data.len() - 2] && crc_hi == data[data.len() - 1] {
        Ok(())
    } else {
        Err(CrcVerificationError::VerificationFailed)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_verify_crc_type_a() {
        // Check validity of CRC16A calculation based on HLTA (SLP_REQ)
        assert!(verify_crc(rf::Technology::NfcA, &[0x50, 0x00, 0x57, 0xcd]).is_ok());
    }

    #[test]
    fn test_verify_crc_type_b() {
        // Check validity of crc16b based on REQB (SENSB_REQ)
        assert!(verify_crc(rf::Technology::NfcB, &[0x05, 0x00, 0x00, 0x71, 0xff]).is_ok());
        // Check validity of crc16b based on WUPB (ALLB_REQ)
        assert!(verify_crc(rf::Technology::NfcB, &[0x05, 0x00, 0x08, 0x39, 0x73]).is_ok());
    }

    #[test]
    fn test_verify_crc_invalid_length() {
        assert!(matches!(
            verify_crc(rf::Technology::NfcA, &[0x00]),
            Err(CrcVerificationError::InvalidInputLength)
        ));
        assert!(matches!(
            verify_crc(rf::Technology::NfcB, &[0x00, 0x01]),
            Err(CrcVerificationError::InvalidInputLength)
        ));
    }

    #[test]
    fn test_verify_crc_verification_failed() {
        // HLTA must end with 57CDh
        assert!(matches!(
            verify_crc(rf::Technology::NfcA, &[0x50, 0x00, 0x00, 0x00]),
            Err(CrcVerificationError::VerificationFailed)
        ));
        // REQB must end with 71ffh
        assert!(matches!(
            verify_crc(rf::Technology::NfcB, &[0x05, 0x00, 0x00, 0x00, 0x00]),
            Err(CrcVerificationError::VerificationFailed)
        ));
    }
}
