#!/usr/bin/env python3

# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import inspect
import json
import random
import readline
import socket
import sys
import time
import requests
import struct
import asyncio
from concurrent.futures import ThreadPoolExecutor

import rf_packets as rf


class T4AT:
    def __init__(self, reader, writer):
        self.nfcid1 = bytes([0x08]) + int.to_bytes(random.randint(0, 0xffffff), length=3)
        self.rats_response = bytes([0x2, 0x0])
        self.reader = reader
        self.writer = writer

    async def _read(self) -> rf.RfPacket:
        header_bytes = await self.reader.read(2)
        packet_length = int.from_bytes(header_bytes, byteorder='little')
        packet_bytes = await self.reader.read(packet_length)

        packet = rf.RfPacket.parse_all(packet_bytes)
        packet.show()
        return packet

    def _write(self, packet: rf.RfPacket):
        packet_bytes = packet.serialize()
        header_bytes = int.to_bytes(len(packet_bytes), length=2, byteorder='little')
        self.writer.write(header_bytes + packet_bytes)

    async def discovery(self):
        """Discovery mode. Respond to poll requests until the device
        is activated by a select command."""
        while True:
            packet = await self._read()
            match packet:
                case rf.PollCommand(technology=rf.Technology.NFC_A):
                    self._write(rf.NfcAPollResponse(
                        nfcid1=self.nfcid1, int_protocol=0b01))
                case rf.T4ATSelectCommand(_):
                    self._write(rf.T4ATSelectResponse(
                        rats_response=self.rats_response,
                        receiver=packet.sender))
                    print(f"t4at device selected by #{packet.sender}")
                    await self.active(packet.sender)
                case _:
                    pass

    async def active(self, peer: int):
        """Active mode. Respond to data requests until the device
        is deselected."""
        while True:
            packet = await self._read()
            match packet:
                case rf.DeactivateNotification(_):
                    return
                case rf.Data(_):
                    pass
                case _:
                    pass


async def run(address: str, rf_port: int):
    """Emulate a T4AT compatible device in Listen mode."""
    try:
        reader, writer = await asyncio.open_connection(address, rf_port)
        device = T4AT(reader, writer)
        await device.discovery()
    except Exception as exn:
        print(
            f'Failed to connect to Casimir server at address {address}:{rf_port}:\n' +
            f'    {exn}\n' +
            'Make sure the server is running')
        exit(1)


def main():
    """Start a Casimir interactive console."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--address',
                        type=str,
                        default='127.0.0.1',
                        help='Select the casimir server address')
    parser.add_argument('--rf-port',
                        type=int,
                        default=7001,
                        help='Select the casimir TCP RF port')
    asyncio.run(run(**vars(parser.parse_args())))


if __name__ == '__main__':
    main()
