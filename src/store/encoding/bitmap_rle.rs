// Copyright 2021 The Meerkat Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! This module contains a byte-aligned bitmap RLE compressor

use std::convert::TryInto;

use crate::store::encoding::varint;
use crate::store::indexing_buffer::Bitmap;

const BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];
const MIN_RUN_LEN: usize = 3;

#[derive(Debug)]
enum State {
    Init,
    Literal,
    StartOfRun,
    Run,
}

/// byte-aligned bitmap RLE encoder
pub struct Encoder {
    encoded_values: Vec<u8>,
    byte: u8,
    bit_pos: usize,
    buffer: Vec<u8>,
    repeat_count: usize,
    last_value: u8,
    run_start_pos: usize,
    state: State,
}

impl Encoder {
    pub fn new() -> Self {
        Self {
            encoded_values: Vec::new(),
            byte: 0,
            bit_pos: 0,
            buffer: vec![],
            repeat_count: 0,
            last_value: 0,
            run_start_pos: 0,
            state: State::Init,
        }
    }

    /// encode a single bit
    pub fn put(&mut self, set: bool) {
        if set {
            self.byte |= BIT_MASK[self.bit_pos];
        }
        self.bit_pos += 1;
        if self.bit_pos == 8 {
            self.bit_pos = 0;
            self.append(self.byte);
            self.byte = 0;
        }
    }

    pub fn put_from(&mut self, bitmap: &Bitmap, start_pos: usize, num_valids: usize) -> usize {
        let mut items = 0;
        let mut bit_pos = start_pos;
        while items < num_valids {
            let bit_value = bitmap.is_set(bit_pos);
            self.put(bit_value);
            items += bit_value as usize;
            bit_pos += 1;
        }
        bit_pos
    }

    /// flush the internal buffer and encode pending values.
    /// once the encoder is flushed use `encoded_values()` to access the encoded
    /// buffer.
    pub fn flush(&mut self) {
        if self.bit_pos > 0 {
            self.append(self.byte);
        }

        match self.state {
            State::Init => {}
            State::Literal => {
                self.emit_bitmap_literal(self.buffer.len());
            }
            State::StartOfRun => {
                self.emit_bitmap_literal(self.buffer.len());
            }
            State::Run => {
                self.emit_run(self.repeat_count, self.last_value);
            }
        }
    }

    /// return a slice containing the compressed bitmap.
    pub fn encoded_values(&self) -> &[u8] {
        &self.encoded_values
    }

    /// clear the internal state.
    pub fn clear(&mut self) {
        self.state = State::Init;
        self.buffer.clear();
        self.encoded_values.clear();
        self.run_start_pos = 0;
        self.byte = 0;
        self.repeat_count = 0;
        self.bit_pos = 0;
    }

    fn append(&mut self, current_value: u8) {
        match self.state {
            State::Init => {
                self.state = State::Literal;
                self.buffer.push(current_value)
            }
            State::Literal => {
                if self.last_value == current_value {
                    self.repeat_count = 2;
                    self.run_start_pos = self.buffer.len() - 1;
                    self.state = State::StartOfRun
                }
                self.buffer.push(current_value);
            }
            State::StartOfRun => {
                self.buffer.push(current_value);
                if self.last_value != current_value {
                    self.transition_to_literal();
                } else {
                    self.repeat_count += 1;
                    if self.repeat_count == MIN_RUN_LEN {
                        if self.run_start_pos > 0 {
                            self.emit_bitmap_literal(self.run_start_pos);
                        }
                        self.buffer.clear();
                        self.state = State::Run
                    }
                }
            }
            State::Run => {
                if self.last_value != current_value {
                    self.emit_run(self.repeat_count, self.last_value);
                    self.buffer.push(current_value);
                    self.transition_to_literal();
                } else {
                    self.repeat_count += 1;
                }
            }
        }
        self.last_value = current_value;
    }

    fn transition_to_literal(&mut self) {
        self.repeat_count = 0;
        self.run_start_pos = 0;
        self.state = State::Literal
    }

    fn emit_bitmap_literal(&mut self, bitmap_len: usize) {
        if self.buffer.is_empty() {
            return;
        }

        let mut literal_header_bytes = [0u8; varint::MAX_VARINT_LEN];

        let literal_header_len =
            varint::encode_varint(((bitmap_len << 1) | 1) as u64, &mut literal_header_bytes);

        self.encoded_values
            .extend_from_slice(&literal_header_bytes[..literal_header_len]);

        self.encoded_values
            .extend_from_slice(&self.buffer[..bitmap_len]);
    }

    fn emit_run(&mut self, run_len: usize, run_value: u8) {
        let mut run_header_bytes = [0u8; varint::MAX_VARINT_LEN];

        let run_header_len =
            varint::encode_varint((run_len << 1) as u64, &mut run_header_bytes[..]);

        self.encoded_values
            .extend_from_slice(&run_header_bytes[..run_header_len]);

        self.encoded_values.push(run_value);
    }
}

#[derive(Debug)]
enum Frame<'a> {
    Run { len: u32, value: u8 },
    Bitmap { bytes: &'a [u8] },
}

/// byte-aligned bitmap RLE decoder.
/// The current implementation decode the bitmap to a sorted lists of integers
/// corresponding to the positions of the set bits.
struct Decoder<'a> {
    encoded_data: &'a [u8],
    decoded_data: &'a mut [u32],
    encoded_data_pos: usize,
    decoded_data_pos: usize,
    bitmap_pos: u32,
}

impl<'a> Decoder<'a> {
    /// Return a new decoder.
    /// `decoded_data` should have enough room to accommodate the decoded values
    /// otherwise `decode()` will panic.
    pub fn new(encoded_data: &'a [u8], decoded_data: &'a mut [u32]) -> Self {
        Self {
            encoded_data,
            decoded_data,
            encoded_data_pos: 0,
            decoded_data_pos: 0,
            bitmap_pos: 0,
        }
    }
    /// Decode the internal buffer and return the number of values decoded.
    pub fn decode(&mut self) -> usize {
        while self.encoded_data_pos < self.encoded_data.len() {
            match self.read_frame() {
                Frame::Run { len, value } => self.decode_run(len, value),
                Frame::Bitmap { bytes } => self.decode_bitmap(bytes),
            }
        }
        self.decoded_data_pos
    }

    pub fn read_frame(&mut self) -> Frame<'a> {
        let (header, bytes_read) =
            varint::decode_varint(&self.encoded_data[self.encoded_data_pos..])
                .expect("cannot decode bitmap");

        self.encoded_data_pos += bytes_read;

        let len: u32 = (header >> 1)
            .try_into()
            .expect("cannot decode frame, frame len overflow");

        if header & 1 == 1 {
            let bitmap_start = self.encoded_data_pos;
            let bitmap_end = self.encoded_data_pos + len as usize;
            self.encoded_data_pos += len as usize;
            return Frame::Bitmap {
                bytes: &self.encoded_data[bitmap_start..bitmap_end],
            };
        };

        let frame = Frame::Run {
            len: len,
            value: self.encoded_data[self.encoded_data_pos],
        };

        self.encoded_data_pos += 1;

        frame
    }

    fn decode_run(&mut self, len: u32, value: u8) {
        match value {
            0x00 => self.bitmap_pos += len << 3,
            0xFF => {
                let bitmap_len = len << 3;
                for _ in 0..bitmap_len {
                    self.decoded_data[self.decoded_data_pos] = self.bitmap_pos;
                    self.bitmap_pos += 1;
                    self.decoded_data_pos += 1;
                }
            }
            _ => {
                for _ in 0..len {
                    self.decode_byte(value)
                }
            }
        }
    }

    fn decode_bitmap(&mut self, bitmap: &[u8]) {
        for byte in bitmap {
            self.decode_byte(*byte);
        }
    }

    fn decode_byte(&mut self, byte: u8) {
        for mask in BIT_MASK {
            if byte & mask != 0 {
                self.decoded_data[self.decoded_data_pos] = self.bitmap_pos;
                self.decoded_data_pos += 1;
            };
            self.bitmap_pos += 1;
        }
    }
}

pub fn decode(encoded_data: &[u8], decoded_data: &mut [u32]) {
    let mut decoder = Decoder::new(encoded_data, decoded_data);
    decoder.decode();
}

#[cfg(test)]
mod test {
    use super::*;

    fn bit_positions(values: &[bool]) -> Vec<u32> {
        values
            .iter()
            .enumerate()
            .filter(|elem| *elem.1)
            .map(|elem| elem.0 as u32)
            .collect()
    }

    fn roundtrip_test(values: &[bool]) -> Vec<u8> {
        let bit_positions = bit_positions(values);

        let mut encoder = Encoder::new();

        for value in values {
            encoder.put(*value);
        }

        encoder.flush();

        let mut decoded_values = vec![0u32; bit_positions.len()];

        let mut decoder = Decoder::new(encoder.encoded_values(), decoded_values.as_mut_slice());
        let num_decoded_values = decoder.decode();

        assert_eq!(num_decoded_values, decoded_values.len());

        for (actual, expected) in decoded_values.iter().zip(bit_positions.iter()) {
            assert_eq!(actual, expected);
        }
        encoder.encoded_values().to_owned()
    }

    #[test]
    fn test_roundtrip_run_1() {
        let values = &[true; 24];
        let encoded = roundtrip_test(values);
        assert_eq!(encoded.len(), 2);
    }

    #[test]
    fn test_roundtrip_run_0() {
        let values = &[false; 24];
        let encoded = roundtrip_test(values);
        assert_eq!(encoded.len(), 2);
    }

    #[test]
    fn test_test_literal() {
        let mut values: Vec<bool> = Vec::new();
        values.extend_from_slice(&[false; 8]);
        values.extend_from_slice(&[true; 8]);
        values.extend_from_slice(&[false; 8]);
        let encoded = roundtrip_test(values.as_slice());
        assert_eq!(encoded.len(), 4);
    }

    #[test]
    fn test_test_literal_run_literal() {
        let mut values: Vec<bool> = Vec::new();
        values.extend_from_slice(&[false; 4]);
        values.extend_from_slice(&[true; 100]);
        values.extend_from_slice(&[false; 14]);
        values.extend_from_slice(&[true; 14]);
        let encoded = roundtrip_test(values.as_slice());
        assert_eq!(encoded.len(), 9); // 2(lit) + 2(run) + 5(lit)
    }
}
