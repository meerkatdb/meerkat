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

use bitpacking::{BitPacker, BitPacker4x};

use crate::store::block_encoders::util::{ceil8, round_upto_power_of_2};

pub struct OffsetEncoder {
    bitpacker: BitPacker4x,
}

impl OffsetEncoder {
    pub fn new() -> Self {
        Self {
            bitpacker: BitPacker4x::new(),
        }
    }

    //
    pub fn encode(&mut self, src: &mut Vec<u32>, dst: &mut Vec<u8>) {
        let num_delta = src.len();

        let len_padded = round_upto_power_of_2(src.len(), BitPacker4x::BLOCK_LEN);
        src.resize(len_padded, 0);

        let num_bits = self.bitpacker.num_bits(src);

        dst.push(num_bits);

        let encoded_data_start = dst.len();

        let mut chunk_start = encoded_data_start;

        dst.resize(dst.len() + ceil8(num_bits as usize * len_padded), 0);

        for chunk in src.chunks(BitPacker4x::BLOCK_LEN) {
            let compressed_len = self
                .bitpacker
                .compress(chunk, &mut dst[chunk_start..], num_bits);
            chunk_start += compressed_len;
        }

        // trim to remove the encoded padding.
        let encoded_data_len = encoded_data_start + ceil8(num_bits as usize * num_delta);

        dst.resize(encoded_data_len, 0);
    }
}
