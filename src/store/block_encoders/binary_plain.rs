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

/// Binary plain block
///
/// ```text
/// ┌─────────────┬────────────┬───────────────────┬─────────────────────────┬──────────────┐
/// │ Data Lenght │    Data    │ Bitpack Num Bytes │ Bitpacked Delta Offsets │ RLE validity │
/// │   Varint    │  N bytes   │      1 Byte       │         N bytes         │    N bytes   │
/// └─────────────┴────────────┴───────────────────┴─────────────────────────┴──────────────┘
/// ```
///
use std::convert::TryInto;
use std::ops::Range;

use anyhow::Result;
use async_trait::async_trait;
use bitpacking::{BitPacker, BitPacker4x};
use tokio::test;

use crate::store::block_encoders::bitmap_rle;
use crate::store::block_encoders::offsets::OffsetEncoder;
use crate::store::block_encoders::util::ceil8;
use crate::store::block_encoders::{BlockEncoder, BlockSink};
use crate::store::indexing_buffer::{BinaryBuffer, BinaryChunk, Bitmap};
use crate::store::segment_metadata::column_layout::EncoderLayout;
use crate::store::segment_metadata::NoLayout;

use super::util;
use super::varint::Encoder as VarintEncoder;

pub struct Encoder {
    remaining: BinaryBuffer,
    encoded_data: Vec<u8>,
    offset_deltas: Vec<u32>,
    bitmap_encoder: bitmap_rle::Encoder,
    offset_encoder: OffsetEncoder,
    min_block_size: u64,
    num_rows: u32,
}

impl Encoder {
    pub fn new(block_size: u32, nullable: bool) -> Self {
        Self {
            remaining: BinaryBuffer::new(nullable),
            encoded_data: Vec::with_capacity(block_size as usize),
            offset_deltas: Vec::with_capacity(1024),
            bitmap_encoder: bitmap_rle::Encoder::new(),
            offset_encoder: OffsetEncoder::new(),
            min_block_size: block_size as u64,
            num_rows: 0,
        }
    }

    async fn write_block<S: BlockSink + Send>(
        &mut self,
        buf: &BinaryBuffer,
        chunk: &BinaryChunk,
        mut bit_pos: usize,
        sink: &mut S,
    ) -> Result<EncodeResult> {
        let result = encode_binary_buffer(
            buf,
            chunk,
            bit_pos,
            &mut self.encoded_data,
            &mut self.offset_deltas,
            &mut self.bitmap_encoder,
            &mut self.offset_encoder,
        );

        self.add_rows(result.num_rows);

        sink.write_block(self.num_rows - 1, &self.encoded_data)
            .await?;

        Ok(result)
    }

    async fn write_remaining<S: BlockSink + Send>(&mut self, sink: &mut S) -> Result<EncodeResult> {
        let chunk = BinaryChunk {
            start_pos: 0,
            end_pos: self.remaining.offsets().len() - 1,
            start_offset: 0,
            end_offset: *self.remaining.offsets().last().unwrap(),
            size: *self.remaining.offsets().last().unwrap(),
            len: self.remaining.len(),
        };

        let result = encode_binary_buffer(
            &self.remaining,
            &chunk,
            0,
            &mut self.encoded_data,
            &mut self.offset_deltas,
            &mut self.bitmap_encoder,
            &mut self.offset_encoder,
        );

        self.add_rows(result.num_rows);

        sink.write_block(self.num_rows - 1, &self.encoded_data)
            .await?;

        Ok(result)
    }

    fn add_rows(&mut self, num_rows: usize) {
        self.num_rows = self
            .num_rows
            .checked_add((num_rows).try_into().expect("segment overflow"))
            .expect("segment overflow");
    }
}

#[async_trait]
impl<S: BlockSink + Send> BlockEncoder<BinaryBuffer, S> for Encoder {
    async fn encode(&mut self, buffer: &BinaryBuffer, sink: &mut S) -> Result<()> {
        let mut buf_pos = 0;
        let mut validity_pos = 0;

        if self.remaining.len() > 0 {
            let remaining_size = self.remaining.data().len();

            let chunk = buffer.chunk(0, self.min_block_size - remaining_size as u64);

            validity_pos = self.remaining.append_from(buffer, &chunk, 0);
            buf_pos = chunk.end_pos;

            if self.remaining.data().len() < self.min_block_size as usize {
                return Ok(());
            }

            self.write_remaining(sink).await?;

            self.remaining.clear();
        }

        while buf_pos < buffer.offsets().len() {
            let chunk = buffer.chunk(buf_pos, self.min_block_size);
            if (chunk.size as u64) < self.min_block_size {
                self.remaining.append_from(buffer, &chunk, validity_pos);
                return Ok(());
            }

            let result = self.write_block(buffer, &chunk, validity_pos, sink).await?;

            buf_pos = chunk.end_pos;
            validity_pos = result.last_validity_pos;
        }

        Ok(())
    }

    async fn flush(&mut self, sink: &mut S) -> Result<EncoderLayout> {
        if self.remaining.len() > 0 {
            self.write_remaining(sink).await?;
        }

        Ok(EncoderLayout::Plain(NoLayout {}))
    }
}

#[derive(Debug)]
struct EncodeResult {
    num_rows: usize,
    last_validity_pos: usize,
}

fn encode_binary_buffer(
    buf: &BinaryBuffer,
    chunk: &BinaryChunk,
    bit_pos: usize,
    encoded_data: &mut Vec<u8>,
    offset_deltas: &mut Vec<u32>,
    bitmap_encoder: &mut bitmap_rle::Encoder,
    offset_encoder: &mut OffsetEncoder,
) -> EncodeResult {
    encoded_data.clear();

    encoded_data.put_varint(chunk.size as u64);
    encoded_data
        .extend_from_slice(&buf.data()[chunk.start_offset as usize..chunk.end_offset as usize]);

    offset_deltas.clear();

    // add the offsets deltas.
    for i in chunk.start_pos..chunk.end_pos {
        offset_deltas.push(buf.item_len(i));
    }

    offset_encoder.encode(offset_deltas, encoded_data);

    match buf.validity() {
        Some(ref mut validity) => {
            bitmap_encoder.clear();
            let last_bit_pos = bitmap_encoder.put_from(validity, bit_pos, chunk.len);
            bitmap_encoder.flush();
            encoded_data.extend_from_slice(bitmap_encoder.encoded_values());

            EncodeResult {
                num_rows: last_bit_pos,
                last_validity_pos: last_bit_pos,
            }
        }
        None => EncodeResult {
            num_rows: chunk.len,
            last_validity_pos: 0,
        },
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::store::block_encoders::test::SinkMock;

    #[tokio::test]
    async fn test_remaining() {
        let mut buf = BinaryBuffer::new(true);
        buf.append(b"test");
        let mut sink_mock = SinkMock::new();
        let mut encoder = Encoder::new(1000, true);
        encoder.encode(&buf, &mut sink_mock).await.unwrap();
        assert_eq!(sink_mock.blocks.len(), 0);
        encoder.flush(&mut sink_mock).await.unwrap();
        assert_eq!(sink_mock.blocks.len(), 1);
    }

    #[tokio::test]
    async fn test_encoder() {
        let mut buf = BinaryBuffer::new(true);
        buf.append(b"test1");
        buf.append(b"test2");
        buf.append(b"test3");
        let mut sink_mock = SinkMock::new();
        let mut encoder = Encoder::new(10, true);
        encoder.encode(&buf, &mut sink_mock).await.unwrap();
        assert_eq!(sink_mock.blocks.len(), 1);
        encoder.flush(&mut sink_mock).await.unwrap();
        assert_eq!(sink_mock.blocks.len(), 2);
    }
}
