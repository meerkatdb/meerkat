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

/// Primitive plain block
///
/// ```text
/// ┌─────────────┬────────────┬──────────────┐
/// │ Data Lenght │    Data    │ RLE validity │
/// │   Varint    │  N bytes   │    N bytes   │
/// └─────────────┴────────────┴──────────────┘
/// ```
use std::convert::TryInto;
use std::mem::size_of;

use anyhow::Result;
use async_trait::async_trait;

use crate::store::block_encoders::bitmap_rle;
use crate::store::block_encoders::{BlockEncoder, BlockSink};
use crate::store::indexing_buffer::PrimitiveBuffer;
use crate::store::segment_metadata::column_layout::EncoderLayout;
use crate::store::segment_metadata::NoLayout;

use super::varint::Encoder as VarintEncoder;

pub struct Encoder<T> {
    remaining: PrimitiveBuffer<T>,
    encoded_data: Vec<u8>,
    bitmap_encoder: bitmap_rle::Encoder,
    min_block_size: usize,
    num_rows: u32,
}

impl<T: Send + Sync + Clone> Encoder<T> {
    pub fn new(block_size: usize, nullable: bool) -> Self {
        Self {
            remaining: PrimitiveBuffer::new(nullable),
            encoded_data: Vec::with_capacity(block_size as usize),
            bitmap_encoder: bitmap_rle::Encoder::new(),
            min_block_size: block_size,
            num_rows: 0,
        }
    }

    async fn write_block<S: BlockSink + Send>(
        &mut self,
        buf: &PrimitiveBuffer<T>,
        start: usize,
        end: usize,
        bit_pos: usize,
        sink: &mut S,
    ) -> Result<EncodeResult> {
        let result = encode_primitive_buffer(
            buf,
            start,
            end,
            bit_pos,
            &mut self.encoded_data,
            &mut self.bitmap_encoder,
        );

        self.add_rows(result.num_rows);

        sink.write_block(self.num_rows - 1, &self.encoded_data)
            .await?;

        Ok(result)
    }

    async fn write_remaining<S: BlockSink + Send>(&mut self, sink: &mut S) -> Result<EncodeResult> {
        let result = encode_primitive_buffer(
            &self.remaining,
            0,
            self.remaining.len(),
            0,
            &mut self.encoded_data,
            &mut self.bitmap_encoder,
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
impl<T: Send + Sync + Clone, S: BlockSink + Send> BlockEncoder<PrimitiveBuffer<T>, S>
    for Encoder<T>
{
    async fn encode(&mut self, buffer: &PrimitiveBuffer<T>, sink: &mut S) -> Result<()> {
        let mut buf_pos = 0;
        let mut validity_pos = 0;

        if self.remaining.len() > 0 {
            let remaining_size = self.remaining.len() * std::mem::size_of::<T>();

            let chunk_end = chunk(buffer, 0, self.min_block_size - remaining_size);

            validity_pos = self.remaining.append_from(buffer, 0, chunk_end, 0);

            buf_pos = chunk_end;

            if self.remaining.len() < self.min_block_size {
                return Ok(());
            }

            self.write_remaining(sink).await?;

            self.remaining.clear();
        }

        while buf_pos < buffer.len() {
            let chunk_end = chunk(buffer, buf_pos, self.min_block_size);

            let chunk_len = chunk_end - buf_pos;
            let chunk_size = chunk_len * size_of::<T>();

            if chunk_size < self.min_block_size {
                self.remaining
                    .append_from(buffer, buf_pos, chunk_end, validity_pos);
                return Ok(());
            }

            let result = self
                .write_block(buffer, buf_pos, chunk_end, validity_pos, sink)
                .await?;

            buf_pos = chunk_end;
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

fn encode_primitive_buffer<T: Clone>(
    buf: &PrimitiveBuffer<T>,
    start: usize,
    end: usize,
    bit_pos: usize,
    encoded_data: &mut Vec<u8>,
    bitmap_encoder: &mut bitmap_rle::Encoder,
) -> EncodeResult {
    encoded_data.clear();

    let block_len = end - start;
    let block_size = size_of::<T>() * block_len;

    encoded_data.put_varint(block_size as u64);

    encoded_data.extend_from_slice(to_byte_slice(&buf.values()[start..end]));

    match buf.validity() {
        Some(ref mut validity) => {
            bitmap_encoder.clear();
            let last_bit_pos = bitmap_encoder.put_from(validity, bit_pos, block_len);
            bitmap_encoder.flush();
            encoded_data.extend_from_slice(bitmap_encoder.encoded_values());

            EncodeResult {
                num_rows: last_bit_pos,
                last_validity_pos: last_bit_pos,
            }
        }
        None => EncodeResult {
            num_rows: block_len,
            last_validity_pos: 0,
        },
    }
}

#[derive(Debug)]
struct EncodeResult {
    num_rows: usize,
    last_validity_pos: usize,
}

fn chunk<T: Clone>(buffer: &PrimitiveBuffer<T>, start: usize, min_size: usize) -> usize {
    // div ceil
    let chunk_len = min_size / size_of::<T>() + ((min_size % size_of::<T>() != 0) as usize);

    let end = start + chunk_len;

    if end > buffer.len() {
        return buffer.len();
    }

    end
}

fn to_byte_slice<'a, T>(src: &'a [T]) -> &'a [u8] {
    unsafe {
        std::slice::from_raw_parts::<u8>(
            src.as_ptr() as *const u8,
            std::mem::size_of::<T>() * src.len(),
        )
    }
}

#[cfg(test)]
mod test {
    use tokio::test;

    use crate::store::block_encoders::test::SinkMock;

    use super::*;

    #[tokio::test]
    async fn test_remaining() {
        let mut buf: PrimitiveBuffer<i32> = PrimitiveBuffer::new(true);
        buf.append(10);
        let mut sink_mock = SinkMock::new();
        let mut encoder = Encoder::new(1000, true);
        encoder.encode(&buf, &mut sink_mock).await.unwrap();
        assert_eq!(sink_mock.blocks.len(), 0);
        encoder.flush(&mut sink_mock).await.unwrap();
        assert_eq!(sink_mock.blocks.len(), 1);
    }

    #[tokio::test]
    async fn test_encoder() {
        let mut buf: PrimitiveBuffer<i32> = PrimitiveBuffer::new(true);
        for i in 0..1005 {
            buf.append(i);
        }
        let mut sink_mock = SinkMock::new();
        let mut encoder = Encoder::new(100, true);
        encoder.encode(&buf, &mut sink_mock).await.unwrap();
        assert_eq!(sink_mock.blocks.len(), 40);
        encoder.flush(&mut sink_mock).await.unwrap();
        assert_eq!(sink_mock.blocks.len(), 41);
    }
}
