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

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use crate::store::indexing_buffer::{
    BinaryBuffer, Float64Buffer, Int32Buffer, Int64Buffer, Uint64Buffer,
};

use crate::store::segment_metadata::column_layout::EncoderLayout;
use crate::store::segment_metadata::Encoding;

mod bitmap_rle;
mod offsets;
mod raw;
mod snappy;
mod util;
mod varint;

#[async_trait]
pub trait BlockSink {
    async fn write_block(&mut self, row_id: u32, block_data: &[u8]) -> Result<()>;
}

#[async_trait]
pub trait BlockEncoder<B, S: BlockSink> {
    async fn encode(&mut self, buffer: &B, sink: &mut S) -> Result<()>;
    async fn flush(&mut self, sink: &mut S) -> Result<EncoderLayout>;
}

pub fn new_i32_encoder<S>(
    encoder: Encoding,
    block_size: usize,
    nullable: bool,
) -> Result<Box<dyn BlockEncoder<Int32Buffer, S>>>
where
    S: BlockSink + Send,
{
    match encoder {
        Encoding::Raw => Ok(Box::new(raw::primitive_enc::Encoder::new(
            block_size, nullable,
        ))),
        _ => Err(anyhow!("invalid encoder for i32 data {:?}", encoder)),
    }
}

pub fn new_i64_encoder<S>(
    encoder: Encoding,
    block_size: usize,
    nullable: bool,
) -> Result<Box<dyn BlockEncoder<Int64Buffer, S>>>
where
    S: BlockSink + Send,
{
    match encoder {
        Encoding::Raw => Ok(Box::new(raw::primitive_enc::Encoder::new(
            block_size, nullable,
        ))),
        _ => Err(anyhow!("invalid encoder for i64 data {:?}", encoder)),
    }
}

pub fn new_u64_encoder<S>(
    encoder: Encoding,
    block_size: usize,
    nullable: bool,
) -> Result<Box<dyn BlockEncoder<Uint64Buffer, S>>>
where
    S: BlockSink + Send,
{
    match encoder {
        Encoding::Raw => Ok(Box::new(raw::primitive_enc::Encoder::new(
            block_size, nullable,
        ))),
        _ => Err(anyhow!("invalid encoder for u64 data {:?}", encoder)),
    }
}

pub fn new_f64_encoder<S>(
    encoder: Encoding,
    block_size: usize,
    nullable: bool,
) -> Result<Box<dyn BlockEncoder<Float64Buffer, S>>>
where
    S: BlockSink + Send,
{
    match encoder {
        Encoding::Raw => Ok(Box::new(raw::primitive_enc::Encoder::new(
            block_size, nullable,
        ))),
        _ => Err(anyhow!("invalid encoder for f64 data {:?}", encoder)),
    }
}

pub fn new_binary_encoder<S>(
    encoder: Encoding,
    block_size: u32,
    nullable: bool,
) -> Result<Box<dyn BlockEncoder<BinaryBuffer, S>>>
where
    S: BlockSink + Send,
{
    match encoder {
        Encoding::Snappy => Ok(Box::new(snappy::snappy_enc::Encoder::new(
            block_size, nullable,
        ))),
        Encoding::Raw => Ok(Box::new(raw::binary_enc::Encoder::new(
            block_size, nullable,
        ))),
        _ => Err(anyhow!("invalid encoder for binary data {:?}", encoder)),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[derive(Debug)]
    pub struct CapturedBlock {
        row_id: u32,
        block_data: Vec<u8>,
    }

    pub struct SinkMock {
        pub blocks: Vec<CapturedBlock>,
    }

    impl SinkMock {
        pub fn new() -> Self {
            Self { blocks: Vec::new() }
        }
    }

    #[async_trait]
    impl BlockSink for SinkMock {
        async fn write_block(&mut self, row_id: u32, block_data: &[u8]) -> Result<()> {
            self.blocks.push(CapturedBlock {
                row_id: row_id,
                block_data: Vec::from(block_data),
            });
            Ok(())
        }
    }
}
