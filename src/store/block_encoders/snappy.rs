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

use async_trait::async_trait;

use crate::store::block_encoders::{BlockEncoder, BlockSink};
use crate::store::indexing_buffer::BinaryBuffer;
use crate::store::segment_metadata::column_layout::EncoderLayout;
use crate::store::segment_metadata::NoLayout;

pub struct SnappyEncoder {}

impl SnappyEncoder {
    pub fn new(block_size: u32) -> Self {
        Self {}
    }
}

#[async_trait]
impl<S: BlockSink + Send> BlockEncoder<BinaryBuffer, S> for SnappyEncoder {
    async fn encode(&mut self, buffer: &BinaryBuffer, sink: &mut S) -> anyhow::Result<()> {
        sink.write_block(43243, b"snappy").await?;
        Ok(())
    }

    async fn flush(&mut self, sink: &mut S) -> anyhow::Result<EncoderLayout> {
        Ok(EncoderLayout::Snappy(NoLayout {}))
    }
}
