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

use anyhow::Result;
use async_trait::async_trait;

use crate::store::block_index::BlockIndex;
use crate::store::encoding::BlockSink;
use crate::store::io::BlockWriter;

/// Index and write the blocks produced by the encoder.
pub struct BlockSinkImpl<'a> {
    data_writer: &'a mut BlockWriter,
    block_index: &'a mut BlockIndex,
}

impl<'a> BlockSinkImpl<'a> {
    pub fn new(block_writer: &'a mut BlockWriter, block_index: &'a mut BlockIndex) -> Self {
        Self {
            data_writer: block_writer,
            block_index: block_index,
        }
    }
    pub fn block_index(&mut self) -> &mut BlockIndex {
        self.block_index
    }
}

#[async_trait]
impl BlockSink for BlockSinkImpl<'_> {
    async fn write_block(&mut self, row_id: u32, block_data: &[u8]) -> Result<()> {
        self.data_writer.write_block(block_data).await?;
        self.block_index.index_block(row_id, self.data_writer.pos());
        Ok(())
    }
}
