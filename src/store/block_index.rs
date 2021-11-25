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

use crate::store::io::BlockWriter;
use crate::store::segment_metadata::{BlockIndexLayout, Range};

/// Index block offsets and row_id ranges
pub struct BlockIndex {
    block_offset: Vec<u64>,
    block_row_id: Vec<u32>,
}

impl BlockIndex {
    pub fn new() -> Self {
        Self {
            block_offset: Vec::new(),
            block_row_id: Vec::new(),
        }
    }

    /// Add a new block to the index
    pub fn index_block(&mut self, row_id: u32, offset: u64) {
        self.block_offset.push(offset);
        self.block_row_id.push(row_id);
    }

    /// clear the index
    pub fn clear(&mut self) {
        self.block_row_id.clear();
        self.block_offset.clear();
    }

    /// flush the index to disk.
    pub async fn flush(&self, block_writer: &mut BlockWriter) -> Result<BlockIndexLayout> {
        Ok(BlockIndexLayout {
            range: Some(Range { start: 0, end: 0 }),
        })
    }
}
