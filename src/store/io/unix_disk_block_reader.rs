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

use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::stream::iter;
use futures::StreamExt;

use crate::store::io::{Block, BlockError, BlockRange, BlockReader, ReadOp};

use super::asyncify;

/// Max number of pending blocks reads operations.
const BLOCK_BUFFER_SIZE: usize = 2;

/// A BlockReader implementation based on the POSIX pread() fn.
///
/// Note: This implementation of the `BlockReader` API is pretty naive an
/// inefficient, it calls `spawn_blocking()` for every block read
/// buffering until `BLOCK_BUFFER_SIZE` futures.
/// Future implementation should use the new `io_uring` API and DIO for disk
/// access and Blocks should be merged to read large IO buffers.
#[derive(Clone)]
pub struct Reader {
    file: Arc<File>,
    len: u64,
}


impl Reader {
    pub async fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref().to_owned();
        let file = asyncify(|| File::open(path)).await?;
        let metadata = file.metadata()?;

        Ok(
            Reader {
                file: Arc::new(file),
                len: metadata.len(),
            }
        )
    }
}

#[async_trait]
impl BlockReader for Reader {
    type Buf = Vec<u8>;


    async fn read_block(&self, range: BlockRange) -> io::Result<Self::Buf> {
        let file = self.file.clone();
        asyncify(move || {
            let buf_len = range.end - range.start;
            let mut buf: Vec<u8> = vec![0; buf_len as usize];
            file.
                read_exact_at(buf.as_mut_slice(), range.start).
                map(|_| buf)
        }).await
    }


    fn read_blocks(
        &self,
        read_ops: Vec<ReadOp>,
    ) -> BoxStream<'_, Result<Block<Self::Buf>, BlockError>> {
        iter(read_ops).
            map(move |op| async move {
                (self.read_block(op.block_range.to_owned()).await, op)
            }).
            buffered(BLOCK_BUFFER_SIZE).
            map(|op_result| {
                match op_result.0 {
                    Ok(buf) => Ok(
                        Block {
                            buf: buf,
                            op_info: op_result.1.op_info,
                        }
                    ),
                    Err(err) => Err(
                        BlockError {
                            op_info: op_result.1.op_info,
                            error: err,
                        }
                    )
                }
            }).
            boxed()
    }

    fn len(&self) -> u64 {
        self.len
    }
}

