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

//! This module defines the IO layer abstractions.
//!
//! The meerkat IO layer is block-oriented. A Block is the smallest unit of data
//! that can be read from a segment file. Unlike database pages, blocks have
//! variable lengths, in the case of data blocks, the segment writer does its
//! best to keep the length around a configured `BLOCKSIZE` (usually 128KiB).
//!
//! Segments are basically sequences of blocks. The segment topology ( the set
//! of blocks offsets ) is typically stored in index files and known in advance.
//! This approach allows batching all the Read Ops related to a query job.
//!
//! Segments can reside on local storage ( attached SSD ), used mostly for
//! recently ingested data ( hot data ) or remotely in cloud object storage
//! ( S3, Azure Blobs, GCS, etc ) for cold data. BlockReaders could implement
//! different fetch strategies depending on the segment location. Block read
//! operations could be merged in one big IO buffer in the case of a large file
//! scan or parallelized to maximize throughput in the case of remote object
//! access where HTTP is the default transport. Blocks can also be cached
//! in-memory to avoid the cost of random read operations.
//!
//! The IO layer could also implement an internal IO scheduler to prioritize
//! certain IO operations related to query or ingestion jobs over maintenance
//! operations like segment merge.


use std::io;

use async_trait::async_trait;
use futures::stream::BoxStream;
use tokio::task::spawn_blocking;

mod unix_disk_block_reader;
mod default_block_writer;

/// Block Read Operation.
#[derive(Clone, Debug)]
pub struct ReadOp {
    /// Block offset info.
    block_range: BlockRange,
    /// User data attached to the op.
    op_info: OpInfo,
}

/// BlockRange defines the start and end offset of a block.
#[derive(Clone, Debug)]
pub struct BlockRange {
    start: u64,
    end: u64,
}

/// OpInfo contains user data attached to the read operation.
/// OpInfo is intended to be used by the application after op completion.
#[derive(Debug, PartialEq, Clone)]
pub enum OpInfo {
    /// Nonspecific block used to fetch auxiliary data from segments like
    /// pointers to metadata blocks.
    GenericBlock,
    /// Blocks containing index related data ( inverted indexes,
    /// FST, Bloom filters, etc )
    IndexBlock,
    /// Represent a column data block with its first row id.
    DataBlock { first_row_id: u32 },
}

/// Block data.
pub struct Block<Buf: AsRef<[u8]>> {
    /// The buffer containing the block data
    buf: Buf,
    /// The OpInfo attached by the user.
    op_info: OpInfo,
}

/// Represent an error in a Block IO op.
#[derive(Debug)]
pub struct BlockError {
    error: io::Error,
    op_info: OpInfo,
}

/// BlockReader reads blocks from a segment.
#[async_trait]
pub trait BlockReader {
    type Buf: AsRef<[u8]>;

    /// Read a single block.
    async fn read_block(
        &self,
        range: BlockRange,
    ) -> io::Result<Self::Buf>;

    /// Read a batch of Blocks.
    /// Blocks are returned in the same order as read_ops
    fn read_blocks(
        &self,
        read_ops: Vec<ReadOp>,
    ) -> BoxStream<'_, Result<Block<Self::Buf>, BlockError>>;
    fn len(&self) -> u64;
}

/// BlockWriter writes blocks sequentially to a segment file.
#[async_trait]
pub trait BlockWriter {
    /// Write a single block to disk.
    async fn write_block(&mut self, block: &[u8]) -> io::Result<()>;
    /// Wait for pending operations and flushes all internal buffers.
    async fn flush(&mut self) -> io::Result<()>;
    /// Sync all OS-internal metadata to disk.
    /// This function will attempt to ensure that all in-core data reaches
    /// the filesystem before returning.
    async fn sync(&mut self) -> io::Result<()>;
}


/// Helper fn brought from tokio::fs to asyncify some IO operations.
async fn asyncify<F, T>(f: F) -> io::Result<T>
    where
        F: FnOnce() -> io::Result<T> + Send + 'static,
        T: Send + 'static,
{
    match spawn_blocking(f).await {
        Ok(res) => res,
        Err(_) => Err(io::Error::new(
            io::ErrorKind::Other,
            "background task failed",
        )),
    }
}


#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use rand::Rng;
    use tempfile::tempdir;

    use crate::store::io::*;

    use super::default_block_writer;

    #[tokio::test]
    async fn io_roundtrip() {
        let tmp_dir = tempdir().unwrap();
        let tmp_file_path = tmp_dir.path().join("io_roundtrip_test");

        let mut blocks_list: Vec<Vec<u8>> = Vec::new();

        let mut rng = rand::thread_rng();

        for _ in 0..128 {
            let rnd_len = rng.gen_range(1..2048);
            let mut block_data = vec![0; rnd_len];
            rng.fill(block_data.as_mut_slice());
            blocks_list.push(block_data);
        }

        let mut io_ops: Vec<ReadOp> = Vec::new();

        let mut block_writer = default_block_writer::Writer::new(&tmp_file_path).await.unwrap();
        let mut last_offset = 0;
        for (i, block_data) in blocks_list.iter().enumerate() {
            io_ops.push(ReadOp {
                block_range: BlockRange {
                    start: last_offset,
                    end: last_offset + block_data.len() as u64,
                },
                op_info: OpInfo::DataBlock { first_row_id: i as u32 },
            });
            last_offset += block_data.len() as u64;
            block_writer.write_block(&block_data).await.unwrap();
        }

        block_writer.flush().await.unwrap();

        let block_reader = unix_disk_block_reader::Reader::new(&tmp_file_path).await.unwrap();

        let mut block_stream = block_reader.read_blocks(io_ops.to_owned());

        let mut io_op_iter = io_ops.iter();
        let mut block_iter = blocks_list.iter();

        while let Some(io_result) = block_stream.next().await {
            let io_op = io_op_iter.next().unwrap();
            let block_data = block_iter.next().unwrap();
            let block = io_result.unwrap();
            assert_eq!(io_op.op_info, block.op_info, "op_info doesn't match");
            assert_eq!(block.buf.len(), block_data.len(), "block data len doesn't match");
            for (actual, expected) in block.buf.iter().zip(block_data.iter()) {
                assert_eq!(actual, expected, "block data doesn't match");
            }
        };

        assert!(io_op_iter.next().is_none());
        assert!(block_iter.next().is_none());
    }
}
