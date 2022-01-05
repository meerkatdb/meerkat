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

//! This module contains the indexing pipeline.
//!
//! ```text
//!
//!  ┌──────────────┐
//!  │IndexingBuffer│
//!  └──────────────┘
//!         ▼
//!  ┌──────────────┐
//!  │ ColumnWriter │
//!  └──────┬───────┘
//!         │
//!         │
//!         │                                                ┌──────────────┐
//!         ├──────────────────────────────────────────────► │ ColumnStats  │
//!         │                                                └───────┬──────┘
//!         │                             ┌──────────────┐           │
//!         ├────────────────────────────►│ ColumnIndex  │           │
//!         │                             └───────┬──────┘           │
//!         ▼                                     │                  │
//!  ┌──────────────┐                             │                  │
//!  │ BlockEncoder │                             │                  │
//!  └──────────────┘                             │                  │
//!         ▼                                     │ flush()          │ flush()
//!  ┌──────────────┐    ┌──────────────┐         │                  │
//!  │   BlockSink  ├──► │ BlockIndex   │         │                  │
//!  └──────────────┘    └──────────────┘         │                  │
//!         ▼                   ▼ flush()         ▼                  ▼
//!  ┌──────────────┐    ┌──────────────┐  ┌──────────────┐   ┌──────────────┐
//!  │  BlockWriter │    │  BlockWriter │  │  BlockWriter │   │  BlockWriter │
//!  └──────┬───────┘    └──────┬───────┘  └──────┬───────┘   └──────┬───────┘
//!         │                   │                 │                  │
//!         │                   └─────────────────┼──────────────────┘
//!         ▼                                     │
//!  ┌──────────────┐                      ┌──────┴───────┐
//!  │   DataFile   │                      │  IndexFile   │
//!  └──────────────┘                      └──────────────┘
//!
//!```
//!  Segments are composed of two files
//!
//!  # Data File:
//! Contains the segment blocks. A Block is the smallest unit of data that can
//! be read from a segment file.
//!
//! ```text
//!┌───────────────────────────────────────────────────────────────────────────────┐
//!│ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ │
//!│ │  Block 1 │ │  Block 2 │ │  Block 3 │ │  Block 1 │ │  Block 2 │ │  Block 3 │ │
//!│ │ Column A │ │ Column A │ │ Column A │ │ Column B │ │ Column B │ │ Column B │ │
//!│ └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘ │
//!└───────────────────────────────────────────────────────────────────────────────┘
//!```
//!
//! # Index File:
//!Composed of
//! - `ColumnIndex`  index the column data ( full-text inverted index, BRIN, BTree )
//! - `BlockIndex`  index the blocks offsets and row_id ranges.
//! - `Metadata`
//!    - `SegmentMeta`, `ColumnMeta` : information about the segment schema
//!       ( columns types, column encoding, column indexes )
//!    - `ColumnStats`: stats about the column data ( min, max, cardinallity,
//!       num_nulls, histograms )
//!    - `ColumnLayout` :  information about the segment physical layout
//!       ( column index offset, BlockIndex offset)
//!
//!```text
//!┌──────────────────────────────────────────────────────────────────────────────┐
//!│┌─────────────┐ ┌────────────┐ ┌─────────────┐ ┌────────────┐ ┌──────────────┐│
//!││ ColumnIndex │ │ BlockIndex │ │ ColumnIndex │ │ BlockIndex │ │   Metadata   ││
//!││ Column A    │ │ Column A   │ │ Column B    │ │ Column B   │ │(ProtoBuf enc)││
//!│└─────────────┘ └────────────┘ └─────────────┘ └────────────┘ └──────────────┘│
//!└──────────────────────────────────────────────────────────────────────────────┘
//!```
//!

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use prost::Message;
use uuid::Uuid;

use crate::store::block_index::BlockIndex;
use crate::store::block_sink::BlockSinkImpl;
use crate::store::encoding;
use crate::store::encoding::BlockEncoder;
use crate::store::index::ColumnIndex;
use crate::store::indexing_buffer::BinaryBuffer;
use crate::store::segment_metadata;
use crate::store::segment_metadata::{ColumnType, Encoding, IndexType, Metadata};

use super::io::BlockWriter;

const DATA_FILE_SIGNATURE: [u8; 13] = *b"MEERKAT_DATA1";
const INDEX_FILE_SIGNATURE: [u8; 12] = *b"MEERKAT_IDX1";
const DATA_FILE_EXT: &str = "dat";
const INDEX_FILE_EXT: &str = "idx";
const BLOCK_SIZE: u32 = 1 << 7; // 128k block size

/// ColumnInfo provides details about the column to be indexed.
pub struct ColumnInfo {
    pub name: String,
    pub column_type: ColumnType,
    pub encoding: Encoding,
    pub index_type: IndexType,
    pub nullable: bool,
}

/// SegmentInfo provides details about the segment to be indexed.
pub struct SegmentInfo {
    pub segment_id: Uuid,
    pub database_name: String,
    pub table_name: String,
    pub shard_id: u32,
    pub num_rows: u32,
}

/// Write segments to disk.
pub struct SegmentWriter {
    data_writer: BlockWriter,
    index_writer: BlockWriter,
    block_index: BlockIndex,
    segment_meta: Metadata,
}

impl SegmentWriter {
    /// Create a new segment on disk.
    /// Segment and index files are created using `path` as the base path.
    /// File names are generated by encoding the `segment_id` in Base64.
    pub async fn new<P: AsRef<Path>>(path: P, segment_info: SegmentInfo) -> Result<Self> {
        let segment_id_str =
            base64::encode_config(segment_info.segment_id.as_bytes(), base64::URL_SAFE_NO_PAD);

        let data_file_name = format!("{}.{}", segment_id_str, DATA_FILE_EXT);
        let index_file_name = format!("{}.{}", segment_id_str, INDEX_FILE_EXT);

        let data_path = path.as_ref().join(&data_file_name);
        let index_path = path.as_ref().join(&index_file_name);

        let index_writer = BlockWriter::new(index_path)
            .await
            .context("cannot create index writer")?;

        let data_writer = BlockWriter::new(data_path)
            .await
            .context("cannot create data writer")?;

        let mut segment_writer = Self {
            data_writer: data_writer,
            index_writer: index_writer,
            block_index: BlockIndex::new(),
            segment_meta: Metadata {
                segment_meta: Some(segment_metadata::SegmentMeta {
                    segment_id: segment_info.segment_id.as_bytes().to_vec(),
                    table_name: segment_info.table_name,
                    database_name: segment_info.database_name,
                    shard_id: segment_info.shard_id,
                    compressed_size: 0,
                    uncompressed_size: 0,
                    num_rows: segment_info.num_rows,
                    columns_meta: HashMap::new(),
                }),
                columns_layout: HashMap::new(),
            },
        };

        segment_writer.write_headers().await?;

        Ok(segment_writer)
    }

    async fn write_headers(&mut self) -> Result<()> {
        self.index_writer
            .write_block(&INDEX_FILE_SIGNATURE)
            .await
            .context("cannot write index header")?;

        self.data_writer
            .write_block(&DATA_FILE_SIGNATURE)
            .await
            .context("cannot write index header")
    }

    /// Creates a new ColumnWriter for BinaryBuffers.
    pub fn new_binary_column_writer(
        &mut self,
        column_info: ColumnInfo,
    ) -> Result<ColumnWriter<'_, BinaryBuffer>> {
        self.block_index.clear();

        let encoder =
            encoding::new_binary_encoder(column_info.encoding, BLOCK_SIZE, column_info.nullable)?;

        let block_sink = BlockSinkImpl::new(&mut self.data_writer, &mut self.block_index);

        Ok(ColumnWriter {
            column_info: column_info,
            metadata: &mut self.segment_meta,
            encoder: encoder,
            block_sink: block_sink,
            index_writer: &mut self.index_writer,
            column_index: None,
        })
    }

    /// Close the SegmentWriter and flush any pending data.
    async fn close(&mut self) -> Result<()> {
        let metadata_bytes = self.segment_meta.encode_to_vec();
        let metadata_pos = self.index_writer.pos();

        self.index_writer
            .write_block(&metadata_bytes)
            .await
            .context("cannot write metadata")?;

        self.index_writer
            .write_block(&metadata_pos.to_le_bytes())
            .await
            .context("cannot write index footer")?;

        self.index_writer
            .flush()
            .await
            .context("cannot flush index file")?;
        self.data_writer
            .flush()
            .await
            .context("cannot flush data file")
    }

    /// Sync all in-memory data to disk.
    async fn sync(mut self) -> Result<()> {
        self.index_writer
            .sync()
            .await
            .context("cannot sync index file")?;
        self.data_writer
            .sync()
            .await
            .context("cannot sync data file")
    }
}

/// Used to write a single column to the segment.
pub struct ColumnWriter<'a, B> {
    column_info: ColumnInfo,
    metadata: &'a mut Metadata,
    block_sink: BlockSinkImpl<'a>,
    index_writer: &'a mut BlockWriter,
    encoder: Box<dyn BlockEncoder<B, BlockSinkImpl<'a>>>,
    column_index: Option<Box<dyn ColumnIndex<B>>>,
}

impl<'a, B> ColumnWriter<'a, B> {
    /// Index a Buffer.
    pub async fn write(&mut self, buf: &B) -> Result<()> {
        self.encoder.encode(buf, &mut self.block_sink).await?;

        if let Some(ref mut column_index) = self.column_index {
            column_index.index(&buf)?;
        }

        Ok(())
    }

    /// Close the ColumnWriter and flush indexes data to disk.
    pub async fn close(&mut self) -> Result<()> {
        let encoder_layout = self.encoder.flush(&mut self.block_sink).await?;

        let block_ndex_layout = self
            .block_sink
            .block_index()
            .flush(&mut self.index_writer)
            .await?;

        let column_index_layout = match self.column_index {
            Some(ref mut column_index) => Some(column_index.flush(&mut self.index_writer).await?),
            None => None,
        };

        // TODO(gvelo): add missing stats.
        let column_meta = segment_metadata::ColumnMeta {
            name: self.column_info.name.clone(),
            column_type: self.column_info.column_type.into(),
            nullable: self.column_info.nullable,
            encoding: self.column_info.encoding.into(),
            cardinality: 0,
            index_type: self.column_info.index_type.into(),
            num_values: 0,
            num_nulls: 0,
            compressed_size: 0,
            uncompressed_size: 0,
            stats: None,
        };

        let column_layout = segment_metadata::ColumnLayout {
            block_index_layout: Some(block_ndex_layout),
            column_index_layout: column_index_layout,
            encoder_layout: Some(encoder_layout),
        };

        self.metadata
            .segment_meta
            .as_mut()
            .unwrap()
            .columns_meta
            .insert(self.column_info.name.clone(), column_meta);

        self.metadata
            .columns_layout
            .insert(self.column_info.name.clone(), column_layout);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[tokio::test]
    async fn test_indexing_pipeline_scaffold() {
        let tmp_dir = tempdir().unwrap();

        let segment_info = SegmentInfo {
            segment_id: Uuid::new_v4(),
            database_name: "test_db".to_string(),
            table_name: "test_table".to_string(),
            shard_id: 4,
            num_rows: 100000,
        };

        let mut segment_writer = SegmentWriter::new(tmp_dir, segment_info).await.unwrap();

        let col_info = ColumnInfo {
            encoding: Encoding::Raw,
            nullable: false,
            name: "test columns".to_string(),
            index_type: IndexType::NoIndex,
            column_type: ColumnType::String,
        };

        {
            let mut col_writer = segment_writer.new_binary_column_writer(col_info).unwrap();

            let mut buffer = BinaryBuffer::new(false);
            buffer.append("test".as_bytes());

            col_writer.write(&buffer).await.unwrap();
            col_writer.write(&buffer).await.unwrap();
            col_writer.close().await.unwrap();
        }

        segment_writer.close().await.unwrap();
        segment_writer.sync().await.unwrap();
    }
}
