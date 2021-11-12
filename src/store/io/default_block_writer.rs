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

use std::path::Path;

use async_trait::async_trait;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::store::io::BlockWriter;

/// A `BlockWriter` based on `tokio::fs::File`.
///
/// Note: This writer perform a `spawn_blocking()` call for every block write to
/// asyncify the operation. This is probably very inefficient.
/// Future implementations will take advantage of the new `io_uring` API or
/// use `AIO/DIO`.
pub struct Writer {
    file: File,
}


impl Writer {
    pub async fn new(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let file = File::create(path).await?;
        Ok(Self { file })
    }
}

#[async_trait]
impl BlockWriter for Writer {
    async fn write_block(&mut self, block: &[u8]) -> std::io::Result<()> {
        self.file.write_all(block).await
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush().await
    }

    async fn sync(&mut self) -> std::io::Result<()> {
        self.file.sync_all().await
    }
}





