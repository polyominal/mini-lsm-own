// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::mem;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;
use bytes::Bytes;

use super::FileObject;
use super::{BlockMeta, SsTable};
use crate::key::KeyBytes;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: vec![],
            last_key: vec![],
            data: vec![],
            meta: vec![],
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.builder.add(key, value) {
            self.post_add(key, value);
            return;
        }

        // otherwise, split a new block
        self.finalize_block();

        // add must succeed this time
        let added = self.builder.add(key, value);
        debug_assert!(added);
        self.post_add(key, value);
    }

    fn post_add(&mut self, key: KeySlice, _value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key = key.raw_ref().to_vec();
        }
        self.last_key = key.raw_ref().to_vec();
    }

    fn finalize_block(&mut self) {
        let finalized_builder = mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));

        // append data
        let encoded = finalized_builder.build().encode();
        self.data.extend(encoded);

        // append metadata
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: KeyBytes::from_bytes(Bytes::from(mem::take(&mut self.first_key))),
            last_key: KeyBytes::from_bytes(Bytes::from(mem::take(&mut self.last_key))),
        });
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        unimplemented!();
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // force finalizing a block
        self.finalize_block();

        // -------------------------------------------------------------------------------------------
        // |         Block Section         |          Meta Section         |          Extra          |
        // -------------------------------------------------------------------------------------------
        // | data block | ... | data block |            metadata           | meta block offset (u32) |
        // -------------------------------------------------------------------------------------------
        let block_meta_offset = self.data.len();
        let mut buf_sst = self.data;
        BlockMeta::encode_block_meta(&self.meta, &mut buf_sst);
        buf_sst.put_u32(block_meta_offset as u32);

        let first_key = self.meta.first().unwrap().first_key.clone();
        let last_key = self.meta.last().unwrap().last_key.clone();
        Ok(SsTable {
            file: FileObject::create(path.as_ref(), buf_sst)?,
            block_meta: self.meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            // for now...
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
