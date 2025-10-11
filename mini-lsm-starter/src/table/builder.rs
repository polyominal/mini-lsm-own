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

use super::BLOOM_FALSE_POSITIVE_RATE;
use super::FileObject;
use super::bloom::Bloom;
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
    key_hashes: Vec<u32>,
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
            key_hashes: vec![],
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        self.key_hashes.push(farmhash::fingerprint32(key.raw_ref()));

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

        // append metadata
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: KeyBytes::from_bytes(Bytes::from(mem::take(&mut self.first_key))),
            last_key: KeyBytes::from_bytes(Bytes::from(mem::take(&mut self.last_key))),
        });

        // append data
        let encoded = finalized_builder.build().encode();
        self.data.extend(encoded);
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
        #[allow(unused_mut)] mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // force finalizing a block
        self.finalize_block();

        let bloom = Bloom::build_from_key_hashes(
            self.key_hashes.as_slice(),
            Bloom::bloom_bits_per_key(self.key_hashes.len(), BLOOM_FALSE_POSITIVE_RATE),
        );

        // -----------------------------------------------------------------------------------------------------
        // |         Block Section         |                            Meta Section                           |
        // -----------------------------------------------------------------------------------------------------
        // | data block | ... | data block | metadata | meta block offset | bloom filter | bloom filter offset |
        // |                               |  varlen  |         u32       |    varlen    |        u32          |
        // -----------------------------------------------------------------------------------------------------
        let block_meta_offset = self.data.len();

        // block section
        let mut buf_sst = self.data;

        // meta section
        BlockMeta::encode_block_meta(&self.meta, &mut buf_sst);
        buf_sst.put_u32(block_meta_offset as u32);
        let bloom_offset = buf_sst.len();
        bloom.encode(&mut buf_sst);
        buf_sst.put_u32(bloom_offset as u32);

        debug_assert!(!self.meta.is_empty());
        let first_key = self.meta[0].first_key.clone();
        let last_key = self.meta[self.meta.len() - 1].last_key.clone();

        Ok(SsTable {
            file: FileObject::create(path.as_ref(), buf_sst)?,
            block_meta: self.meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
