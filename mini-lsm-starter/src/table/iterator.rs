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

use std::cmp::Ordering;
use std::sync::Arc;

use anyhow::Result;

use bytes::Buf;

use super::LEN_U32;
use super::SsTable;
use crate::block::Block;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    fn new(table: Arc<SsTable>) -> Result<Self> {
        let blk_iter = Self::get_block_iter(Arc::clone(&table), 0)?;
        Ok(Self {
            table,
            blk_iter,
            blk_idx: 0,
        })
    }

    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        Self::new(table)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.seek_to(0)
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let mut ss_iter = Self::new(table)?;
        ss_iter.seek_to_key(key)?;

        Ok(ss_iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let meta = &self.table.block_meta;
        let num_blocks = meta.len();

        let mut min = 0;
        let mut max = num_blocks;
        while min < max {
            let idx = (min + max - 1) / 2;
            debug_assert!(idx < num_blocks);
            if meta[idx].last_key.as_key_slice().cmp(&key) == Ordering::Less {
                min = idx + 1;
            } else {
                max = idx;
            }
        }

        let target_blk_idx = max;
        self.seek_to(target_blk_idx)?;
        debug_assert!(target_blk_idx == self.blk_idx);
        if target_blk_idx != num_blocks {
            debug_assert!(self.blk_iter.is_valid());
            self.blk_iter.seek_to_key(key);
            debug_assert!(self.blk_iter.is_valid());
            debug_assert_ne!(self.key().cmp(&key), Ordering::Less);
        } else {
            debug_assert!(target_blk_idx == num_blocks);
        }

        Ok(())
    }

    fn seek_to(&mut self, blk_idx: usize) -> Result<()> {
        debug_assert!(blk_idx <= self.table.block_meta.len());
        self.blk_idx = blk_idx;
        if self.blk_idx != self.table.block_meta.len() {
            self.blk_iter = Self::get_block_iter(Arc::clone(&self.table), blk_idx)?;
        }
        Ok(())
    }

    fn get_block_iter(table: Arc<SsTable>, blk_idx: usize) -> Result<BlockIterator> {
        let num_blocks = table.block_meta.len();
        debug_assert!(blk_idx < num_blocks, "blk_idx overbound");

        let blk_start = table.block_meta[blk_idx].offset;
        let blk_end = if blk_idx + 1 != num_blocks {
            table.block_meta[blk_idx + 1].offset
        } else {
            let file_size = table.file.size();
            let meta_offset_end = file_size - LEN_U32 as u64;
            let meta_offset = table.file.read(meta_offset_end, LEN_U32 as u64)?;
            meta_offset.as_slice().get_u32() as usize
        };
        debug_assert!(blk_start < blk_end);

        let blk = table
            .file
            .read(blk_start as u64, (blk_end - blk_start) as u64)?;
        let blk = Block::decode(blk.as_slice());
        Ok(BlockIterator::create_and_seek_to_first(Arc::new(blk)))
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice<'_> {
        debug_assert!(self.is_valid());
        debug_assert!(self.blk_iter.is_valid());
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        debug_assert!(self.is_valid());
        debug_assert!(self.blk_iter.is_valid());
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        debug_assert!(self.blk_idx <= self.table.block_meta.len());
        self.blk_idx != self.table.block_meta.len()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        debug_assert!(self.is_valid());
        debug_assert!(self.blk_iter.is_valid());

        self.blk_iter.next();

        if !self.blk_iter.is_valid() {
            self.seek_to(self.blk_idx + 1)?;
        }

        Ok(())
    }
}
