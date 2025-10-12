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

use std::mem;
use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
#[derive(Default)]
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        debug_assert!(block.num_of_blocks() >= 1);

        let (len, rest_len, start) = block.parse_key(0);
        debug_assert!(len == 0);
        debug_assert!(rest_len >= 1);
        debug_assert!(start == mem::size_of::<u16>() + mem::size_of::<u16>());
        let first_key = Vec::from(block.data_slice(start, rest_len));
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::from_vec(first_key),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();

        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);

        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice<'_> {
        debug_assert!(self.is_valid());

        KeySlice::from_slice(self.key.raw_ref())
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        debug_assert!(self.is_valid());

        self.block
            .data_slice(self.value_range.0, self.value_range.1 - self.value_range.0)
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.seek(self.block.find_key_idx(key));
    }

    fn seek(&mut self, idx: usize) {
        if self.block.num_of_blocks() <= idx {
            // clean up the key and return silently
            self.key.clear();
            // make sure that we're really invalidated
            debug_assert!(!self.is_valid());
            return;
        }

        // set idx
        self.idx = idx;

        // parse key info
        let (overlap_len, key_len, start) = self.block.parse_key(idx);
        // set key
        self.key.clear();
        self.key.append(&self.first_key.raw_ref()[..overlap_len]);
        self.key.append(self.block.data_slice(start, key_len));

        // set value range
        let (value_start, value_len) = self.block.parse_value_at(start + key_len);
        self.value_range = (value_start, value_start + value_len);
    }
}
