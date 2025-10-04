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

use bytes::Buf;

use crate::key::{KeySlice, KeyVec};

use super::Block;
use super::LEN_U16;

/// Iterates on a block.
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
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
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

        &self.block.data[self.value_range.0..self.value_range.1]
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
        // we're essentially searching for the smallest index
        // whose key is not less than the input key

        let num_elements = self.block.offsets.len();

        let mut lo = 0;
        let mut hi = num_elements;
        while lo < hi {
            let idx = (lo + hi - 1) / 2;
            debug_assert!(idx < num_elements);

            let (key_start, key_end) = self.parse_key(idx);
            let key_idx = KeySlice::from_slice(&self.block.data[key_start..key_end]);

            if key_idx.cmp(&key) == Ordering::Less {
                lo = idx + 1;
            } else {
                hi = idx;
            }
        }

        debug_assert!(lo == hi);
        self.seek(hi);
    }

    fn seek(&mut self, idx: usize) {
        let num_elements = self.block.offsets.len();
        if num_elements <= idx {
            // clean up the key and return silently
            self.key.clear();
            // make sure that we're really invalidated
            debug_assert!(!self.is_valid());
            return;
        }

        // set idx
        self.idx = idx;

        // parse key range
        let (key_start, key_end) = self.parse_key(idx);

        // set key
        self.key
            .set_from_slice(KeySlice::from_slice(&self.block.data[key_start..key_end]));

        // set value range
        let value_start = key_end + LEN_U16;
        let value_len = (&self.block.data[key_end..value_start]).get_u16() as usize;
        self.value_range = (value_start, value_start + value_len);
    }

    fn parse_key_inner(&self, offset: usize) -> (usize, usize) {
        let key_start = offset + LEN_U16;
        let key_len = (&self.block.data[offset..key_start]).get_u16() as usize;
        (key_start, key_start + key_len)
    }

    /// Parses the key (range) given index.
    fn parse_key(&self, idx: usize) -> (usize, usize) {
        self.parse_key_inner(self.block.offsets[idx] as usize)
    }
}
