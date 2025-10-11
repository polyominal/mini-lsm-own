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

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        debug_assert!(self.offsets.is_empty() == self.data.is_empty());
        debug_assert!(!key.is_empty());

        // the first pair is allowed to exceed the target block size
        if !self.is_empty() {
            let size_after_add = self.data.len()
                + (mem::size_of::<u16>() + key.len())
                + (mem::size_of::<u16>() + value.len())
                + (self.offsets.len() + 1) * mem::size_of::<u16>()
                + mem::size_of::<u16>();
            if self.block_size < size_after_add {
                // an overflow
                return false;
            }
        }

        self.offsets.push(self.data.len() as u16);

        // key overlap len + rest key len + key + value len + value + offset
        let key_len = key.len() as u16;
        if !self.first_key.is_empty() {
            let key_overlap_len = key
                .raw_ref()
                .iter()
                .zip(self.first_key.raw_ref().iter())
                .take_while(|(l, r)| l == r)
                .count() as u16;
            debug_assert!(key_overlap_len <= key_len);
            debug_assert!(key_overlap_len <= self.first_key.len() as u16);
            self.data.put_u16(key_overlap_len);
            self.data.put_u16(key_len - key_overlap_len);
            self.data.put(&key.raw_ref()[key_overlap_len as usize..]);
        } else {
            self.first_key = KeyVec::from_vec(Vec::from(key.raw_ref()));
            debug_assert!(!self.first_key.is_empty());
            self.data.put_u16(0);
            self.data.put_u16(key_len);
            self.data.put(key.raw_ref());
        }
        self.data.put_u16(value.len() as u16);
        self.data.put(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        debug_assert!(self.offsets.is_empty() == self.data.is_empty());

        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        debug_assert!(self.offsets.is_empty() == self.data.is_empty());

        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
