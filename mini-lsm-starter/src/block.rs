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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
pub use iterator::BlockIterator;

use crate::key::KeySlice;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
#[derive(Default)]
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let num_elements = self.offsets.len();

        let size_total =
            self.data.len() + num_elements * mem::size_of::<u16>() + mem::size_of::<u16>();
        let mut combined = Vec::with_capacity(size_total);

        combined.extend(self.data.iter());
        for offset in &self.offsets {
            combined.put_u16(*offset);
        }
        combined.put_u16(num_elements as u16);

        debug_assert!(combined.len() == size_total);

        Bytes::from(combined)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        // contains at least (# of elements)
        debug_assert!(mem::size_of::<u16>() <= data.len());

        let offsets_end = data.len() - mem::size_of::<u16>();
        let num_elements = (&data[offsets_end..]).get_u16() as usize;

        debug_assert!(num_elements * mem::size_of::<u16>() <= offsets_end);
        let offsets_start = offsets_end - num_elements * mem::size_of::<u16>();

        let mut buf_offsets = &data[offsets_start..];

        Self {
            data: data[0..offsets_start].to_vec(),
            offsets: (0..num_elements).map(|_| buf_offsets.get_u16()).collect(),
        }
    }

    pub fn num_of_blocks(&self) -> usize {
        self.offsets.len()
    }

    /// Seek to the first key that >= `key`.
    pub fn find_key_idx(&self, key: KeySlice) -> usize {
        let num_elements = self.num_of_blocks();

        let key = key.raw_ref();

        // we're essentially searching for the smallest index
        // whose key is not less than the input key

        // invariant: min..=max being the set of candidates
        let mut min = 0;
        let mut max = num_elements;
        while min < max {
            let idx = (min + max - 1) / 2;
            debug_assert!(idx < num_elements);

            let (key_start, key_len) = self.parse_key(idx);
            let key_at_idx = self.data_slice(key_start, key_len);

            if key_at_idx < key {
                // answer must be greater than min
                min = idx + 1;
            } else {
                // answer must be no greater than max
                max = idx;
            }
        }

        max
    }

    /// Parses the key range given index.
    pub fn parse_key(&self, idx: usize) -> (usize, usize) {
        self.parse_entry_at(self.offsets[idx] as usize)
    }

    /// Parses the entry (either key or value) range starting at the given offset.
    fn parse_entry_at(&self, offset: usize) -> (usize, usize) {
        let key_len = self.data_slice(offset, mem::size_of::<u16>()).get_u16() as usize;

        (offset + mem::size_of::<u16>(), key_len)
    }

    pub fn data_slice(&self, offset: usize, len: usize) -> &[u8] {
        debug_assert!(offset + len <= self.data.len());

        &self.data[offset..offset + len]
    }
}
