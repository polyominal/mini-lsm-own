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

use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    current_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self::dummy());
        }

        let current = SsTableIterator::create_and_seek_to_first(Arc::clone(&sstables[0]))?;
        let (current, current_sst_idx) = Self::move_until_valid_or_end(current, 0, &sstables)?;
        Ok(Self {
            current,
            current_sst_idx,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(Self::dummy());
        }

        let mut current = SsTableIterator::create_and_seek_to_key(Arc::clone(&sstables[0]), key)?;
        let mut current_sst_idx = 0;
        loop {
            if current.is_valid() {
                return Ok(Self {
                    current: Some(current),
                    current_sst_idx,
                    sstables,
                });
            }

            current_sst_idx += 1;
            if current_sst_idx == sstables.len() {
                return Ok(Self::dummy());
            }

            current = SsTableIterator::create_and_seek_to_key(
                Arc::clone(&sstables[current_sst_idx]),
                key,
            )?;
        }
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice<'_> {
        // precondition: self.is_valid()
        assert!(self.is_valid());

        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        // precondition: self.is_valid()
        assert!(self.is_valid());

        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        let Some(current) = self.current.as_ref() else {
            return false;
        };
        current.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        // precondition: self.is_valid()
        assert!(self.is_valid());

        // move
        self.current.as_mut().unwrap().next()?;

        (self.current, self.current_sst_idx) = Self::move_until_valid_or_end(
            self.current.take().unwrap(),
            self.current_sst_idx,
            &self.sstables,
        )?;

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}

impl SstConcatIterator {
    fn move_until_valid_or_end(
        mut current: SsTableIterator,
        mut current_sst_idx: usize,
        sstables: &[Arc<SsTable>],
    ) -> Result<(Option<SsTableIterator>, usize)> {
        assert!(current_sst_idx < sstables.len());

        loop {
            if current.is_valid() {
                return Ok((Some(current), current_sst_idx));
            }

            current_sst_idx += 1;
            if current_sst_idx == sstables.len() {
                return Ok((None, current_sst_idx));
            }

            current =
                SsTableIterator::create_and_seek_to_first(Arc::clone(&sstables[current_sst_idx]))?;
        }
    }

    fn dummy() -> Self {
        Self {
            current: None,
            current_sst_idx: 0,
            sstables: vec![],
        }
    }
}
