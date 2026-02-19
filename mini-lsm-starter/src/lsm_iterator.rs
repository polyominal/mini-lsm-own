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

use std::ops::Bound;

use anyhow::Result;
use anyhow::bail;
use bytes::Bytes;

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::table::SsTableIterator;
use crate::{
    iterators::{StorageIterator, merge_iterator::MergeIterator},
    mem_table::MemTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
pub(crate) type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    is_valid: bool,
    end_bound: Bound<Bytes>,
}

impl LsmIterator {
    fn compute_is_valid(iter: &LsmIteratorInner, end_bound: &Bound<Bytes>) -> bool {
        if !iter.is_valid() {
            return false;
        }

        let k = iter.key().raw_ref();
        match end_bound {
            Bound::Included(end) => k <= end.as_ref(),
            Bound::Excluded(end) => k < end.as_ref(),
            Bound::Unbounded => true,
        }
    }

    pub(crate) fn new(iter: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<Self> {
        let is_valid = Self::compute_is_valid(&iter, &end_bound);
        let mut iter = Self {
            inner: iter,
            is_valid,
            end_bound,
        };
        iter.skip_deleted()?;

        Ok(iter)
    }

    fn next_inner(&mut self) -> Result<()> {
        debug_assert!(self.is_valid());

        self.inner.next()?;
        self.is_valid =
            self.inner.is_valid() && Self::compute_is_valid(&self.inner, &self.end_bound);

        Ok(())
    }

    fn skip_deleted(&mut self) -> Result<()> {
        loop {
            if !self.is_valid() {
                break;
            }
            // skip tombstones
            if !self.inner.value().is_empty() {
                break;
            }
            self.next_inner()?;
        }

        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        debug_assert!(self.is_valid());

        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        debug_assert!(self.is_valid());

        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        debug_assert!(self.is_valid());

        self.next_inner()?;
        self.skip_deleted()
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        if self.has_errored {
            false
        } else {
            self.iter.is_valid()
        }
    }

    fn key(&self) -> Self::KeyType<'_> {
        debug_assert!(self.is_valid(), "calling key() on an invalid FusedIterator");
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        debug_assert!(
            self.is_valid(),
            "calling value() on an invalid FusedIterator"
        );
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("we've seen an error :(");
        }

        if !self.iter.is_valid() {
            // do nothing
            return Ok(());
        }

        if let Err(e) = self.iter.next() {
            self.has_errored = true;
            return Err(e);
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
