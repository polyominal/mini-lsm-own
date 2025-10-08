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

use std::cmp::{self};
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    /// A heap that keeps all valid iterators.
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut iters = BinaryHeap::from(
            iters
                .into_iter()
                .filter(|iter| iter.is_valid())
                .enumerate()
                .map(|(i, iter)| HeapWrapper(i, iter))
                .collect::<Vec<_>>(),
        );
        let current = iters.pop();
        Self { iters, current }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice<'_> {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        debug_assert!(
            self.is_valid(),
            "calling next() on an invalid MergeIterator"
        );

        let current = self.current.as_mut().unwrap();

        // consume all items in the heap that has the same key as that in current's
        while let Some(mut heap_iter) = self.iters.peek_mut() {
            debug_assert!(current.1.key() <= heap_iter.1.key());

            if heap_iter.1.key() != current.1.key() {
                break;
            }

            // handle error from calling next()
            if let e @ Err(_) = heap_iter.1.next() {
                PeekMut::pop(heap_iter);
                return e;
            }

            // invariant: all iterators in the heap are valid
            if !heap_iter.1.is_valid() {
                PeekMut::pop(heap_iter);
            }
        }

        // advance the iterator in current
        current.1.next()?;

        // put current back into the heap
        if current.1.is_valid() {
            self.iters.push(self.current.take().unwrap());
        }

        // refresh current item
        self.current = self.iters.pop();

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iters
            .iter()
            .map(|hw| hw.1.num_active_iterators())
            .sum::<usize>()
            + self
                .current
                .as_ref()
                .map_or(0, |hw| hw.1.num_active_iterators())
    }
}
