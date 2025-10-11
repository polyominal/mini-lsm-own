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

use anyhow::Result;

use super::StorageIterator;

#[derive(PartialEq)]
enum Which {
    TakeA,
    TakeB,
    Invalid,
}

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    which: Which,
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let which = Self::compute_which(&a, &b);
        Ok(Self { a, b, which })
    }

    fn compute_which(a: &A, b: &B) -> Which {
        match (a.is_valid(), b.is_valid()) {
            (true, true) => {
                if a.key() <= b.key() {
                    Which::TakeA
                } else {
                    Which::TakeB
                }
            }
            (true, false) => Which::TakeA,
            (false, true) => Which::TakeB,
            (false, false) => Which::Invalid,
        }
    }
}

impl<
    A: 'static + StorageIterator,
    B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
> StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        debug_assert!(self.is_valid());

        match self.which {
            Which::TakeA => self.a.key(),
            Which::TakeB => self.b.key(),
            Which::Invalid => unreachable!(),
        }
    }

    fn value(&self) -> &[u8] {
        debug_assert!(self.is_valid());

        match self.which {
            Which::TakeA => self.a.value(),
            Which::TakeB => self.b.value(),
            Which::Invalid => unreachable!(),
        }
    }

    fn is_valid(&self) -> bool {
        self.which != Which::Invalid
    }

    fn next(&mut self) -> Result<()> {
        debug_assert!(self.is_valid());

        match self.which {
            Which::TakeA => {
                if self.b.is_valid() && self.a.key() == self.b.key() {
                    self.b.next()?;
                }
                self.a.next()?;
            }
            Which::TakeB => {
                self.b.next()?;
            }
            Which::Invalid => unreachable!(),
        }

        // update cached state
        self.which = Self::compute_which(&self.a, &self.b);

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
