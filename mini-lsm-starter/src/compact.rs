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

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(
        &self,
        snapshot: &Arc<LsmStorageState>,
        task: &CompactionTask,
    ) -> Result<Vec<Arc<SsTable>>> {
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let l0_iter = l0_sstables
                    .iter()
                    .map(|i| Arc::clone(&snapshot.sstables[i]))
                    .map(|table| -> Result<_> {
                        let iter = SsTableIterator::create_and_seek_to_first(table)?;
                        Ok(Box::new(iter))
                    })
                    .collect::<Result<Vec<_>>>()
                    .map(MergeIterator::create)?;
                let l1_sstables = l1_sstables
                    .iter()
                    .map(|i| Arc::clone(&snapshot.sstables[i]))
                    .collect();
                let l1_iter = SstConcatIterator::create_and_seek_to_first(l1_sstables)?;
                let mut iter = TwoMergeIterator::create(l0_iter, l1_iter)?;

                // now build compacted SSTs from the iterator
                let mut compacted = Vec::new();
                let mut add_table = |builder: SsTableBuilder| -> Result<()> {
                    let id = self.next_sst_id();
                    let path = self.path_of_sst(id);
                    let sst = Arc::new(builder.build(id, Some(self.block_cache.clone()), path)?);
                    compacted.push(sst);
                    Ok(())
                };

                let mut builder = None;
                while iter.is_valid() {
                    if builder.is_none() {
                        builder = Some(SsTableBuilder::new(self.options.block_size));
                    }

                    // append the tuple
                    let builder_mut = builder.as_mut().unwrap();
                    let (key, value) = (iter.key(), iter.value());
                    if !value.is_empty() {
                        builder_mut.add(key, value);
                    }
                    iter.next()?;

                    if self.options.target_sst_size <= builder_mut.estimated_size() {
                        add_table(builder.take().unwrap())?;
                    }
                }
                if let Some(builder) = builder {
                    add_table(builder)?;
                }

                Ok(compacted)
            }
            _ => {
                unimplemented!();
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = self.state.read().clone();

        let l0_sstables = snapshot.l0_sstables.clone();
        let l1_sstables = snapshot.levels[0].1.clone();
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };

        eprintln!("force full compaction: {task:?}");
        let compacted = self.compact(&snapshot, &task)?;
        drop(snapshot);

        let state_lock = self.state_lock.lock();
        let mut new_state = self.state.read().as_ref().clone();
        assert_eq!(l0_sstables, new_state.l0_sstables);
        assert_eq!(l1_sstables, new_state.levels[0].1);

        // remove all L0 + L1 SSTs
        for i in l0_sstables.iter().chain(l1_sstables.iter()) {
            new_state.sstables.remove(i).unwrap();
        }

        // L0
        let mut l0_to_remove: HashSet<_> = l0_sstables.iter().copied().collect();
        new_state.l0_sstables = new_state
            .l0_sstables
            .iter()
            .filter(|i| !l0_to_remove.remove(i))
            .copied()
            .collect();
        assert!(l0_to_remove.is_empty());

        // L1
        let mut new_l1_sstables = Vec::with_capacity(compacted.len());
        for sst in compacted {
            let id = sst.sst_id();
            new_state.sstables.insert(id, sst);
            new_l1_sstables.push(id);
        }
        new_state.levels[0].1 = new_l1_sstables;

        // update state
        *self.state.write() = Arc::new(new_state);

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        // should flush if the number of memtables exceed the limit
        if self.options.num_memtable_limit < 1 + self.state.read().imm_memtables.len() {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
