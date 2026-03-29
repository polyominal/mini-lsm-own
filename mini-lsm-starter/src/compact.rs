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
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
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
                let l0_iter = MergeIterator::create(
                    l0_sstables
                        .iter()
                        .map(|i| {
                            let table = Arc::clone(&snapshot.sstables[i]);
                            let iter = SsTableIterator::create_and_seek_to_first(table)?;
                            Ok(Box::new(iter))
                        })
                        .collect::<Result<Vec<_>>>()?,
                );
                let l1_iter = SstConcatIterator::create_and_seek_to_first(
                    l1_sstables
                        .iter()
                        .map(|i| Arc::clone(&snapshot.sstables[i]))
                        .collect(),
                )?;
                let iter = TwoMergeIterator::create(l0_iter, l1_iter)?;
                self.sst_from_iter(iter, task.compact_to_bottom_level())
            }
            // [IMPLEMENTED VIA KIMI CODE] Handle Simple and Leveled compaction tasks
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids,
                ..
            })
            | CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids,
                ..
            }) => {
                if let Some(upper_level) = upper_level {
                    let upper_iter = SstConcatIterator::create_and_seek_to_first(
                        upper_level_sst_ids
                            .iter()
                            .map(|i| Arc::clone(&snapshot.sstables[i]))
                            .collect(),
                    )?;
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(
                        lower_level_sst_ids
                            .iter()
                            .map(|i| Arc::clone(&snapshot.sstables[i]))
                            .collect(),
                    )?;
                    let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                    self.sst_from_iter(iter, task.compact_to_bottom_level())
                } else {
                    let upper_iter = MergeIterator::create(
                        upper_level_sst_ids
                            .iter()
                            .map(|i| {
                                let table = Arc::clone(&snapshot.sstables[i]);
                                let iter = SsTableIterator::create_and_seek_to_first(table)?;
                                Ok(Box::new(iter))
                            })
                            .collect::<Result<Vec<_>>>()?,
                    );
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(
                        lower_level_sst_ids
                            .iter()
                            .map(|i| Arc::clone(&snapshot.sstables[i]))
                            .collect(),
                    )?;
                    let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                    self.sst_from_iter(iter, task.compact_to_bottom_level())
                }
            }
            // [IMPLEMENTED VIA KIMI CODE] Handle Tiered compaction task
            CompactionTask::Tiered(TieredCompactionTask { tiers, .. }) => {
                let mut iters = Vec::with_capacity(tiers.len());
                for (_, tier_sst_ids) in tiers {
                    let mut ssts = Vec::with_capacity(tier_sst_ids.len());
                    for id in tier_sst_ids.iter() {
                        ssts.push(Arc::clone(&snapshot.sstables[id]));
                    }
                    iters.push(Box::new(SstConcatIterator::create_and_seek_to_first(ssts)?));
                }
                self.sst_from_iter(MergeIterator::create(iters), task.compact_to_bottom_level())
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

        {
            let _state_lock = self.state_lock.lock();
            let mut new_state = self.state.read().as_ref().clone();
            assert_eq!(l0_sstables, new_state.l0_sstables);
            assert_eq!(l1_sstables, new_state.levels[0].1);

            // remove all L0 + L1 SSTs
            for i in l0_sstables.iter().chain(l1_sstables.iter()) {
                new_state.sstables.remove(i).unwrap();
            }

            // insert compacted SSTs to L1
            let mut new_l1_sstables = Vec::with_capacity(compacted.len());
            for sst in compacted {
                let id = sst.sst_id();
                new_state.sstables.insert(id, sst);
                new_l1_sstables.push(id);
            }
            new_state.l0_sstables.clear();
            new_state.levels[0].1 = new_l1_sstables;

            // update state
            *self.state.write() = Arc::new(new_state);
        }

        Ok(())
    }

    // [IMPLEMENTED VIA KIMI CODE] Restructured to insert SSTs before apply_compaction_result
    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = self.state.read().clone();
        let Some(task) = self
            .compaction_controller
            .generate_compaction_task(&snapshot)
        else {
            // if no task needs to be scheduled, return ok
            return Ok(());
        };

        // run the compaction and get a list of new SSTs
        let compacted = self.compact(&snapshot, &task)?;
        drop(snapshot);

        let state_lock = self.state_lock.lock();
        // First, insert the new SSTs into the state so that apply_compaction_result
        // can access them for sorting (needed by leveled compaction)
        let mut snapshot = self.state.read().as_ref().clone();
        let mut new_ids = Vec::with_capacity(compacted.len());
        for file_to_add in compacted {
            new_ids.push(file_to_add.sst_id());
            let result = snapshot.sstables.insert(file_to_add.sst_id(), file_to_add);
            debug_assert!(result.is_none());
        }
        let (mut new_state, files_to_remove) = self
            .compaction_controller
            .apply_compaction_result(&snapshot, &task, &new_ids, false);

        for file_to_remove in files_to_remove {
            let result = new_state.sstables.remove(&file_to_remove);
            debug_assert!(result.is_some(), "cannot remove {}.sst", file_to_remove);
        }

        // update state
        *self.state.write() = Arc::new(new_state);

        self.sync_dir()?;
        self.manifest
            .as_ref()
            .unwrap()
            .add_record(&state_lock, ManifestRecord::Compaction(task, new_ids))?;

        Ok(())
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
        // should flush if the number of memtables exceeds the limit
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

    /// Generate SSTs from an iterator.
    ///
    /// [IMPLEMENTED VIA KIMI CODE] Added compact_to_bottom_level parameter to filter tombstones
    fn sst_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut compacted = Vec::new();

        let mut builder = None;
        while iter.is_valid() {
            if builder.is_none() {
                builder = Some(SsTableBuilder::new(self.options.block_size));
            }

            // append the tuple
            let builder_mut = builder.as_mut().unwrap();
            let (key, value) = (iter.key(), iter.value());
            // Filter out tombstones (empty values) when compacting to bottom level
            if compact_to_bottom_level {
                if !value.is_empty() {
                    builder_mut.add(key, value);
                }
            } else {
                builder_mut.add(key, value);
            }
            iter.next()?;

            if self.options.target_sst_size <= builder_mut.estimated_size() {
                let builder = builder.take().unwrap();
                let id = self.next_sst_id();
                let path = self.path_of_sst(id);
                let sst = Arc::new(builder.build(id, Some(self.block_cache.clone()), path)?);
                compacted.push(sst);
            }
        }
        if let Some(builder) = builder {
            let id = self.next_sst_id();
            let path = self.path_of_sst(id);
            let sst = Arc::new(builder.build(id, Some(self.block_cache.clone()), path)?);
            compacted.push(sst);
        }

        Ok(compacted)
    }
}
