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

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        debug_assert_eq!(snapshot.levels[0].0, 1);
        debug_assert_eq!(self.options.max_levels, snapshot.levels.len());

        if self.options.level0_file_num_compaction_trigger <= snapshot.l0_sstables.len() {
            debug_assert!(1 <= self.options.max_levels);
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: snapshot.levels[0].0,
                lower_level_sst_ids: snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: snapshot.levels[0].0 == self.options.max_levels,
            });
        }

        for window in snapshot.levels.windows(2) {
            let (upper, lower) = (&window[0], &window[1]);
            if upper.1.is_empty() {
                continue;
            }

            // condition for compacting to the lower level:
            // [lower size] / [upper size] < ratio limit
            if self.options.size_ratio_percent * upper.1.len() <= 100 * lower.1.len() {
                continue;
            }

            return Some(SimpleLeveledCompactionTask {
                upper_level: Some(upper.0),
                upper_level_sst_ids: upper.1.clone(),
                lower_level: lower.0,
                lower_level_sst_ids: lower.1.clone(),
                is_lower_level_bottom_level: lower.0 == self.options.max_levels,
            });
        }

        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut removed = Vec::new();

        if let Some(upper_level) = task.upper_level {
            debug_assert_eq!(task.lower_level, upper_level + 1);

            let upper_idx = upper_level - 1;
            debug_assert!(upper_idx < snapshot.levels.len());
            debug_assert_eq!(task.upper_level_sst_ids, snapshot.levels[upper_idx].1);

            removed.extend(&snapshot.levels[upper_idx].1);
            snapshot.levels[upper_idx].1.clear();
        } else {
            debug_assert_eq!(task.lower_level, 1);

            debug_assert_eq!(task.upper_level_sst_ids, snapshot.l0_sstables);
            removed.extend(&snapshot.l0_sstables);
            snapshot.l0_sstables.clear();
        }

        let lower_idx = task.lower_level - 1;
        debug_assert_eq!(task.lower_level_sst_ids, snapshot.levels[lower_idx].1);
        removed.extend(&snapshot.levels[lower_idx].1);
        snapshot.levels[lower_idx].1 = output.to_owned();

        (snapshot, removed)
    }
}
