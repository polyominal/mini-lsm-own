# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A personal implementation of [Mini-LSM](https://github.com/skyzh/mini-lsm) — a Log-Structured Merge (LSM) tree storage engine built as a 3-week course project. The current work is in `mini-lsm-starter/`, which is the student implementation package.

## Commands

```bash
# Build
cargo build -p mini-lsm-starter

# Run all tests for the starter package
cargo test -p mini-lsm-starter

# Run a specific test file (e.g., week 2 day 6)
cargo test -p mini-lsm-starter --test week2_day6

# Run a specific test by name
cargo test -p mini-lsm-starter --test week2_day6 test_name

# Copy test cases for a specific day
cargo x copy-test --week 2 --day 6

# Lint/check the starter package
cargo x scheck
# Or directly:
cargo fmt --package mini-lsm-starter
cargo clippy --package mini-lsm-starter

# Run the interactive CLI
cargo run --bin mini-lsm-cli

# Run compaction simulator
cargo run --bin compaction-simulator
```

## Workspace Layout

```
mini-lsm-own/
├── mini-lsm/           # Reference solution (weeks 1-2)
├── mini-lsm-mvcc/      # Reference solution (week 3, MVCC)
├── mini-lsm-starter/   # THIS is where implementation work happens
├── xtask/              # Build automation (cargo x <cmd>)
└── mini-lsm-book/      # Course materials (mdbook)
```

**When reading reference implementations**, look in `mini-lsm/src/` for the completed solution to weeks 1-2.

## Architecture

### Core Data Flow

```
put(key, value)
  → write to MemTable (crossbeam SkipMap + optional WAL)
  → when MemTable exceeds size_limit: freeze → imm_memtables
  → flush thread: imm_memtable → SSTable on disk (L0)
  → compaction thread: merge L0 SSTables into deeper levels
```

### Key Types and Files (`mini-lsm-starter/src/`)

| File | Purpose |
|------|---------|
| `lsm_storage.rs` | Main engine: `MiniLsm` (public API) + `LsmStorageInner` + `LsmStorageState` |
| `mem_table.rs` | Skiplist-based memtable with optional WAL |
| `table.rs` | `SsTableBuilder`, `SsTable`, `SsTableIterator` |
| `block.rs` | `BlockBuilder`, `Block`, `BlockIterator` (4KB fixed-size units) |
| `manifest.rs` | JSON operation log with CRC32 checksums |
| `wal.rs` | Per-memtable write-ahead log |
| `key.rs` | `Key<T>` wrapper: `KeySlice`, `KeyVec`, `KeyBytes` |
| `iterators/` | `MergeIterator`, `TwoMergeIterator`, `ConcatIterator`, `LsmIterator` |
| `compact/` | Compaction strategies: `Leveled`, `SimpleLeveled`, `Tiered` |

### State Snapshot (`LsmStorageState`)

The in-memory state protected by `RwLock`:
- `memtable`: Current mutable memtable
- `imm_memtables`: Vec of frozen memtables awaiting flush
- `l0_sstables`: SSTable IDs at level 0 (unordered)
- `levels`: Vec of (level, Vec<sst_id>) for L1+
- `sstables`: `HashMap<usize, Arc<SsTable>>` — all open SSTs

### Thread Model

- **Flush thread**: Watches `imm_memtables`, writes oldest to L0 SST
- **Compaction thread**: Triggered by level size thresholds, merges SSTables
- Coordination via crossbeam channels; both threads run until `MiniLsm::close()`

### Key Encoding

`Key<T>` is a generic wrapper. For weeks 1-2, keys are plain byte slices. Week 3 (MVCC) adds timestamps. The `ts()` method is gated behind `#[cfg]` features — don't add timestamp handling in the starter unless implementing week 3.

### Compaction Controllers

All implement a common pattern: `generate_compaction_task()` → `CompactionTask` → execute merge → `apply_compaction_result()` → update manifest + state. The `NoCompaction` strategy is used in tests that don't exercise compaction.

### Manifest and Recovery

On `open()`, the manifest is replayed to reconstruct which SSTs exist and at which levels. WAL files are replayed into memtables for unflushed entries. SST files are opened from disk and inserted into `sstables` HashMap.

## Test Structure

Tests are in `mini-lsm-starter/tests/`:
- `week1_day{1-7}.rs` — Memtable, Block, SSTable, read/write paths
- `week2_day{1-7}.rs` — Compaction, manifest, WAL, checksums
- `week3_day{1-7}.rs` — MVCC, transactions, serializable isolation
- `harness.rs` — Shared test utilities

Tests for a given day must be copied in with `cargo x copy-test --week W --day D` before they appear.
