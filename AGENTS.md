# Mini-LSM Project Guide for AI Agents

## Project Overview

Mini-LSM is an educational Rust project that guides users through building a key-value storage engine based on LSM-tree (Log-Structured Merge-tree) architecture. The project is structured as a tutorial course spanning 3 weeks, with each week containing 7 chapters (days) of incremental implementation tasks.

**Repository:** https://github.com/skyzh/mini-lsm  
**License:** Apache-2.0 (starter code and solution)  
**Course Materials:** CC BY-NC-SA 4.0

### Course Structure

- **Week 1:** Storage Format + Engine Skeleton
  - Day 1: Memtable
  - Day 2: Merge Iterator
  - Day 3: Block
  - Day 4: Sorted String Table (SST)
  - Day 5: Read Path
  - Day 6: Write Path
  - Day 7: SST Optimizations (Prefix Key Encoding + Bloom Filters)

- **Week 2:** Compaction and Persistence
  - Day 1: Compaction Implementation
  - Day 2: Simple Compaction Strategy (Traditional Leveled Compaction)
  - Day 3: Tiered Compaction Strategy (RocksDB Universal Compaction)
  - Day 4: Leveled Compaction Strategy (RocksDB Leveled Compaction)
  - Day 5: Manifest
  - Day 6: Write-Ahead Log (WAL)
  - Day 7: Batch Write and Checksums

- **Week 3:** Multi-Version Concurrency Control (MVCC)
  - Day 1: Timestamp Key Encoding
  - Day 2: Snapshot Read - Memtables and Timestamps
  - Day 3: Snapshot Read - Transaction API
  - Day 4: Watermark and Garbage Collection
  - Day 5: Transactions and Optimistic Concurrency Control
  - Day 6: Serializable Snapshot Isolation
  - Day 7: Compaction Filters

## Technology Stack

- **Language:** Rust (Edition 2024)
- **Rust Toolchain:** Stable with `rustfmt` and `clippy` components
- **Build System:** Cargo with workspace configuration
- **Documentation:** mdbook (for the course/tutorial)
- **Testing:** cargo-nextest (recommended), built-in cargo test

### Key Dependencies

All crates use these core dependencies:
- `anyhow` - Error handling
- `bytes` - Efficient byte string handling (Arc<[u8]> like)
- `crossbeam-epoch` - Lock-free memory reclamation
- `crossbeam-skiplist` - Concurrent skip list for memtable
- `crossbeam-channel` - Multi-producer multi-consumer channels
- `parking_lot` - Efficient synchronization primitives
- `ouroboros` - Self-referential structs
- `moka` - Caching library
- `clap` - CLI argument parsing
- `rand` - Random number generation
- `serde`/`serde_json` - Serialization
- `farmhash` - Hashing for bloom filters
- `crc32fast` - Checksums (week 2+)
- `nom` - Parser combinators
- `rustyline` - Interactive CLI
- `tempfile` - Test utilities (dev dependency)

## Project Structure

This is a Cargo workspace with 4 crates:

```
mini-lsm/                 # Reference solution for Week 1-2
├── src/
│   ├── bin/              # CLI binaries (mini-lsm-cli-ref, compaction-simulator-ref)
│   ├── block/            # SST block builder and iterator
│   ├── compact/          # Compaction strategies (simple, tiered, leveled)
│   ├── iterators/        # Storage iterators (merge, concat, two-merge)
│   ├── mvcc/             # MVCC module (txn, watermark)
│   ├── table/            # SSTable (bloom, builder, iterator)
│   ├── tests/            # Test cases organized by week/day
│   ├── debug.rs          # Debug utilities (dump_structure)
│   ├── key.rs            # Key types and timestamp handling
│   ├── lib.rs            # Module exports
│   ├── lsm_iterator.rs   # LSM storage iterator
│   ├── lsm_storage.rs    # Main storage engine
│   ├── manifest.rs       # Metadata persistence
│   ├── mem_table.rs      # In-memory table (skiplist-based)
│   ├── mvcc.rs           # MVCC module root
│   ├── wal.rs            # Write-ahead log
│   └── tests.rs          # Test module declarations
└── Cargo.toml

mini-lsm-mvcc/            # Reference solution for Week 3 (MVCC)
├── src/                  # Same structure as mini-lsm with MVCC extensions
└── Cargo.toml

mini-lsm-starter/         # Starter code for students
├── src/                  # Skeleton implementation with TODOs
└── Cargo.toml

mini-lsm-xtask/           # Build automation and tooling
├── src/
│   └── main.rs           # Custom cargo commands
└── Cargo.toml

mini-lsm-book/            # Course documentation (mdbook)
├── book/                 # Generated HTML output
├── src/                  # Markdown source files
├── book.toml             # mdbook configuration
└── custom.css            # Styling
```

## Build and Development Commands

### Setup (First Time)

```bash
# Install required tools
cargo x install-tools
# Installs: cargo-nextest, mdbook, mdbook-toc, cargo-semver-checks
```

### For Students (Working on mini-lsm-starter)

```bash
# Copy test cases for a specific week/day
cargo x copy-test --week 1 --day 1

# Run checks (format, check, test, clippy) on starter code
cargo x scheck

# Run the CLI interactively
cargo run --bin mini-lsm-cli

# Run the compaction simulator
cargo run --bin compaction-simulator
```

### For Course Developers (Working on mini-lsm/mini-lsm-mvcc)

```bash
# Run full check suite (format, check, test, clippy)
cargo x check

# Build and serve the book locally
cargo x book

# Run CI jobs locally
cargo x ci

# Sync starter repo and reference solution (after API changes)
cargo x sync

# Copy test cases to starter
cargo x copy-test --week 1 --day 1
```

### Reference Solution Demos

```bash
# Run reference CLI (week 1-2)
cargo run --bin mini-lsm-cli-ref

# Run MVCC reference CLI (week 3)
cargo run --bin mini-lsm-cli-mvcc-ref

# Run compaction simulators
cargo run --bin compaction-simulator-ref
cargo run --bin compaction-simulator-mvcc-ref
```

## Code Style Guidelines

### Formatting

Uses `rustfmt` with nightly-specific options defined in `rustfmt.toml.nightly`:

```toml
comment_width = 120
format_code_in_doc_comments = true
format_macro_bodies = true
format_macro_matchers = true
normalize_comments = true
normalize_doc_attributes = true
imports_granularity = "Module"
group_imports = "StdExternalCrate"
reorder_impl_items = true
reorder_imports = true
tab_spaces = 4
wrap_comments = true
```

### License Headers

All Rust source files must include the Apache-2.0 license header:

```rust
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
```

The project uses `licensesnip` for license header management (see `.licensesnip` and `licensesnip.config.jsonc`).

### Code Organization

- Each module is in its own file/directory
- Public APIs are exported in `lib.rs`
- Tests are organized in `src/tests/` with harness utilities
- CLI binaries are in `src/bin/`
- Use `anyhow::Result` for error handling
- Prefer `bytes::Bytes` for byte data

## Testing Instructions

### Test Structure

Tests are organized by course week and day:
- `src/tests/week1_day1.rs` through `src/tests/week2_day6.rs`
- `src/tests/harness.rs` - Shared test utilities
- `mini-lsm-mvcc/src/tests/` - Week 3 MVCC tests

### Running Tests

```bash
# Run all tests for starter package
cargo test --package mini-lsm-starter

# Run with nextest (recommended)
cargo nextest run --package mini-lsm-starter

# Run specific test file
cargo test --package mini-lsm-starter week1_day1

# Copy tests before running
cargo x copy-test --week 1 --day 1
cargo x scheck
```

### Test Harness

The `harness.rs` provides:
- `MockIterator` - For testing iterator implementations
- `generate_sst()` / `generate_sst_with_ts()` - SST file generation
- `compaction_bench()` - Compaction algorithm validation
- `check_compaction_ratio()` - Compaction strategy verification
- `sync()` - Force memtable freeze and flush

## Key Implementation Notes

### LSM Storage Architecture

1. **MemTable:** In-memory skiplist (`crossbeam_skiplist::SkipMap`), single mutable + multiple immutable
2. **SST (Sorted String Table):** Disk-based sorted key-value files with block-based structure
3. **Manifest:** Metadata file tracking SST files and levels
4. **WAL:** Write-ahead log for durability (optional, week 2+)
5. **Compaction:** Background merging of SST files (3 strategies)

### MVCC (Week 3)

- Timestamp-based versioning with `KeySlice` carrying timestamp
- Watermark for garbage collection
- Optimistic concurrency control (OCC) for transactions
- Serializable snapshot isolation support

### Iterator Pattern

Storage engines implement `StorageIterator` trait:
```rust
pub trait StorageIterator {
    type KeyType<'a>;
    fn next(&mut self) -> Result<()>;
    fn key(&self) -> Self::KeyType<'_>;
    fn value(&self) -> &[u8];
    fn is_valid(&self) -> bool;
}
```

### Feature Flags

The code uses `TS_ENABLED` constant in `key.rs` to control MVCC timestamp features. The starter code begins without timestamps (Week 1-2), and Week 3 enables MVCC support.

## CI/CD

GitHub Actions workflow (`.github/workflows/check.yml`):
- Format check: `cargo fmt --package mini-lsm-starter --check`
- Clippy: `cargo clippy --package mini-lsm-starter` (with `-Dwarnings`)
- Test: `cargo test --package mini-lsm-starter`

Uses `rust-cache` for faster builds.

## Security Considerations

- This is an educational project, not production-ready
- No encryption at rest or in transit
- No authentication/authorization
- File permissions should be restricted for database directories
- WAL provides durability but not Byzantine fault tolerance

## Contributing

- Students should work only in `mini-lsm-starter/`
- Course developers modify `mini-lsm/` and `mini-lsm-mvcc/`
- After changing public APIs in reference solutions, run `cargo x sync`
- Follow the license header requirement
- Maintain test compatibility with the course structure

## Useful Resources

- **Course Book:** https://skyzh.github.io/mini-lsm
- **Discord Community:** Available via link in README
- **Community Solutions:** See `SOLUTIONS.md`
- **Solution Checkpoint Repo:** https://github.com/skyzh/mini-lsm-solution-checkpoint

## Common Tasks for AI Agents

### Adding a New Feature

1. Implement in `mini-lsm/src/` first (reference solution)
2. Create corresponding test in `mini-lsm/src/tests/`
3. Copy test to starter: `cargo x copy-test --week X --day Y`
4. Add skeleton/TODO in `mini-lsm-starter/src/`
5. Document in `mini-lsm-book/src/`

### Fixing a Bug

1. Identify if bug is in starter code, reference solution, or book
2. Reference solution fixes go to `mini-lsm/` or `mini-lsm-mvcc/`
3. Starter code fixes go to `mini-lsm-starter/`
4. Run `cargo x scheck` to verify

### Refactoring

1. Ensure tests pass before and after
2. If changing public APIs, run `cargo x sync` afterwards
3. Update course materials if APIs change
