# Mini-LSM Project Guide for AI Agents

## Project Overview

Mini-LSM is an educational tutorial project for building a simple key-value storage engine based on LSM-Tree (Log-Structured Merge Tree) architecture. The project is structured as a 3-week course where students progressively implement a fully functional storage engine.

- **Repository**: https://github.com/skyzh/mini-lsm
- **Book/Course**: https://skyzh.github.io/mini-lsm
- **License**: Apache 2.0
- **Author**: Alex Chi Z

### Course Structure

| Week | Topic |
|------|-------|
| Week 1 | Storage Format + Engine Skeleton (Memtable, SST, Block, Iterator) |
| Week 2 | Compaction and Persistence (Compaction strategies, Manifest, WAL) |
| Week 3 | Multi-Version Concurrency Control (MVCC, Transactions, Snapshot Isolation) |

## Technology Stack

- **Language**: Rust (Edition 2024)
- **Build System**: Cargo
- **Documentation**: mdbook
- **Testing**: cargo-nextest
- **Minimum Rust Version**: Stable channel with rustfmt and clippy

### Key Dependencies

| Crate | Purpose |
|-------|---------|
| `bytes` | Efficient byte string handling |
| `crossbeam-skiplist` | Concurrent skip list for memtable |
| `crossbeam-epoch` | Lock-free memory management |
| `parking_lot` | High-performance synchronization primitives |
| `moka` | Caching (block cache) |
| `serde`/`serde_json` | Serialization for manifest |
| `farmhash` | Hashing for bloom filters |
| `crc32fast` | Checksum computation |
| `nom` | Parser combinator framework |
| `rustyline` | CLI readline support |
| `ouroboros` | Self-referential structs |
| `arc-swap` | Atomic swap for Arc pointers |
| `clap` | CLI argument parsing |
| `rand` | Random number generation |

## Project Structure

This is a Cargo workspace with the following crates:

```
├── Cargo.toml              # Workspace root
├── mini-lsm/               # Reference solution (Week 1-2)
├── mini-lsm-mvcc/          # Reference solution (Week 3 MVCC)
├── mini-lsm-starter/       # Starter code for students
├── mini-lsm-book/          # Course documentation (mdbook)
└── xtask/                  # Build automation tasks
```

### Crate Details

#### `mini-lsm` (Reference Solution - Weeks 1-2)
- Complete implementation for weeks 1-2
- Binaries: `mini-lsm-cli-ref`, `mini-lsm-wrapper-ref`, `compaction-simulator-ref`

#### `mini-lsm-mvcc` (Reference Solution - Week 3)
- MVCC-extended version with full 3-week implementation
- Binaries: `mini-lsm-cli-mvcc-ref`, `mini-lsm-wrapper-mvcc-ref`, `compaction-simulator-mvcc-ref`

#### `mini-lsm-starter` (Student Code)
- **Students should modify code here**
- Contains starter templates with `todo!()` placeholders
- Initially only includes tests up to week 2 day 2; additional tests are copied via `cargo x copy-test`

#### `mini-lsm-book`
- Course material written in Markdown
- Uses mdbook with mdbook-toc preprocessor
- Hosted at https://skyzh.github.io/mini-lsm

#### `xtask`
- Build automation and development tasks
- Provides `cargo x` alias commands

## Build and Test Commands

### For Students (Working on `mini-lsm-starter`)

```bash
# Install required tools (run once)
cargo x install-tools

# Copy test cases for a specific week/day
cargo x copy-test --week 1 --day 1

# Check starter code (format, check, test, clippy)
cargo x scheck

# Run the CLI
cargo run --bin mini-lsm-cli

# Run the compaction simulator
cargo run --bin compaction-simulator
```

### For Course Developers (Working on reference solutions)

```bash
# Install required tools
cargo x install-tools

# Full check (format, check, test, clippy) on reference solutions
cargo x check

# Build and serve the book locally
cargo x book

# Sync starter repo with reference solution (check API compatibility)
cargo x sync

# Run CI checks locally
cargo x ci
```

### Running Reference Solutions

```bash
# Week 1-2 reference CLI
cargo run --bin mini-lsm-cli-ref

# Week 3 MVCC reference CLI
cargo run --bin mini-lsm-cli-mvcc-ref

# Compaction simulators
cargo run --bin compaction-simulator-ref
cargo run --bin compaction-simulator-mvcc-ref
```

### Standard Cargo Commands

```bash
# Format code
cargo fmt

# Check compilation
cargo check --all-targets

# Run tests
cargo test --package mini-lsm-starter
# or with nextest:
cargo nextest run

# Run clippy
cargo clippy --all-targets

# Build book
cd mini-lsm-book && mdbook build

# Serve book locally
cd mini-lsm-book && mdbook serve
```

## Code Style Guidelines

### Rustfmt Configuration

The project uses nightly rustfmt features defined in `rustfmt.toml.nightly`:

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

To apply these settings:
```bash
rustfmt +nightly --edition 2024
```

### License Header

All Rust source files must include the Apache 2.0 license header:

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

The `licensesnip` tool is configured to automatically add headers.

### Coding Conventions

1. **Imports**: Group as `StdExternalCrate` (std lib, external crates, local modules)
2. **Documentation**: Document all public APIs with doc comments
3. **Error Handling**: Use `anyhow::Result` for error propagation
4. **Unsafe Code**: Minimize unsafe code; use `crossbeam-epoch` for lock-free structures
5. **Naming**: Follow standard Rust naming conventions (snake_case for functions/variables, PascalCase for types)

## Testing Instructions

### Test Organization

Tests are organized by week and day:
- `src/tests/week1_day1.rs` through `src/tests/week1_day7.rs`
- `src/tests/week2_day1.rs` through `src/tests/week2_day6.rs`
- `src/tests/week3_day1.rs` through `src/tests/week3_day7.rs`
- `src/tests/harness.rs` - Shared test utilities

### Running Tests

```bash
# Run all tests for the starter package
cargo test --package mini-lsm-starter

# Run a specific test
cargo test --package mini-lsm-starter test_name

# Run tests with output
cargo test --package mini-lsm-starter -- --nocapture

# Using nextest (recommended)
cargo nextest run
```

### Test Harness

The `harness.rs` module provides:
- `MockIterator` - For testing iterators
- Test helper functions for SST creation
- Storage state verification utilities

### Copying Test Cases

When working through the course, copy tests incrementally:

```bash
# After completing week 1 day 1, get tests for day 2
cargo x copy-test --week 1 --day 2
```

This copies test files from the reference solution and updates `tests.rs` module declarations.

## Module Architecture

### Core Modules (in `lib.rs`)

| Module | Description |
|--------|-------------|
| `block` | SST block format and builder |
| `table` | SSTable (Sorted String Table) implementation |
| `mem_table` | In-memory skip list memtable |
| `iterators` | Iterator traits and implementations |
| `lsm_storage` | Main storage engine |
| `lsm_iterator` | Storage engine iterator |
| `compact` | Compaction algorithms |
| `manifest` | Metadata persistence |
| `wal` | Write-Ahead Log |
| `mvcc` | Multi-version concurrency control |
| `key` | Key encoding/decoding |
| `debug` | Debug utilities |

### Iterator Pattern

The storage engine uses a composable iterator pattern:
- `StorageIterator` trait - Core iteration interface
- `MergeIterator` - Merges sorted iterators
- `TwoMergeIterator` - Merges two iterators
- `SstConcatIterator` - Concatenates SST iterators
- `BlockIterator` - Iterates within a block

## Development Workflow

### For Students

1. Start with `mini-lsm-starter`
2. Follow the book at https://skyzh.github.io/mini-lsm
3. Implement `todo!()` placeholders
4. Copy tests via `cargo x copy-test` as you progress
5. Run `cargo x scheck` to verify your implementation

### For Contributors

1. Reference solutions are in `mini-lsm` and `mini-lsm-mvcc`
2. After modifying public APIs, run `cargo x sync` to check compatibility
3. Ensure all tests pass with `cargo x check`
4. Update the book in `mini-lsm-book/src/` if needed
5. CI runs: format check, clippy (with `-Dwarnings`), and tests

### CI Pipeline

The GitHub Actions workflow (`.github/workflows/check.yml`) runs:
1. Format check: `cargo fmt --package mini-lsm-starter --check`
2. Clippy: `cargo clippy --package mini-lsm-starter` (with `RUSTFLAGS="-Dwarnings"`)
3. Tests: `cargo test --package mini-lsm-starter`

## Security Considerations

1. **Checksums**: SST files use CRC32 checksums for data integrity
2. **WAL**: Write-ahead logging ensures durability
3. **MVCC**: Timestamp-based isolation prevents dirty reads
4. **File Permissions**: Uses standard filesystem permissions
5. **No Encryption**: The tutorial implementation does not include encryption

## Common Issues

### Build Issues

- Ensure you're using the stable Rust toolchain
- Run `cargo x install-tools` to install required dependencies
- The project requires `mdbook`, `mdbook-toc`, `cargo-nextest`, and `cargo-semver-checks`

### Test Failures

- Make sure you've copied the correct test files: `cargo x copy-test --week X --day Y`
- Check that `TS_ENABLED` constant is set correctly for the week you're implementing
- Some weeks require previous weeks' implementations to be complete

### API Compatibility

When modifying the reference solution, use `cargo x sync` to verify that the starter code remains compatible. This uses `cargo-semver-checks` to detect breaking changes.

## Additional Resources

- **Book**: https://skyzh.github.io/mini-lsm
- **Discord Community**: https://skyzh.dev/join/discord
- **Solution Checkpoints**: https://github.com/skyzh/mini-lsm-solution-checkpoint
- **Community Solutions**: See `SOLUTIONS.md`

## Related Projects

- [SlateDB](https://slatedb.io/) - LSM engine over object storage
- [Tonbo](https://tonbo.io/) - Parquet-based LSM on object storage
