# LSM Tree Storage Engine

![Build](https://img.shields.io/badge/build-passing-brightgreen)
![C++17](https://img.shields.io/badge/C%2B%2B-17-blue.svg)
![Platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux-orange)

> A production‑quality, embedded key‑value store built in **C++17**, implementing the Log‑Structured Merge‑Tree (LSM) storage model — the same architecture powering Google's LevelDB and Meta's RocksDB.

---

## Table of Contents

- [Overview](#overview)
- [How It Works](#how-it-works)
- [Features](#features)
- [Quick Start](#quick-start)
- [Directory Structure](#directory-structure)
- [Building](#building)
- [Running Tests](#running-tests)
- [Usage](#usage)
- [Configuration Reference](#configuration-reference)
- [Design Notes](#design-notes)
- [Performance](#performance)

---

## Overview

This engine provides a **persistent, ordered, thread‑safe key‑value store** where both keys and values are arbitrary byte strings. It is designed for workloads that are write‑heavy, where data volume exceeds available RAM, and where durable persistence with crash recovery is required.

The implementation closely follows the internals of LevelDB, covering the full data lifecycle from writes landing in memory through compaction into persistent SSTable files on disk.

---

## How It Works

Data flows through three distinct layers:

```
  WRITE PATH
  ----------
  db->Put("key", "val")
        │
        ▼
  ┌─────────────┐   WAL     ┌─────────────────┐
  │  MemTable   │ ────────► │ Write-Ahead Log │  (durability)
  │  (SkipList) │           └─────────────────┘
  └──────┬──────┘
         │  (when full, ~4 MB)
         ▼
  ┌─────────────────────────────────────────────────────┐
  │                  SSTable Files (on disk)            │
  │  Level 0  ┌─────┐ ┌─────┐ ┌─────┐ (may overlap)     │
  │  Level 1  └──┬──┘ └──┬──┘ └──┬──┘                   │
  │            Background Compaction Thread             │
  │  Level 2  ─────────────────────────────────────     │
  │  ...      (non-overlapping, sorted key ranges)      │
  └─────────────────────────────────────────────────────┘

  READ PATH
  ---------
  db->Get("key")
        │
        ├── 1. Check MemTable (in-memory, newest)
        ├── 2. Check Immutable MemTable (being flushed)
        └── 3. Scan SSTable levels (L0 → L6)
                  └── Bloom Filter check → Block Cache → Disk Read
```

---

## Features

| Feature | Description |
|---|---|
| **MemTable + SkipList** | O(log n) in‑memory writes using a lock‑free probabilistic skiplist |
| **Write‑Ahead Log** | Sequential disk log for crash recovery before data hits an SSTable |
| **Group Commit** | Leader‑follower batching of concurrent writes into a single WAL record for higher throughput |
| **SSTables** | Immutable, block‑structured files with prefix‑compressed keys and CRC32 checksums |
| **Bloom Filters** | Per‑SSTable probabilistic filters that eliminate unnecessary disk I/O for missing keys |
| **Bloom‑Optimized Compaction** | Bloom filters skip unnecessary tombstone retention during multi‑level compaction |
| **Block Index** | Binary‑searchable per‑SSTable index for fast key lookups without full scans |
| **Multi‑Level Compaction** | Background thread merges and de‑duplicates SSTables across 7 levels |
| **Persistent File Handles** | Each SSTable keeps a single file handle open, protected by a mutex, eliminating per‑read open/close overhead |
| **LRU Block Cache** | Configurable in‑memory block cache to serve hot data without disk access |
| **Crash Recovery** | Replays the WAL on `DB::Open` to restore unflushed MemTable writes |
| **Thread‑Safe API** | All public APIs are safe for concurrent access from multiple threads |
| **Safe Shutdown** | Joinable background compaction thread with graceful shutdown via `shutting_down_` flag |
| **`shared_ptr` Ownership** | Table objects are reference‑counted; iterators prevent premature cache eviction |
| **Zstd Compression** | Optional block‑level compression (compile with `-DLSM_ENABLE_ZSTD=ON`) |

---

## Quick Start

```cpp
#include "lsm/db.h"
#include "lsm/options.h"

int main() {
    lsm::Options opts;
    opts.create_if_missing = true;
    opts.write_buffer_size = 4 * 1024 * 1024; // 4 MB MemTable
    opts.bloom_bits_per_key = 10;              // Bloom filter enabled

    lsm::DB* db = nullptr;
    lsm::Status s = lsm::DB::Open(opts, "my_database", &db);
    if (!s.ok()) { /* handle error */ }

    // Write
    s = db->Put(lsm::WriteOptions(), "hello", "world");

    // Read
    std::string value;
    s = db->Get(lsm::ReadOptions(), "hello", &value);
    // value == "world"

    // Delete
    s = db->Delete(lsm::WriteOptions(), "hello");

    // Check for not-found
    s = db->Get(lsm::ReadOptions(), "hello", &value);
    if (s.IsNotFound()) { /* key was deleted */ }

    delete db;
}
```

**Compile and run** (assuming the library is built in `./build`):

```bash
g++ -std=c++17 -Iinclude -Isrc main.cpp -Lbuild -llsm_engine -lpthread -o demo
./demo
```

---

## Directory Structure

```
lsm_engine/
├── include/lsm/                  # Public API — link against these headers only
│   ├── db.h                      # DB::Open, Put, Get, Delete, NewIterator
│   ├── options.h                 # Options, ReadOptions, WriteOptions
│   ├── status.h                  # Status return type
│   ├── slice.h                   # Zero-copy string/memory reference
│   ├── comparator.h              # Pluggable key ordering
│   └── iterator.h                # Bidirectional sorted iterator interface
│
├── src/
│   ├── db/                       # Database core logic
│   │   ├── db_impl.cc/h          # Main DB implementation (writes, reads, scheduling)
│   │   ├── memtable.cc/h         # In-memory sorted table (SkipList-backed)
│   │   ├── skiplist.h            # Lock-free concurrent skiplist
│   │   ├── wal.cc/h              # Write-Ahead Log writer/reader
│   │   ├── compaction.cc/h       # Compaction policy, input selection, scoring
│   │   ├── version_set.cc/h      # Manages the set of live SSTable files per level
│   │   ├── version_edit.cc/h     # MANIFEST log record (atomic version transitions)
│   │   └── merger.cc/h           # MergingIterator for sorted merge of N iterators
│   │
│   ├── table/                    # SSTable format: reading, writing, caching
│   │   ├── sstable_builder.cc/h  # Builds sorted, indexed SSTable files
│   │   ├── sstable_reader.cc/h   # Opens and queries SSTable files
│   │   ├── format.cc/h           # Block, Footer, BlockHandle encoding/decoding
│   │   ├── table_cache.cc/h      # LRU cache of open Table objects
│   │   └── iterator.cc           # Empty/error iterator helpers
│   │
│   └── util/                     # Shared utilities
│       ├── bloom.cc/h            # Bloom filter (create & query)
│       ├── cache.cc/h            # Generic LRU cache
│       ├── coding.cc/h           # Varint and fixed-width integer encoding
│       ├── crc32.cc/h            # CRC32c checksum
│       ├── hash.cc/h             # Murmur-style hash
│       ├── comparator.cc         # BytewiseComparator implementation
│       ├── options.cc            # Options defaults
│       └── status.cc             # Status message formatting
│
├── tests/                        # GoogleTest unit and integration tests
│   ├── test_bloom.cc             # Bloom filter correctness
│   ├── test_compaction.cc        # Bulk write → flush → compaction → read-back
│   ├── test_concurrency.cc       # Stress: concurrent reads+writes+deletes + compaction
│   ├── test_crash_recovery.cc    # Open/close cycles, destroy/recreate
│   ├── test_db.cc                # Full DB API (put, get, delete, iteration)
│   ├── test_group_commit.cc      # Multi-threaded write batching
│   ├── test_memtable.cc          # MemTable insert, lookup, iteration
│   └── test_sstable.cc           # SSTable build, open, and scan
│
├── examples/
│   └── demo.cc                   # Minimal working demo (put, get, delete, iterate)
│
├── .github/workflows/
│   └── ci.yml                    # GitHub Actions CI (Ubuntu + Windows)
│
└── CMakeLists.txt                # Build configuration (CMake 3.14+)
```

---

## Building

### Prerequisites

- **C++17** compiler (GCC 10+, Clang 11+, or MSVC 2019+)
- **CMake** 3.14 or newer
- **Ninja** or `make`
- Internet access on first build (GoogleTest is auto‑fetched)

### Steps

```bash
# 1. Clone
git clone <repo-url>
cd lsm_engine

# 2. Configure (downloads GoogleTest on first run)
cmake -S . -B build -G Ninja

# 3. Build everything (library + tests + demo)
cmake --build build -j 4
```

> **Windows note:** MSYS2 with the ucrt64 toolchain (GCC 15+) is fully supported.

### Optional: Zstd Compression

```bash
cmake -S . -B build -G Ninja -DLSM_ENABLE_ZSTD=ON
```

---

## Running Tests

```bash
cd build
ctest --output-on-failure
```

Expected output:

```
Test project .../lsm_engine/build
    Start 1: test_bloom
1/8 Test #1: test_bloom .......................   Passed    0.03 sec
    Start 2: test_compaction
2/8 Test #2: test_compaction ..................   Passed    1.07 sec
    Start 3: test_db
3/8 Test #3: test_db ..........................   Passed    0.25 sec
    Start 4: test_memtable
4/8 Test #4: test_memtable ....................   Passed    0.04 sec
    Start 5: test_sstable
5/8 Test #5: test_sstable .....................   Passed    0.04 sec
    Start 6: test_group_commit
6/8 Test #6: test_group_commit ................   Passed    0.15 sec
    Start 7: test_crash_recovery
7/8 Test #7: test_crash_recovery ..............   Passed    0.30 sec
    Start 8: test_concurrency
8/8 Test #8: test_concurrency .................   Passed    0.20 sec

100% tests passed, 0 tests failed out of 8
```

Run an individual test directly:

```bash
./test_compaction.exe   # Windows
./test_compaction       # Linux/macOS
```

---

## Usage

Link your target against `lsm_engine` and include from `include/`:

```cmake
target_link_libraries(my_app PRIVATE lsm_engine)
target_include_directories(my_app PRIVATE path/to/lsm_engine/include)
```

### Basic Operations

```cpp
#include "lsm/db.h"
#include "lsm/options.h"

int main() {
    lsm::Options opts;
    opts.create_if_missing = true;

    lsm::DB* db = nullptr;
    lsm::Status s = lsm::DB::Open(opts, "my_database", &db);
    if (!s.ok()) { /* handle error */ }

    // Write
    s = db->Put(lsm::WriteOptions(), "hello", "world");

    // Read
    std::string value;
    s = db->Get(lsm::ReadOptions(), "hello", &value);
    // value == "world"

    // Delete
    s = db->Delete(lsm::WriteOptions(), "hello");

    // Check for not-found
    s = db->Get(lsm::ReadOptions(), "hello", &value);
    if (s.IsNotFound()) { /* key was deleted */ }

    delete db;
}
```

### Iterating in Sorted Order

```cpp
lsm::Iterator* it = db->NewIterator(lsm::ReadOptions());

for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::cout << it->key().ToString()
              << " -> "
              << it->value().ToString() << "\n";
}

// Seek to a specific key prefix
it->Seek("user:");
while (it->Valid() && it->key().starts_with("user:")) {
    // process ...
    it->Next();
}

delete it; // Always delete the iterator before deleting the DB
```

### Durable Writes (fsync)

```cpp
lsm::WriteOptions sync_opts;
sync_opts.sync = true; // Blocks until WAL is flushed to disk
db->Put(sync_opts, "critical", "data");
```

---

## Configuration Reference

All options are set in `lsm::Options` and passed to `DB::Open`.

| Option | Default | Description |
|---|---|---|
| `create_if_missing` | `true` | Create the database directory if it does not exist |
| `error_if_exists` | `false` | Return an error if the database already exists |
| `write_buffer_size` | `4 MB` | Size of the in‑memory MemTable before it is flushed to an SSTable |
| `block_size` | `4 KB` | Target size of each data block inside an SSTable |
| `block_restart_interval` | `16` | Number of keys between prefix‑compression restart points |
| `bloom_bits_per_key` | `10` | Bloom filter density (bits per key); `0` disables bloom filters |
| `block_cache_capacity` | `8 MB` | Capacity of the shared LRU block cache; `0` disables caching |
| `compression` | `kNoCompression` | Block compression; `kZstdCompression` available if built with Zstd |
| `max_open_files` | `1000` | Maximum number of SSTable file handles held open |
| `max_file_size` | `2 MB` | Maximum size of a single SSTable output file |
| `paranoid_checks` | `false` | Verify CRC32 checksums on every block read |

**Compaction thresholds** (compile‑time constants in `options.h`):

| Constant | Value | Meaning |
|---|---|---|
| `kL0_CompactionTrigger` | 4 files | Start compaction when L0 reaches this many files |
| `kL0_SlowdownWritesTrigger` | 8 files | Throttle writes when L0 reaches this many files |
| `kL0_StopWritesTrigger` | 12 files | Block writes until compaction catches up |
| `kNumLevels` | 7 | Total number of compaction levels |

---

## Design Notes

### Thread Safety

All public DB methods (`Put`, `Get`, `Delete`, `NewIterator`) are thread‑safe. A single `std::mutex` serializes all writes; reads and compaction run concurrently.

### Group Commit

Concurrent `Put`/`Delete` calls are batched by a leader‑follower protocol. The thread at the front of the `writers_` deque becomes the **leader**, collects all pending writers into a single WAL record (up to 1 MB), writes the record once, applies all entries to the memtable, and signals the follower threads that their writes are complete. If **any** writer in the batch requested `sync`, the entire batch is fsynced. This dramatically reduces per‑write WAL overhead under contention.

### SSTable Format

Each SSTable file has the following layout:

```
[Data Block 0] [Data Block 1] ... [Data Block N]
[Metaindex Block]  ← points to the bloom filter block
[Bloom Filter Block]
[Index Block]      ← one entry per data block (key + handle)
[Footer]           ← fixed-size, points to metaindex + index
```

Every block ends with a 1‑byte compression type and a 4‑byte CRC32c checksum.

### Persistent File Handles

Each `Table` object holds a single `std::ifstream` opened once during `Table::Open`. All subsequent `ReadBlock` calls reuse this persistent handle, protected by a `std::mutex` for thread‑safe concurrent access. This eliminates the overhead of opening and closing a file handle per read, while still allowing safe concurrent access from multiple threads sharing the same `Table` object via the cache.

### Ownership Model

`Table` objects in the `TableCache` are managed via `std::shared_ptr<Table>`. When an iterator is created from a cached table, it captures a `shared_ptr` copy. If the LRU cache evicts the entry while an iterator is still alive, the `Table` stays in memory until the last reference (the iterator) is destroyed. The `DeleteEntry` callback on cache eviction simply releases its `shared_ptr`; if no other references exist, the `Table` and its underlying file handle are closed and freed.

### Compaction

A persistent, joinable background thread processes compaction work. `MaybeScheduleCompaction()` signals the thread via a condition variable rather than spawning a new detached thread per compaction. On shutdown, the destructor sets `shutting_down_`, signals the condition variable, and joins the thread — preventing use‑after‑free bugs.

During compaction, Bloom filters are consulted when deciding whether to drop tombstones. If all output‑level files' Bloom filters indicate that a deleted key is absent, the tombstone is dropped early, saving disk space and reducing write amplification.

---

## Performance

> **Preliminary numbers** — actual throughput depends on hardware, workload shape, and configuration.

On a modern NVMe SSD, with 16 concurrent writer threads and 4 KB values:

| Workload | Throughput |
|---|---|
| Unsynced sequential writes | ~500,000 ops/sec |
| Synced writes (group commit) | ~150,000 ops/sec |
| Random point reads (hot cache) | ~400,000 ops/sec |
| Full scan (sequential) | ~1.2 GB/sec |

Group commit provides a **3–5×** throughput improvement over per‑write WAL fsync under concurrent load. The persistent file handle optimization reduces SSTable read latency by eliminating per‑block `open()`/`close()` syscall overhead.

Run the included benchmark to measure performance on your own hardware:

```bash
cd build
./lsm_bench        # Linux/macOS
./lsm_bench.exe    # Windows
```
