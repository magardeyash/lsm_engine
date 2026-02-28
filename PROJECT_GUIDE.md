# 🔍 LSM Engine — Complete File-by-File Explanation

This is a **Log-Structured Merge Tree (LSM Tree)** storage engine — the same architecture used by **RocksDB, LevelDB, and Cassandra**. It's a key-value store optimized for high write throughput by batching writes in memory and flushing them to disk in sorted files.

---

## 🗂️ Project Structure Overview

```
lsm_engine/
├── include/lsm/       ← Public API (what users of the engine see)
├── src/
│   ├── db/            ← Core engine logic (MemTable, WAL, Versioning, Compaction)
│   ├── table/         ← SSTable read/write (on-disk sorted files)
│   └── util/          ← Low-level primitives (hashing, caching, encoding)
├── tests/             ← Unit tests for every major component
└── CMakeLists.txt     ← Build system
```

---

## 📁 `include/lsm/` — The Public API

These are the **only headers an external user of your engine needs**. Think of them as the contract.

| File | What it does |
|------|-------------|
| `db.h` | **The main interface.** Defines the abstract `DB` class with `Open()`, `Put()`, `Get()`, `Delete()`, `NewIterator()`, and `DestroyDB()`. Users only ever touch this. |
| `options.h` | **Configuration knobs.** `Options` (write buffer size, bloom filter bits, compaction triggers, block size, cache capacity), `ReadOptions` (verify checksums?, fill cache?), `WriteOptions` (fsync WAL before ack?). |
| `slice.h` | **A non-owning string view.** Like `std::string_view` but with a custom compare. Used everywhere instead of copying strings to avoid heap allocations. The core data type for keys and values. |
| `status.h` | **Error handling.** A `Status` object encoding OK/NotFound/Corruption/IOError etc. in a compact `char*` buffer. Every operation returns one. |
| `iterator.h` | **Abstract bidirectional cursor.** `Seek`, `Next`, `Prev`, `key()`, `value()`. Implemented by MemTable, SSTable, merge iterators. |
| `comparator.h` | **Ordering contract.** Abstract class with `Compare()`, `FindShortestSeparator()`, `FindShortSuccessor()`. Used to sort keys. Default is byte-lexicographic. |

---

## 📁 `src/db/` — The Core Engine

This is the **brain** of the LSM tree.

### `db_impl.h` / `db_impl.cc` — **The Main Implementation**
`DBImpl` is the concrete class that implements the abstract `DB` interface.

**Key responsibilities:**
- **`Open()`** → Creates WAL, recovers from crash, initializes MemTable & VersionSet
- **`Put()` / `Delete()`** → Calls `Write()`, which appends to WAL then inserts into MemTable
- **`Get()`** → Checks MemTable first → then immutable MemTable → then each SSTable level via VersionSet (newest-first)
- **`MakeRoomForWrite()`** → When MemTable is full, rotates it to `imm_` (immutable) and signals the background thread to flush it
- **Group Commit (`BuildBatchGroup`)** → Multiple concurrent writers are batched into one WAL write for efficiency
- **Background thread** → Runs `BackgroundCall()` which either flushes `imm_` to L0 SSTable or runs compaction

**Key members:**
- `mem_` — the active, mutable MemTable
- `imm_` — the immutable MemTable being flushed to disk
- `log_` — the WAL writer
- `versions_` — manages all SSTable file metadata
- `table_cache_` — LRU cache of open SSTable file handles

---

### `skiplist.h` — **The MemTable's Data Structure**
A **lock-free probabilistic sorted data structure** (header-only template).

- `Insert(key)` — O(log n) random insertion
- `Contains(key)` — O(log n) lookup
- `Iterator` — sequential scan in sorted order
- Uses `std::atomic` for its forward pointers → readers need no lock, writers use external mutex
- Max height = 12 levels, branching factor = 4

> **Why SkipList over Red-Black Tree?** Concurrent reads without locking, cache-friendlier traversal, no rebalancing.

---

### `memtable.h` / `memtable.cc` — **In-Memory Write Buffer**
Wraps the SkipList to store **internal keys** (user key + sequence number + value type).

- **`Add(seq, type, key, value)`** → encodes as internal key, inserts into skip list
- **`Get(LookupKey, value, status)`** → finds latest version of a key (sequence number descending), handles tombstones
- **`Ref()` / `Unref()`** → reference counting for safe shared access
- **`ApproximateMemoryUsage()`** → triggers flush when it exceeds `write_buffer_size`

**Internal key format:** `[user_key bytes][7 bytes seq num][1 byte type]`
- `type = 0x1` → value present
- `type = 0x0` → deletion tombstone

---

### `wal.h` / `wal.cc` — **Write-Ahead Log (Crash Recovery)**
Physical log written to disk *before* every write.

**Record format:** `[CRC32: 4 bytes][Length: 2 bytes][Type: 1 byte][Data: N bytes]`

- **`WalWriter::AddRecord(slice)`** → appends a record to the log file. Calls `Sync()` if `WriteOptions::sync == true` (fsync to disk)
- **`WalReader::ReadRecord()`** → reads records back sequentially during recovery, validates CRC32, reports corruption
- On restart, `Recover()` replays unprocessed WAL records back into a fresh MemTable

> **Interview point:** WAL guarantees durability. Without it, a crash after MemTable write but before SSTable flush loses data.

---

### `version_edit.h` / `version_edit.cc` — **SSTable Change Log (Delta)**
A `VersionEdit` is a **diff** describing changes to the set of SSTable files.

- **`AddFile(level, file_number, size, smallest, largest)`** → records a new SSTable added
- **`DeleteFile(level, file_number)`** → records an SSTable removed by compaction
- **`EncodeTo(string*)`** / **`DecodeFrom(Slice)`** → serialized with varints and written to the MANIFEST file
- `FileMetaData` — metadata for one SSTable: file number, size, key range (smallest/largest `InternalKey`), reference count, allowed seeks before compaction

---

### `version_set.h` / `version_set.cc` — **The Version Manager (MANIFEST)**
Tracks the complete set of SSTables across all 7 levels at any given point in time.

- **`Version`** — a snapshot of all SSTable files at a moment. Linked list of versions.
  - `Get()` → searches files at each level for a key (L0 searches all files, L1+ uses binary search on key ranges)
  - `AddIterators()` → exposes iterators for all files at all levels (used for range scans)
- **`VersionSet`** — manages the linked list of `Version`s
  - `LogAndApply(edit)` → applies a `VersionEdit`, creates a new `Version`, writes it to the MANIFEST, installs it as current
  - `Recover()` → reads MANIFEST on startup to reconstruct the current file set
  - `PickCompaction()` → selects which level and files to compact next (score-based)
  - `Finalize()` → computes a compaction score for each version (triggers compaction)

> **MANIFEST file** = a WAL of `VersionEdit`s — lets you recover the exact set of live SSTables after a crash.

---

### `compaction.h` / `compaction.cc` — **The Compaction Job Descriptor**
A `Compaction` object describes *one* compaction job picked by `VersionSet::PickCompaction()`.

- `level_` — which level is being compacted
- `inputs_[0]` — files FROM the source level
- `inputs_[1]` — overlapping files FROM the next level
- `grandparents_` — files in level+2 used to limit output file size
- `IsTrivialMove()` — true if input[1] is empty → file just moves down a level, no merge needed
- `IsBaseLevelForKey()` — checks if a key exists in deeper levels (to decide if a tombstone can be dropped)
- `ShouldStopBefore()` — limits output file size to avoid too much overlap with grandparent level

The actual compaction work (merging + writing) happens in `DBImpl::DoCompactionWork()`.

---

### `merger.h` / `merger.cc` — **N-Way Merge Iterator**
Merges N sorted iterators (from MemTable + multiple SSTable levels) into a single sorted stream.

- `NewMergingIterator(comparator, children, n)` → returns a single iterator that yields keys in globally sorted order by always picking the smallest key across all child iterators
- Used during compaction (to merge input files) and during full iteration (`NewIterator`)

---

## 📁 `src/table/` — SSTable Files (On-Disk Sorted Storage)

### `format.h` / `format.cc` — **SSTable Binary Format**
Defines the on-disk layout of every SSTable file.

**SSTable structure:**
```
[Data Block 0]
[Data Block 1]
...
[Data Block N]
[Meta Block: bloom filter]
[Metaindex Block]    ← maps meta block names → BlockHandles
[Index Block]        ← maps last_key_in_block → BlockHandle (for binary search)
[Footer: 48 bytes]   ← BlockHandle to index + metaindex, 8-byte magic
```

- **`BlockHandle`** — an `(offset, size)` pair pointing to any block in the file
- **`Footer`** — the fixed-size tail of every SSTable, read first. Contains handles to the index and metaindex blocks
- **`BlockBuilder`** — builds a single data block. Keys are prefix-compressed (delta-encoded). Every `block_restart_interval` entries, a full key is stored as a restart point for binary search

---

### `sstable_builder.h` / `sstable_builder.cc` — **Writing SSTables**
`TableBuilder` writes a new SSTable file sequentially (keys must be added in sorted order).

- `Add(key, value)` → adds a key-value pair, flushing the current data block when it exceeds `block_size`
- `Flush()` → finalizes the current data block, writes it to disk, adds an entry to the index block
- `Finish()` → writes bloom filter block, metaindex block, index block, then footer
- `Abandon()` → call if you don't want to finish (e.g., on error)
- Uses `BlockBuilder` internally for delta-compressed key encoding

---

### `sstable_reader.h` / `sstable_reader.cc` — **Reading SSTables**
`Table` opens and reads an existing SSTable file.

- `Open(options, filename, size, &table)` → reads the footer, then index block, then bloom filter metadata into memory
- `NewIterator()` → a **Two-Level Iterator**: outer iterator walks the index block to find a data block handle, then opens that data block and scans entries
- `MayContain(user_key)` → queries the bloom filter — avoids reading a data block if the key is definitely absent
- `InternalGet()` → used by `TableCache::Get()` — binary searches index, reads one data block, finds exact key
- `ApproximateOffsetOf()` → used for estimating compaction output sizes

---

### `table_cache.h` / `table_cache.cc` — **LRU Cache for Open SSTable Files**
Avoids repeatedly opening and closing SSTable file handles.

- `NewIterator(file_number, file_size)` → looks up the file in the LRU cache; if absent, opens it and caches the `Table*`
- `Get(file_number, file_size, key, ...)` → cache lookup + `InternalGet()` on the table
- `MayContain(file_number, file_size, key)` → bloom filter check through the cache
- `Evict(file_number)` → called after a file is deleted by compaction to remove it from the cache

---

## 📁 `src/util/` — Low-Level Utilities

| File | What it does |
|------|-------------|
| `coding.h/.cc` | **Binary serialization.** Fixed-width (`EncodeFixed32/64`) and variable-length (`PutVarint32/64`) integer encoders. Little-endian. Used for SSTable binary format and WAL records. |
| `crc32.h/.cc` | **CRC32 checksum.** Used to detect corruption in WAL records and SSTable blocks. |
| `hash.h/.cc` | **MurmurHash** variant used by the Bloom filter to hash keys. |
| `bloom.h/.cc` | **Bloom Filter.** Probabilistic set membership test. `CreateFilter(keys, n)` → builds bit array. `KeyMayMatch(key, filter)` → returns false if definitely absent, true if probably present. Reduces disk I/O on point lookups. |
| `cache.h/.cc` | **LRU Cache.** Thread-safe, shard-locked LRU cache. Abstract `Cache` interface + `NewLRUCache(capacity)`. Used by `TableCache`. Handles are reference-counted. |
| `comparator.cc` | Implements the default `BytewiseComparatorImpl` (lexicographic byte comparison). Also implements `FindShortestSeparator` (shorten SSTable index entries) and `FindShortSuccessor` (used for last entry in index block). |
| `options.cc` | Constructs the default `Options` (sets `comparator` to bytewise). |
| `status.cc` | Implements `Status::ToString()` and copy operations. State stored as `[4-byte len][1-byte code][message]`. |

---

## 📁 `tests/` — Unit Tests

| Test File | What it tests |
|-----------|--------------|
| `test_db.cc` | End-to-end: `Open`, `Put`, `Get`, `Delete`, reopen persistence |
| `test_memtable.cc` | MemTable insert, lookup, tombstones, sequence numbers |
| `test_sstable.cc` | `TableBuilder` write + `Table::Open` read, key lookup |
| `test_bloom.cc` | False positive rate, no false negatives |
| `test_compaction.cc` | L0→L1 compaction, key merging, tombstone dropping |
| `test_concurrency.cc` | Multi-threaded reads and writes, writer group commit |
| `test_crash_recovery.cc` | WAL replay after simulated crash |
| `test_group_commit.cc` | Multiple writers batched into single WAL record |
| `test_bench.cc` | Throughput benchmark for sequential and random writes |

---

## 🔄 The Complete Write Flow (Interview Ready)

```
Put("key", "value")
  │
  ▼
DBImpl::Put() → DBImpl::Write()
  │
  ├─ 1. Acquire mutex
  ├─ 2. BuildBatchGroup() ← coalesce concurrent writers
  ├─ 3. WalWriter::AddRecord() ← write to WAL on disk
  ├─ 4. MemTable::Add() ← insert into skip list
  ├─ 5. MakeRoomForWrite() ← if MemTable full:
  │       ├─ rotate mem_ → imm_
  │       ├─ create new MemTable + new WAL file
  │       └─ signal background thread
  └─ 6. Return OK to all batched writers
```

## 🔄 The Complete Read Flow

```
Get("key")
  │
  ├─ 1. Check mem_ (active MemTable, skip list lookup)
  ├─ 2. Check imm_ (immutable MemTable, if exists)
  ├─ 3. Check current Version (VersionSet)
  │       ├─ L0: search ALL files (overlap possible) → newest first
  │       ├─ L1: binary search by key range → at most 1 file
  │       ├─ L2..L6: same binary search
  │       └─ Each file: TableCache lookup → Bloom filter check → Index binary search → Block read
  └─ 4. Return value or NotFound
```

## 🔄 The Background Compaction Flow

```
BackgroundCall()
  │
  ├─ If imm_ exists:
  │     WriteLevel0Table() → TableBuilder writes sorted SSTable
  │                         → VersionEdit records new L0 file
  │                         → LogAndApply() installs new Version
  │
  └─ Else:
        PickCompaction() → selects files from level N + level N+1
        DoCompactionWork():
          ├─ MergingIterator over all input files
          ├─ For each key (newest sequence wins):
          │     Skip older duplicates
          │     Drop tombstones at bottom level
          │     Write survivors to new SSTable(s)
          └─ LogAndApply() → deletes old files, adds new files as atomic version change
```

---

## 🎯 Key Concepts for Interviews

| Concept | Your Implementation |
|---------|-------------------|
| **Write path** | WAL → MemTable (SkipList) → SSTable (on flush) |
| **Read path** | MemTable → imm → L0 (all) → L1+ (binary search) |
| **Why WAL?** | Crash recovery — replay log to rebuild lost MemTable |
| **Why SkipList?** | Lock-free concurrent reads, O(log n) writes |
| **Why Bloom filters?** | Skip disk I/O for definitely-absent keys |
| **Why compaction?** | Merge duplicate keys, drop tombstones, bound read amplification |
| **Version/MANIFEST** | Atomic metadata changes — consistent view even during compaction |
| **Group commit** | Batch N concurrent `Put()` calls into 1 WAL write = higher throughput |
| **Block cache** | LRU cache of decompressed 4KB data blocks — reduces disk reads |
| **Sequence numbers** | MVCC-lite — each write gets a monotonically increasing seq num; reads see consistent snapshots |
