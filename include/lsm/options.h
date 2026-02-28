#pragma once

#include <cstddef>
#include <string>

namespace lsm {

class Comparator;

struct Options {
    Options();

    // Default: lexicographic byte-wise comparator. Must use the same comparator
    // on every open of the same DB.
    const Comparator* comparator;

    bool create_if_missing = true;

    bool error_if_exists = false;

    static constexpr int kNumLevels = 7;
    static constexpr int kL0_CompactionTrigger = 4;
    static constexpr int kL0_SlowdownWritesTrigger = 8;
    static constexpr int kL0_StopWritesTrigger = 12;
    size_t max_file_size = 2 * 1024 * 1024;

    // If true, stop early on data corruption (may make more entries unreadable).
    bool paranoid_checks = false;

    // Data buffered in memory before flushing to disk. Larger = better bulk
    // throughput but more memory usage and longer recovery on restart.
    size_t write_buffer_size = 4 * 1024 * 1024;

    // Max open file descriptors (budget ~1 per 2MB of working set).
    int max_open_files = 1000;

    // Approximate uncompressed size of user data per block.
    size_t block_size = 4 * 1024;

    // Keys between delta-encoding restart points. Leave at default unless tuning.
    int block_restart_interval = 16;

    enum CompressionType {
        kNoCompression = 0x0,
        kZstdCompression = 0x1
    };

    CompressionType compression = kNoCompression;

    int bloom_bits_per_key = 10;

    // Capacity of the block cache in bytes. If 0, no cache is used.
    size_t block_cache_capacity = 8 * 1024 * 1024;
};

struct ReadOptions {
    bool verify_checksums = false;

    bool fill_cache = true;
};

struct WriteOptions {
    // If true, fsync() the WAL before acknowledging the write.
    // Slower but durable across process crashes. If false, writes
    // may be lost on machine crash (not process crash).
    bool sync = false;
};

}
