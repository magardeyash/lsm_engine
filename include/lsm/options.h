#pragma once

#include <cstddef>
#include <string>

namespace lsm {

class Comparator;

// Options to control the behavior of a database (passed to DB::Open)
struct Options {
    // Create an Options object with default values
    Options();

    // The comparator used to define the order of keys in the table.
    // Default: a comparator that uses lexicographic byte-wise ordering
    //
    // REQUIRES: The client must ensure that the comparator supplied
    // here has the same name and orders keys *exactly* the same as the
    // comparator provided to previous open calls on the same DB.
    const Comparator* comparator;

    // If true, the database will be created if it is missing.
    bool create_if_missing = true;

    // If true, an error is raised if the database already exists.
    bool error_if_exists = false;

    // Optional bounds
    static constexpr int kNumLevels = 7;
    static constexpr int kL0_CompactionTrigger = 4;
    static constexpr int kL0_SlowdownWritesTrigger = 8;
    static constexpr int kL0_StopWritesTrigger = 12;
    size_t max_file_size = 2 * 1024 * 1024;

    // If true, the implementation will do aggressive checking of the
    // data it is processing and will stop early if it detects any
    // errors.  This may have unforeseen ramifications: for example, a
    // corruption of one DB entry may cause a large number of entries to
    // become unreadable or for the entire DB to become unopenable.
    bool paranoid_checks = false;

    // Amount of data to build up in memory (backed by an unsorted log
    // on disk) before converting to a sorted on-disk file.
    //
    // Larger values increase performance, especially during bulk loads.
    // Up to two write buffers may be held in memory at the same time,
    // so you may wish to adjust this parameter to control memory usage.
    // Also, a larger write buffer will result in a longer recovery time
    // the next time the database is opened.
    size_t write_buffer_size = 4 * 1024 * 1024;

    // Number of open files that can be used by the DB.  You may need to
    // increase this if your database has a large working set (budget
    // one open file per 2MB of working set).
    int max_open_files = 1000;

    // Approximate size of user data packed per block.  Note that the
    // block size specified here corresponds to uncompressed data.  The
    // actual size of the unit read from disk may be smaller if
    // compression is enabled.  This parameter can be changed dynamically.
    size_t block_size = 4 * 1024;

    // Number of keys between restart points for delta encoding of keys.
    // This parameter can be changed dynamically.  Most clients should
    // leave this parameter alone.
    int block_restart_interval = 16;

    // LevelDB-style compression types
    enum CompressionType {
        kNoCompression = 0x0,
        kZstdCompression = 0x1
    };

    // Compress blocks using the specified compression algorithm.  This
    // parameter can be changed dynamically.
    //
    // Default: kNoCompression
    // If zstd is built in, you can set this to kZstdCompression.
    CompressionType compression = kNoCompression;

    // If non-zero, use a bloom filter with approximately this many bits
    // per key in each sstable.
    int bloom_bits_per_key = 10;

    // Capacity of the block cache in bytes. If 0, no cache is used.
    size_t block_cache_capacity = 8 * 1024 * 1024;
};

// Options that control read operations
struct ReadOptions {
    // If true, all data read from underlying storage will be
    // verified against corresponding checksums.
    bool verify_checksums = false;

    // Should the data read for this iteration be cached in memory?
    // Callers may wish to set this field to false for bulk scans.
    bool fill_cache = true;
};

// Options that control write operations
struct WriteOptions {
    // If true, the write will be flushed from the operating system
    // buffer cache (by calling fsync() on the WAL file)
    // before the write is considered complete.  If this flag is true,
    // writes will be slower.
    //
    // If this flag is false, and the machine crashes, some recent
    // writes may be lost.  Note that if it is just the process that
    // crashes (i.e., the machine does not reboot), no writes will be
    // lost even if sync==false.
    //
    // In other words, a DB write with sync==false has similar
    // crash semantics as the "write()" system call.  A DB write
    // with sync==true has similar crash semantics to a "write()"
    // system call followed by "fsync()".
    bool sync = false;
};

}  // namespace lsm
