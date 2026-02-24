#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include <thread>

#include "lsm/db.h"
#include "lsm/options.h"
#include "src/db/memtable.h"
#include "src/db/version_set.h"
#include "src/db/wal.h"
#include "src/table/sstable_builder.h"

namespace lsm {

class DBImpl : public DB {
public:
    DBImpl(const Options& options, const std::string& dbname);
    ~DBImpl() override;

    Status Put(const WriteOptions& options, const Slice& key, const Slice& value) override;
    Status Delete(const WriteOptions& options, const Slice& key) override;
    Status Get(const ReadOptions& options, const Slice& key, std::string* value) override;
    Iterator* NewIterator(const ReadOptions& options) override;

    // Recover the database
    Status Recover();

private:
    friend class DB;
    struct Writer;

    Status MakeRoomForWrite(bool force = false);
    Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base);
    Status BackgroundCompaction();
    void BackgroundCall();
    void MaybeScheduleCompaction();

    Status DoCompactionWork(Compaction* c);
    void CleanupCompaction(Compaction* c);

    // Constant after construction
    const Options options_;
    const std::string dbname_;
    InternalKeyComparator internal_comparator_;

    // Options copy with internal comparator for SSTable I/O
    Options internal_options_;

    // table_cache_ provides its own synchronization
    TableCache* table_cache_;

    // Lock over the mutable state of the DB.
    std::mutex mutex_;
    std::condition_variable bg_cv_;
    std::atomic<bool> shutting_down_;
    bool bg_compaction_scheduled_;

    MemTable* mem_;
    MemTable* imm_;  // Memtable being compacted
    std::atomic<bool> has_imm_;

    std::unique_ptr<WalWriter> log_;
    uint64_t logfile_number_;

    VersionSet* versions_;

    std::deque<Writer*> writers_;
    Status bg_error_;

    // Information for a compaction in progress.
    struct CompactionState {
        Compaction* const compaction;
        
        // Output state
        uint64_t outfile_number;
        uint64_t outfile_size;
        std::unique_ptr<TableBuilder> builder;
        std::ofstream* outfile;

        InternalKey smallest;
        InternalKey largest;

        explicit CompactionState(Compaction* c)
            : compaction(c),
              outfile_number(0),
              outfile_size(0),
              builder(nullptr),
              outfile(nullptr) {}
    };
};

}  // namespace lsm
