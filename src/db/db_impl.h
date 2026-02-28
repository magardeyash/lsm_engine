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

    Status Recover();

private:
    friend class DB;
    struct Writer;

    Status Write(Writer* my_writer);
    Writer* BuildBatchGroup(Writer** last_writer);

    Status MakeRoomForWrite(bool force = false);
    Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base);
    Status BackgroundCompaction();
    void BackgroundCall();
    void BackgroundThreadMain();
    void MaybeScheduleCompaction();

    Status DoCompactionWork(Compaction* c);
    void CleanupCompaction(Compaction* c);

    const Options options_;
    const std::string dbname_;
    InternalKeyComparator internal_comparator_;

    Options internal_options_;

    // table_cache_ provides its own synchronization
    TableCache* table_cache_;

    std::mutex mutex_;
    std::condition_variable bg_cv_;
    std::atomic<bool> shutting_down_;
    bool bg_compaction_scheduled_;

    std::thread bg_thread_;
    std::condition_variable bg_work_cv_;

    MemTable* mem_;
    MemTable* imm_;
    std::atomic<bool> has_imm_;

    std::unique_ptr<WalWriter> log_;
    uint64_t logfile_number_;

    VersionSet* versions_;

    std::deque<Writer*> writers_;
    Status bg_error_;

    // Information for a compaction in progress.
    struct CompactionState {
        Compaction* const compaction;
        
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

}
