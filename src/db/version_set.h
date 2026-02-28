#pragma once

#include <vector>
#include <map>
#include <set>
#include <string>
#include <mutex>
#include "lsm/options.h"
#include "lsm/status.h"
#include "lsm/iterator.h"
#include "src/db/version_edit.h"
#include "src/db/compaction.h"
#include "src/db/wal.h"
#include "src/db/memtable.h"

namespace lsm {

class VersionSet;

class Version {
public:
    void Ref();
    void Unref();

    void Get(const ReadOptions&, const Slice& key, std::string* val, Status* status);

    void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

    std::string DebugString() const;

private:
    friend class VersionSet;
    friend class Compaction;
    friend class VersionSetBuilder;
    friend class DBImpl;

    class LevelFileNumIterator;

    explicit Version(VersionSet* vset);
    ~Version();

    Version(const Version&) = delete;
    Version& operator=(const Version&) = delete;

    Iterator* NewConcatenatingIterator(const ReadOptions&, int level) const;

    VersionSet* vset_;
    Version* next_;
    Version* prev_;
    int refs_;

    std::vector<FileMetaData*> files_[lsm::Options::kNumLevels];

    FileMetaData* file_to_compact_;
    int file_to_compact_level_;

    double compaction_score_;
    int compaction_level_;
};

class VersionSet {
public:
    VersionSet(const std::string& dbname, const Options* options, TableCache* table_cache);
    ~VersionSet();

    VersionSet(const VersionSet&) = delete;
    VersionSet& operator=(const VersionSet&) = delete;

    // Applies *edit to current version, persists it, and installs it as current.
    Status LogAndApply(VersionEdit* edit, std::mutex* mu);

    Status Recover();

    Version* current() const { return current_; }

    uint64_t ManifestFileNumber() const { return manifest_file_number_; }

    uint64_t NewFileNumber() { return next_file_number_++; }

    // Reuses file_number if no newer number has been allocated since.
    void ReuseFileNumber(uint64_t file_number) {
        if (next_file_number_ == file_number + 1) {
            next_file_number_ = file_number;
        }
    }

    int NumLevelFiles(int level) const;

    int64_t NumLevelBytes(int level) const;

    uint64_t LastSequence() const { return last_sequence_; }

    void SetLastSequence(uint64_t s) {
        assert(s >= last_sequence_);
        last_sequence_ = s;
    }

    void MarkFileNumberUsed(uint64_t number);

    uint64_t LogNumber() const { return log_number_; }

    uint64_t PrevLogNumber() const { return prev_log_number_; }

    Compaction* PickCompaction();

    void AddLiveFiles(std::set<uint64_t>* live);

    uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

private:
    friend class Version;
    friend class Compaction;
    friend class DBImpl;
    friend class VersionSetBuilder;

    void AppendVersion(Version* v);
    void Finalize(Version* v);

    void SetupOtherInputs(Compaction* c);

    Status WriteSnapshot(WalWriter* log);

    const std::string dbname_;
    const Options* const options_;
    TableCache* const table_cache_;
    uint64_t next_file_number_;
    uint64_t manifest_file_number_;
    uint64_t last_sequence_;
    uint64_t log_number_;
    uint64_t prev_log_number_;

    InternalKeyComparator icmp_;

    std::unique_ptr<WalWriter> descriptor_log_;
    std::string descriptor_filename_;

    Version dummy_versions_;
    Version* current_;

    std::string compact_pointer_[lsm::Options::kNumLevels];
};

}
