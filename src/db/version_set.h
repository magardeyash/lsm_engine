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

    // Returns a string summarizing internal state
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

    // Next file to compact based on seek stats.
    FileMetaData* file_to_compact_;
    int file_to_compact_level_;

    // Next level to compact based on size of files in level.
    double compaction_score_;
    int compaction_level_;
};

class VersionSet {
public:
    VersionSet(const std::string& dbname, const Options* options, TableCache* table_cache);
    ~VersionSet();

    VersionSet(const VersionSet&) = delete;
    VersionSet& operator=(const VersionSet&) = delete;

    // Apply *edit to the current version to form a new descriptor that
    // is both saved to persistent state and installed as the new
    // current version.
    Status LogAndApply(VersionEdit* edit, std::mutex* mu);

    // Recover the last saved descriptor from persistent storage.
    Status Recover();

    // Return the current version.
    Version* current() const { return current_; }

    // Return the current manifest file number
    uint64_t ManifestFileNumber() const { return manifest_file_number_; }

    // Allocate and return a new file number
    uint64_t NewFileNumber() { return next_file_number_++; }

    // Arrange to reuse "file_number" unless a newer file number has
    // already been allocated.
    // REQUIRES: "file_number" was returned by a call to NewFileNumber().
    void ReuseFileNumber(uint64_t file_number) {
        if (next_file_number_ == file_number + 1) {
            next_file_number_ = file_number;
        }
    }

    int NumLevelFiles(int level) const;

    // Return the combined file size of all files at the specified level.
    int64_t NumLevelBytes(int level) const;

    // Return the last sequence number.
    uint64_t LastSequence() const { return last_sequence_; }

    // Set the last sequence number to s.
    void SetLastSequence(uint64_t s) {
        assert(s >= last_sequence_);
        last_sequence_ = s;
    }

    // Mark the specified file number as used.
    void MarkFileNumberUsed(uint64_t number);

    // Return the current log file number.
    uint64_t LogNumber() const { return log_number_; }

    // Return the log file number for the log file that is currently
    // being compacted, or zero if there is no such log file.
    uint64_t PrevLogNumber() const { return prev_log_number_; }

    // Pick level and inputs for a new compaction.
    // Returns nullptr if there is no compaction to be done.
    Compaction* PickCompaction();

    // Add all files listed in any live version to *live.
    void AddLiveFiles(std::set<uint64_t>* live);

    // Return the approximate offset in the database of the data for
    // "key" as of version "v".
    uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

private:
    friend class Version;
    friend class Compaction;
    friend class DBImpl;
    friend class VersionSetBuilder;

    void AppendVersion(Version* v);
    void Finalize(Version* v);

    // Setup the compaction pointers.
    void SetupOtherInputs(Compaction* c);

    // Save current contents to *log
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

    // Per-level key at which the next compaction at that level should start.
    // Either an empty string, or a valid InternalKey.
    std::string compact_pointer_[lsm::Options::kNumLevels];
};

}  // namespace lsm
