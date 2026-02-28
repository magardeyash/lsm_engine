#pragma once

#include <vector>
#include <map>
#include <set>
#include <string>
#include "lsm/options.h"
#include "lsm/status.h"
#include "src/db/version_edit.h"
#include "src/db/memtable.h"

namespace lsm {

class Version;
class VersionSet;
class Compaction;
class TableCache;

class Compaction {
public:
    ~Compaction();

    int level() const { return level_; }

    VersionEdit* edit() { return &edit_; }

    int num_input_files(int which) const { return inputs_[which].size(); }

    FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

    uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

    bool IsTrivialMove() const;

    void AddInputDeletions(VersionEdit* edit);

    bool IsBaseLevelForKey(const Slice& user_key);

    bool ShouldStopBefore(const Slice& internal_key);

    void ReleaseInputs();

private:
    friend class Version;
    friend class VersionSet;
    friend class DBImpl;

    Compaction(const Options* options, int level);

    int level_;
    uint64_t max_output_file_size_;
    Version* input_version_;
    VersionEdit edit_;

    std::vector<FileMetaData*> inputs_[2];

    std::vector<FileMetaData*> grandparents_;
    size_t grandparent_index_;
    bool seen_key_;
    int64_t overlapped_bytes_;
    
    size_t level_ptrs_[lsm::Options::kNumLevels];
};

}
