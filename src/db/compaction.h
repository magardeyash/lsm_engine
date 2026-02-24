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
class TableCache;       // Forward declaration, we will need it to read tables.

// Represents a compaction of Level N to Level N+1
class Compaction {
public:
    ~Compaction();

    // Return the level that is being compacted.  Inputs from "level"
    // and "level+1" will be merged to produce a set of "level+1" files.
    int level() const { return level_; }

    // Return the object that holds the edits to the descriptor done
    // by this compaction.
    VersionEdit* edit() { return &edit_; }

    // "which" must be either 0 or 1
    int num_input_files(int which) const { return inputs_[which].size(); }

    // Return the ith input file at "level()+which" ("which" must be 0 or 1).
    FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

    // Maximum size of files to build during this compaction.
    uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

    // Is this a trivial compaction that can be implemented by just
    // moving a single input file to the next level (no merging or splitting)
    bool IsTrivialMove() const;

    // Add all inputs to this compaction as delete operations to *edit.
    void AddInputDeletions(VersionEdit* edit);

    // Returns true if the information we have available guarantees that
    // the compaction is producing data in "level+1" for which no data exists
    // in levels greater than "level+1".
    bool IsBaseLevelForKey(const Slice& user_key);

    // Returns true iff we should stop building the current output
    // before processing "internal_key".
    bool ShouldStopBefore(const Slice& internal_key);

    // Release the input version for the compaction, once the compaction
    // is successful.
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

    // Each compaction reads inputs from "level_" and "level_+1"
    std::vector<FileMetaData*> inputs_[2];      // The two sets of inputs

    // State used to check for number of overlapping grandparent files
    // (parent == level_ + 1, grandparent == level_ + 2)
    std::vector<FileMetaData*> grandparents_;
    size_t grandparent_index_;  // Index in grandparent_starts_
    bool seen_key_;             // Some output key has been seen
    int64_t overlapped_bytes_;  // Bytes of overlap between current output
                                // and grandparent files
    
    // State for implementing IsBaseLevelForKey
    size_t level_ptrs_[lsm::Options::kNumLevels];
};

}  // namespace lsm
