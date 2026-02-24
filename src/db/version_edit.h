#pragma once

#include <string>
#include <vector>
#include <set>
#include "lsm/slice.h"
#include "lsm/status.h"
#include "src/db/memtable.h" // For InternalKey

namespace lsm {

struct FileMetaData {
    int refs;
    int allowed_seeks; // Seeks allowed until compaction
    uint64_t number;
    uint64_t file_size;         // File size in bytes
    InternalKey smallest;       // Smallest internal key served by table
    InternalKey largest;        // Largest internal key served by table

    FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) {}
};

class VersionEdit {
public:
    VersionEdit() { Clear(); }
    ~VersionEdit() = default;

    void Clear();

    void SetComparatorName(const Slice& name) {
        has_comparator_ = true;
        comparator_ = name.ToString();
    }
    void SetLogNumber(uint64_t num) {
        has_log_number_ = true;
        log_number_ = num;
    }
    void SetPrevLogNumber(uint64_t num) {
        has_prev_log_number_ = true;
        prev_log_number_ = num;
    }
    void SetNextFile(uint64_t num) {
        has_next_file_number_ = true;
        next_file_number_ = num;
    }
    void SetLastSequence(uint64_t seq) {
        has_last_sequence_ = true;
        last_sequence_ = seq;
    }

    void SetCompactPointer(int level, const InternalKey& key) {
        // Ignored in this simplified class but provided for compatibility
        // with the VersionSet API.
    }

    // Add the specified file at the specified number.
    // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
    // REQUIRES: "smallest" and "largest" are smallest and largest keys in file
    void AddFile(int level, uint64_t file,
                 uint64_t file_size,
                 const InternalKey& smallest,
                 const InternalKey& largest);

    // Delete the specified "file" from the specified "level".
    void DeleteFile(int level, uint64_t file);

    void EncodeTo(std::string* dst) const;
    Status DecodeFrom(const Slice& src);

    const std::string& DebugString() const { return comparator_; } // Simplified

private:
    friend class VersionSet;
    friend class VersionSetBuilder;

    typedef std::set< std::pair<int, uint64_t> > DeletedFileSet;

    std::string comparator_;
    uint64_t log_number_;
    uint64_t prev_log_number_;
    uint64_t next_file_number_;
    uint64_t last_sequence_;
    bool has_comparator_;
    bool has_log_number_;
    bool has_prev_log_number_;
    bool has_next_file_number_;
    bool has_last_sequence_;

    std::vector< std::pair<int, FileMetaData> > new_files_;
    DeletedFileSet deleted_files_;
};

}  // namespace lsm
