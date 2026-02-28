#include "src/db/version_edit.h"
#include "src/util/coding.h"

namespace lsm {

enum Tag {
    kComparator           = 1,
    kLogNumber            = 2,
    kNextFileNumber       = 3,
    kLastSequence         = 4,
    kCompactPointer       = 5,
    kDeletedFile          = 6,
    kNewFile              = 7,
    kPrevLogNumber        = 9
};

void VersionEdit::Clear() {
    comparator_.clear();
    log_number_ = 0;
    prev_log_number_ = 0;
    last_sequence_ = 0;
    next_file_number_ = 0;
    has_comparator_ = false;
    has_log_number_ = false;
    has_prev_log_number_ = false;
    has_next_file_number_ = false;
    has_last_sequence_ = false;
    deleted_files_.clear();
    new_files_.clear();
}

void VersionEdit::AddFile(int level, uint64_t file,
                          uint64_t file_size,
                          const InternalKey& smallest,
                          const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
}

void VersionEdit::DeleteFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
}

void VersionEdit::EncodeTo(std::string* dst) const {
    if (has_comparator_) {
        PutVarint32(dst, kComparator);
        PutLengthPrefixedSlice(dst, Slice(comparator_));
    }
    if (has_log_number_) {
        PutVarint32(dst, kLogNumber);
        PutVarint64(dst, log_number_);
    }
    if (has_prev_log_number_) {
        PutVarint32(dst, kPrevLogNumber);
        PutVarint64(dst, prev_log_number_);
    }
    if (has_next_file_number_) {
        PutVarint32(dst, kNextFileNumber);
        PutVarint64(dst, next_file_number_);
    }
    if (has_last_sequence_) {
        PutVarint32(dst, kLastSequence);
        PutVarint64(dst, last_sequence_);
    }

    for (const auto& deleted : deleted_files_) {
        PutVarint32(dst, kDeletedFile);
        PutVarint32(dst, deleted.first);
        PutVarint64(dst, deleted.second);
    }

    for (size_t i = 0; i < new_files_.size(); i++) {
        const FileMetaData& f = new_files_[i].second;
        PutVarint32(dst, kNewFile);
        PutVarint32(dst, new_files_[i].first);
        PutVarint64(dst, f.number);
        PutVarint64(dst, f.file_size);
        PutLengthPrefixedSlice(dst, f.smallest.Encode());
        PutLengthPrefixedSlice(dst, f.largest.Encode());
    }
}

static bool GetInternalKey(Slice* input, InternalKey* dst) {
    Slice str;
    if (GetLengthPrefixedSlice(input, &str)) {
        dst->SetFrom(str);
        return true;
    } else {
        return false;
    }
}

Status VersionEdit::DecodeFrom(const Slice& src) {
    Clear();
    Slice input = src;
    int msg;
    uint32_t level;
    uint64_t number;
    FileMetaData f;
    Slice str;

    while (!input.empty() && GetVarint32(&input, reinterpret_cast<uint32_t*>(&msg))) {
        switch (msg) {
            case kComparator:
                if (GetLengthPrefixedSlice(&input, &str)) {
                    comparator_ = str.ToString();
                    has_comparator_ = true;
                } else {
                    return Status::Corruption("VersionEdit: comparator name");
                }
                break;

            case kLogNumber:
                if (GetVarint64(&input, &log_number_)) {
                    has_log_number_ = true;
                } else {
                    return Status::Corruption("VersionEdit: log number");
                }
                break;

            case kPrevLogNumber:
                if (GetVarint64(&input, &prev_log_number_)) {
                    has_prev_log_number_ = true;
                } else {
                    return Status::Corruption("VersionEdit: previous log number");
                }
                break;

            case kNextFileNumber:
                if (GetVarint64(&input, &next_file_number_)) {
                    has_next_file_number_ = true;
                } else {
                    return Status::Corruption("VersionEdit: next file number");
                }
                break;

            case kLastSequence:
                if (GetVarint64(&input, &last_sequence_)) {
                    has_last_sequence_ = true;
                } else {
                    return Status::Corruption("VersionEdit: last sequence");
                }
                break;

            case kCompactPointer:
                if (GetVarint32(&input, &level) &&
                    GetInternalKey(&input, &f.smallest)) {
                } else {
                    return Status::Corruption("VersionEdit: compact pointer");
                }
                break;

            case kDeletedFile:
                if (GetVarint32(&input, &level) &&
                    GetVarint64(&input, &number)) {
                    deleted_files_.insert(std::make_pair(level, number));
                } else {
                    return Status::Corruption("VersionEdit: deleted file");
                }
                break;

            case kNewFile:
                if (GetVarint32(&input, &level) &&
                    GetVarint64(&input, &f.number) &&
                    GetVarint64(&input, &f.file_size) &&
                    GetInternalKey(&input, &f.smallest) &&
                    GetInternalKey(&input, &f.largest)) {
                    new_files_.push_back(std::make_pair(level, f));
                } else {
                    return Status::Corruption("VersionEdit: new file");
                }
                break;

            default:
                return Status::Corruption("VersionEdit: unknown tag");
        }
    }
    
    if (!input.empty()) {
        return Status::Corruption("VersionEdit: extra data after end of version edit");
    }
    return Status::OK();
}

}
