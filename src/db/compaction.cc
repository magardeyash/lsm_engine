#include "src/db/compaction.h"
#include "src/db/version_set.h"

namespace lsm {

static const int64_t kMaxGrandParentOverlapBytes = 10 * 1048576; // 10MB

static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
    int64_t sum = 0;
    for (size_t i = 0; i < files.size(); i++) {
        sum += files[i]->file_size;
    }
    return sum;
}

Compaction::Compaction(const Options* options, int level)
    : level_(level),
      max_output_file_size_(options->max_file_size),
      input_version_(nullptr),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0) {
    for (int i = 0; i < lsm::Options::kNumLevels; i++) {
        level_ptrs_[i] = 0;
    }
}

Compaction::~Compaction() {
    if (input_version_ != nullptr) {
        input_version_->Unref();
    }
}

bool Compaction::IsTrivialMove() const {
    const VersionSet* vset = input_version_->vset_;
    // Avoid a move if there is lots of overlapping grandparent data.
    // Otherwise, the move could create a parent file that will require
    // a very expensive merge later on.
    return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
            TotalFileSize(grandparents_) <=
                kMaxGrandParentOverlapBytes);
}

void Compaction::AddInputDeletions(VersionEdit* edit) {
    for (int which = 0; which < 2; which++) {
        for (size_t i = 0; i < inputs_[which].size(); i++) {
            edit->DeleteFile(level_ + which, inputs_[which][i]->number);
        }
    }
}

bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
    // Maybe use binary search to find right entry instead of linear search?
    const Comparator* user_cmp = input_version_->vset_->options_->comparator;
    for (int lvl = level_ + 2; lvl < lsm::Options::kNumLevels; lvl++) {
        const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
        while (level_ptrs_[lvl] < files.size()) {
            FileMetaData* f = files[level_ptrs_[lvl]];
            if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
                // We've advanced far enough
                if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
                    // Key falls in this file's range, so definitely
                    // not base level
                    return false;
                }
                break;
            }
            level_ptrs_[lvl]++;
        }
    }
    return true;
}

bool Compaction::ShouldStopBefore(const Slice& internal_key) {
    const VersionSet* vset = input_version_->vset_;
    // Scan to find earliest grandparent file that contains key.
    const InternalKeyComparator* icmp = &vset->icmp_;
    while (grandparent_index_ < grandparents_.size() &&
           icmp->Compare(internal_key,
                         grandparents_[grandparent_index_]->largest.Encode()) > 0) {
        if (seen_key_) {
            overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
        }
        grandparent_index_++;
    }
    seen_key_ = true;

    if (overlapped_bytes_ > kMaxGrandParentOverlapBytes) {
        // Too much overlap for current output; start new output
        overlapped_bytes_ = 0;
        return true;
    } else {
        return false;
    }
}

void Compaction::ReleaseInputs() {
    if (input_version_ != nullptr) {
        input_version_->Unref();
        input_version_ = nullptr;
    }
}

}  // namespace lsm
