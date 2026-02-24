#include "src/db/version_set.h"
#include <algorithm>
#include <cassert>
#include "src/util/coding.h"
#include "src/db/merger.h"
#include "src/table/sstable_reader.h"
#include "src/table/table_cache.h"

namespace lsm {

static const int kTargetFileSize = 2 * 1048576; // 2MB
static const int64_t kMaxGrandParentOverlapBytes = 10 * kTargetFileSize;
static const int64_t kExpandedCompactionByteSizeLimit = 25 * kTargetFileSize;

static double MaxBytesForLevel(int level) {
    // Note: the result for level zero is not really used since we set
    // the level-0 compaction threshold based on number of files.
    double result = 10. * 1048576.0;  // 10 MB
    while (level > 1) {
        result *= 10;
        level--;
    }
    return result;
}

static uint64_t MaxFileSizeForLevel(int level) {
    return kTargetFileSize;  // We could vary this by level if desired
}

static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
    int64_t sum = 0;
    for (size_t i = 0; i < files.size(); i++) {
        sum += files[i]->file_size;
    }
    return sum;
}

int VersionSet::NumLevelFiles(int level) const {
    assert(level >= 0 && level < lsm::Options::kNumLevels);
    return current_->files_[level].size();
}

int64_t VersionSet::NumLevelBytes(int level) const {
    assert(level >= 0 && level < lsm::Options::kNumLevels);
    return TotalFileSize(current_->files_[level]);
}

namespace {
    std::string DescriptorFileName(const std::string& dbname, uint64_t number) {
        char buf[100];
        snprintf(buf, sizeof(buf), "%s/MANIFEST-%06llu", dbname.c_str(),
                 static_cast<unsigned long long>(number));
        return std::string(buf);
    }
}

class Version::LevelFileNumIterator : public Iterator {
public:
    LevelFileNumIterator(const InternalKeyComparator& icmp,
                         const std::vector<FileMetaData*>* flist)
        : icmp_(icmp),
          flist_(flist),
          index_(flist->size()) {        // Marks as invalid
    }
    bool Valid() const override {
        return index_ < flist_->size();
    }
    void Seek(const Slice& target) override {
        // Binary search. flist is sorted by largest key.
        size_t left = 0;
        size_t right = flist_->size();
        while (left < right) {
            size_t mid = left + (right - left) / 2;
            if (icmp_.Compare((*flist_)[mid]->largest.Encode(), target) < 0) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        index_ = left;
    }
    void SeekToFirst() override { index_ = 0; }
    void SeekToLast() override {
        index_ = flist_->empty() ? 0 : flist_->size() - 1;
    }
    void Next() override {
        assert(Valid());
        index_++;
    }
    void Prev() override {
        assert(Valid());
        if (index_ == 0) {
            index_ = flist_->size();  // Marks as invalid
        } else {
            index_--;
        }
    }
    Slice key() const override {
        assert(Valid());
        return (*flist_)[index_]->largest.Encode();
    }
    Slice value() const override {
        assert(Valid());
        auto f = (*flist_)[index_];
        char buf[16];
        EncodeFixed64(buf, f->number);
        EncodeFixed64(buf + 8, f->file_size);
        value_buf_.assign(buf, sizeof(buf));
        return Slice(value_buf_);
    }
    Status status() const override { return Status::OK(); }

private:
    const InternalKeyComparator icmp_;
    const std::vector<FileMetaData*>* const flist_;
    size_t index_;
    mutable std::string value_buf_;
};

static Iterator* GetFileIterator(void* arg, const ReadOptions& options, const Slice& file_value) {
    TableCache* cache = reinterpret_cast<TableCache*>(arg);
    if (file_value.size() != 16) {
        return NewErrorIterator(Status::Corruption("FileReader invoked with unexpected value"));
    } else {
        uint64_t file_num = DecodeFixed64(file_value.data());
        uint64_t file_size = DecodeFixed64(file_value.data() + 8);
        return cache->NewIterator(options, file_num, file_size);
    }
}

Iterator* Version::NewConcatenatingIterator(const ReadOptions& options, int level) const {
    return NewTwoLevelIterator(
               new LevelFileNumIterator(vset_->icmp_, &files_[level]),
               &GetFileIterator, vset_->table_cache_, options);
}

void Version::AddIterators(const ReadOptions& options, std::vector<Iterator*>* iters) {
    // Merge all level zero files together since they may overlap
    for (size_t i = 0; i < files_[0].size(); i++) {
        iters->push_back(
            vset_->table_cache_->NewIterator(options, files_[0][i]->number, files_[0][i]->file_size));
    }

    // For levels > 0, we can use a concatenating iterator that sequentially
    // walks through the non-overlapping files in the level, opening them
    // lazily.
    for (int level = 1; level < lsm::Options::kNumLevels; level++) {
        if (!files_[level].empty()) {
            iters->push_back(NewConcatenatingIterator(options, level));
        }
    }
}

// Callback from TableCache::Get()
namespace {
enum SaverState {
    kNotFound,
    kFound,
    kDeleted,
    kCorrupt,
};
struct Saver {
    SaverState state;
    const Comparator* ucmp;
    Slice user_key;
    std::string* value;
};
}

static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
    Saver* s = reinterpret_cast<Saver*>(arg);
    ParsedInternalKey parsed_key;
    if (!ParseInternalKey(ikey, &parsed_key)) {
        s->state = kCorrupt;
    } else {
        if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
            s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
            if (s->state == kFound) {
                s->value->assign(v.data(), v.size());
            }
        }
    }
}

static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
    return a->number > b->number;
}

static int FindFile(const InternalKeyComparator& icmp,
                    const std::vector<FileMetaData*>& files,
                    const Slice& key) {
    uint32_t left = 0;
    uint32_t right = files.size();
    while (left < right) {
        uint32_t mid = (left + right) / 2;
        const FileMetaData* f = files[mid];
        if (icmp.Compare(f->largest.Encode(), key) < 0) {
            // Key at "mid.largest" is < "target".  Therefore all
            // files at or before "mid" are uninteresting.
            left = mid + 1;
        } else {
            // Key at "mid.largest" is >= "target".  Therefore all files
            // after "mid" are uninteresting.
            right = mid;
        }
    }
    return right;
}

static bool AfterFile(const Comparator* ucmp,
                      const Slice* user_key, const FileMetaData* f) {
    // null user_key occurs before all keys and is therefore never after *f
    return (user_key != nullptr &&
            ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

static bool BeforeFile(const Comparator* ucmp,
                       const Slice* user_key, const FileMetaData* f) {
    // null user_key occurs after all keys and is therefore never before *f
    return (user_key != nullptr &&
            ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key) {
    const Comparator* ucmp = icmp.user_comparator();
    if (!disjoint_sorted_files) {
        // Need to check against all files
        for (size_t i = 0; i < files.size(); i++) {
            const FileMetaData* f = files[i];
            if (AfterFile(ucmp, smallest_user_key, f) ||
                BeforeFile(ucmp, largest_user_key, f)) {
                // No overlap
            } else {
                return true;  // Overlap
            }
        }
        return false;
    }

    // Binary search over file list
    uint32_t index = 0;
    if (smallest_user_key != nullptr) {
        // Find the earliest possible internal key for smallest_user_key
        InternalKey small(*smallest_user_key, kMaxSequenceNumber, kTypeValue);
        index = FindFile(icmp, files, small.Encode());
    }

    if (index >= files.size()) {
        // beginning of range is after all files, so no overlap.
        return false;
    }

    return !BeforeFile(ucmp, largest_user_key, files[index]);
}


void Version::Get(const ReadOptions& options,
                  const Slice& k,
                  std::string* value,
                  Status* status) {
    Slice ikey = k;
    Slice user_key = InternalKey::ExtractUserKey(ikey);
    const Comparator* ucmp = vset_->icmp_.user_comparator();
    Status s;

    Saver saver;
    saver.state = kNotFound;
    saver.ucmp = ucmp;
    saver.user_key = user_key;
    saver.value = value;

    // We can search level-by-level since entries never hop across
    // levels.  Therefore we are guaranteed that if we find data
    // in an earlier level, it will be newer than data in a
    // later level.
    std::vector<FileMetaData*> tmp;
    FileMetaData* tmp2;

    for (int level = 0; level < lsm::Options::kNumLevels; level++) {
        size_t num_files = files_[level].size();
        if (num_files == 0) continue;

        FileMetaData* const* files = &files_[level][0];
        if (level == 0) {
            // Level-0 files may overlap each other.  Find all files that
            // overlap user_key and process them in order from newest to oldest.
            tmp.reserve(num_files);
            for (uint32_t i = 0; i < num_files; i++) {
                FileMetaData* f = files[i];
                if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
                    ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
                    tmp.push_back(f);
                }
            }
            if (tmp.empty()) continue;

            std::sort(tmp.begin(), tmp.end(), NewestFirst);
            files = &tmp[0];
            num_files = tmp.size();
        } else {
            // Binary search to find earliest index whose largest key >= ikey.
            uint32_t index = FindFile(vset_->icmp_, files_[level], ikey);
            if (index >= num_files) {
                files = nullptr;
                num_files = 0;
            } else {
                tmp2 = files[index];
                if (ucmp->Compare(user_key, tmp2->smallest.user_key()) < 0) {
                    // All of "tmp2" is past any data for user_key
                    files = nullptr;
                    num_files = 0;
                } else {
                    files = &tmp2;
                    num_files = 1;
                }
            }
        }

        for (uint32_t i = 0; i < num_files; ++i) {
            if (saver.state == kNotFound) {
                FileMetaData* f = files[i];
                s = vset_->table_cache_->Get(options, f->number, f->file_size,
                                             ikey, &saver, SaveValue);
                if (!s.ok()) {
                    *status = s;
                    return;
                }
                switch (saver.state) {
                    case kNotFound:
                        break;      // Keep searching in other files
                    case kFound:
                        *status = s;
                        return;
                    case kDeleted:
                        s = Status::NotFound(Slice());  // Use empty error message for speed
                        *status = s;
                        return;
                    case kCorrupt:
                        s = Status::Corruption("corrupted key for ", user_key);
                        *status = s;
                        return;
                }
            }
        }
    }

    *status = Status::NotFound(Slice());
}

Version::Version(VersionSet* vset)
    : vset_(vset),
      next_(this),
      prev_(this),
      refs_(0),
      file_to_compact_(nullptr),
      file_to_compact_level_(-1),
      compaction_score_(-1),
      compaction_level_(-1) {
}

Version::~Version() {
    assert(refs_ == 0);
    // Remove from linked list
    prev_->next_ = next_;
    next_->prev_ = prev_;

    // Drop references to files
    for (int level = 0; level < lsm::Options::kNumLevels; level++) {
        for (size_t i = 0; i < files_[level].size(); i++) {
            FileMetaData* f = files_[level][i];
            f->refs--;
            if (f->refs <= 0) {
                delete f;
            }
        }
    }
}

void Version::Ref() {
    refs_++;
}

void Version::Unref() {
    assert(this != &vset_->dummy_versions_);
    assert(refs_ >= 1);
    refs_--;
    if (refs_ == 0) {
        delete this;
    }
}

// A helper class so we can mutate a VersionSet without modifying it directly
// until we are sure it's correct.
class VersionSetBuilder {
private:
    struct BySmallestKey {
        const InternalKeyComparator* internal_comparator;

        bool operator()(FileMetaData* f1, FileMetaData* f2) const {
            int r = internal_comparator->Compare(f1->smallest.Encode(),
                                                 f2->smallest.Encode());
            if (r != 0) {
                return (r < 0);
            } else {
                // Break ties by file number
                return (f1->number < f2->number);
            }
        }
    };

    typedef std::set<FileMetaData*, BySmallestKey> FileSet;
    struct LevelState {
        std::set<uint64_t> deleted_files;
        FileSet* added_files;
    };

    VersionSet* vset_;
    Version* base_;
    LevelState levels_[lsm::Options::kNumLevels];

public:
    VersionSetBuilder(VersionSet* vset, Version* base)
        : vset_(vset), base_(base) {
        base_->Ref();
        BySmallestKey cmp;
        cmp.internal_comparator = &vset_->icmp_;
        for (int level = 0; level < lsm::Options::kNumLevels; level++) {
            levels_[level].added_files = new FileSet(cmp);
        }
    }

    ~VersionSetBuilder() {
        for (int level = 0; level < lsm::Options::kNumLevels; level++) {
            const FileSet* added = levels_[level].added_files;
            std::vector<FileMetaData*> to_unref;
            to_unref.reserve(added->size());
            for (FileSet::const_iterator it = added->begin();
                 it != added->end(); ++it) {
                to_unref.push_back(*it);
            }
            delete added;
            for (uint32_t i = 0; i < to_unref.size(); i++) {
                FileMetaData* f = to_unref[i];
                f->refs--;
                if (f->refs <= 0) {
                    delete f;
                }
            }
        }
        base_->Unref();
    }

    void Apply(VersionEdit* edit) {
        // Apply deleted files
        for (auto const& df : edit->deleted_files_) {
            levels_[df.first].deleted_files.insert(df.second);
        }

        for (size_t i = 0; i < edit->new_files_.size(); i++) {
            const int level = edit->new_files_[i].first;
            FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
            f->refs = 1;

            // We arrange to automatically compact this file after
            // a certain number of seeks.
            f->allowed_seeks = static_cast<int>(f->file_size / 16384);
            if (f->allowed_seeks < 100) f->allowed_seeks = 100;

            levels_[level].deleted_files.erase(f->number);
            levels_[level].added_files->insert(f);
        }
    }

    void SaveTo(Version* v) {
        BySmallestKey cmp;
        cmp.internal_comparator = &vset_->icmp_;
        for (int level = 0; level < lsm::Options::kNumLevels; level++) {
            // Merge the set of added files with the set of pre-existing files.
            // Drop any deleted files.  Store the result in *v.
            const std::vector<FileMetaData*>& base_files = base_->files_[level];
            std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
            std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
            const FileSet* added_files = levels_[level].added_files;
            v->files_[level].reserve(base_files.size() + added_files->size());
            for (const auto& added_file : *added_files) {
                // Add all smaller files listed in base_
                for (std::vector<FileMetaData*>::const_iterator bpos =
                         std::upper_bound(base_iter, base_end, added_file, cmp);
                     base_iter != bpos;
                     ++base_iter) {
                    MaybeAddFile(v, level, *base_iter);
                }

                MaybeAddFile(v, level, added_file);
            }

            // Add remaining base files
            for (; base_iter != base_end; ++base_iter) {
                MaybeAddFile(v, level, *base_iter);
            }

#ifndef NDEBUG
            // Make sure there is no overlap in levels > 0
            if (level > 0) {
                for (uint32_t i = 1; i < v->files_[level].size(); i++) {
                    const InternalKey& prev_end = v->files_[level][i-1]->largest;
                    const InternalKey& this_begin = v->files_[level][i]->smallest;
                    if (vset_->icmp_.Compare(prev_end.Encode(), this_begin.Encode()) >= 0) {
                        assert(false);
                    }
                }
            }
#endif
        }
    }
    
    void MaybeAddFile(Version* v, int level, FileMetaData* f) {
        if (levels_[level].deleted_files.count(f->number) > 0) {
            // File is deleted: do nothing
        } else {
            std::vector<FileMetaData*>* files = &v->files_[level];
            if (level > 0 && !files->empty()) {
                // Must not overlap
                assert(vset_->icmp_.Compare((*files)[files->size()-1]->largest.Encode(),
                                            f->smallest.Encode()) < 0);
            }
            f->refs++;
            files->push_back(f);
        }
    }
};

VersionSet::VersionSet(const std::string& dbname,
                       const Options* options,
                       TableCache* table_cache)
    : dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(options->comparator),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      dummy_versions_(this),
      current_(nullptr) {
    AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
    current_->Unref();
    assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
}

void VersionSet::AppendVersion(Version* v) {
    // Make "v" current
    assert(v->refs_ == 0);
    assert(v != current_);
    if (current_ != nullptr) {
        current_->Unref();
    }
    current_ = v;
    v->Ref();

    // Append to linked list
    v->prev_ = dummy_versions_.prev_;
    v->next_ = &dummy_versions_;
    v->prev_->next_ = v;
    v->next_->prev_ = v;
}

void VersionSet::Finalize(Version* v) {
    // Precomputed best level for next compaction
    int best_level = -1;
    double best_score = -1;

    for (int level = 0; level < lsm::Options::kNumLevels-1; level++) {
        double score;
        if (level == 0) {
            // We treat level-0 specially by bounding the number of files
            // instead of number of bytes for two reasons:
            //
            // (1) With larger write-buffer sizes, it is nice not to do too
            // many level-0 compactions.
            //
            // (2) The files in level-0 are merged on every read and
            // therefore we wish to avoid too many files when the individual
            // file size is small (perhaps because of a small write-buffer
            // setting, or very high compression ratios, or lots of
            // overwrites/deletions).
            score = v->files_[level].size() /
                    static_cast<double>(Options::kL0_CompactionTrigger);
        } else {
            // Compute the ratio of current size to size limit.
            const uint64_t level_bytes = TotalFileSize(v->files_[level]);
            score = static_cast<double>(level_bytes) / MaxBytesForLevel(level);
        }

        if (score > best_score) {
            best_level = level;
            best_score = score;
        }
    }

    v->compaction_level_ = best_level;
    v->compaction_score_ = best_score;
}

Status VersionSet::LogAndApply(VersionEdit* edit, std::mutex* mu) {
    if (edit->has_log_number_) {
        assert(edit->log_number_ >= log_number_);
        assert(edit->log_number_ < next_file_number_);
    } else {
        edit->SetLogNumber(log_number_);
    }

    if (!edit->has_prev_log_number_) {
        edit->SetPrevLogNumber(prev_log_number_);
    }

    edit->SetNextFile(next_file_number_);
    edit->SetLastSequence(last_sequence_);

    Version* v = new Version(this);
    {
        VersionSetBuilder builder(this, current_);
        builder.Apply(edit);
        builder.SaveTo(v);
    }
    Finalize(v);

    // Initialize new descriptor log file if necessary by creating
    // a temporary file that contains a snapshot of the current version.
    std::string new_manifest_file;
    Status s;
    if (!descriptor_log_) {
        // No reason to save a new snapshot if we already have one
        // and nobody is making a new one (not supported in this simplified engine yet).
        new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
        edit->SetNextFile(next_file_number_);
        descriptor_log_.reset(new WalWriter(new_manifest_file));
        s = WriteSnapshot(descriptor_log_.get());
    }

    if (s.ok()) {
        std::string record;
        edit->EncodeTo(&record);
        s = descriptor_log_->AddRecord(record);
        if (s.ok()) {
            s = descriptor_log_->Sync();
        }
        if (s.ok()) {
            // Install the new version
            AppendVersion(v);
            log_number_ = edit->log_number_;
            prev_log_number_ = edit->prev_log_number_;
        }
    }

    if (!s.ok()) {
        delete v;
        if (!new_manifest_file.empty()) {
            // Failed to write new manifest, delete it
            descriptor_log_.reset();
            remove(new_manifest_file.c_str());
        }
    }

    return s;
}

Status VersionSet::WriteSnapshot(WalWriter* log) {
    // Save metadata
    VersionEdit edit;
    edit.SetComparatorName(Slice(icmp_.user_comparator()->Name()));

    // Save compaction pointers
    for (int level = 0; level < lsm::Options::kNumLevels; level++) {
        if (!compact_pointer_[level].empty()) {
            // Not used in this subset but good to preserve structure
            InternalKey key;
            key.SetFrom(compact_pointer_[level]);
            // edit.SetCompactPointer(level, key);
        }
    }

    // Save files
    for (int level = 0; level < lsm::Options::kNumLevels; level++) {
        const std::vector<FileMetaData*>& files = current_->files_[level];
        for (size_t i = 0; i < files.size(); i++) {
            const FileMetaData* f = files[i];
            edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
        }
    }

    std::string record;
    edit.EncodeTo(&record);
    return log->AddRecord(record);
}

// In a real implementation this would read CURRENT file etc.
// For simplicity, we assume application manages open on correct db properly.
Status VersionSet::Recover() {
    // Dummy recover, assume empty database for this exercise
    // In full LevelDB it reads CURRENT file to find right MANIFEST
    return Status::OK();
}

Compaction* VersionSet::PickCompaction() {
    Compaction* c;
    int level;

    // We prefer compactions triggered by too much data in a level over
    // the compactions triggered by seeks.
    const bool size_compaction = (current_->compaction_score_ >= 1);
    const bool seek_compaction = (current_->file_to_compact_ != nullptr);
    
    if (size_compaction) {
        level = current_->compaction_level_;
        assert(level >= 0);
        assert(level + 1 < lsm::Options::kNumLevels);
        c = new Compaction(options_, level);

        // Pick the first file that comes after compact_pointer_[level]
        for (size_t i = 0; i < current_->files_[level].size(); i++) {
            FileMetaData* f = current_->files_[level][i];
            if (compact_pointer_[level].empty() ||
                icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
                c->inputs_[0].push_back(f);
                break;
            }
        }
        if (c->inputs_[0].empty()) {
            // Wrap-around to the beginning of the key space
            c->inputs_[0].push_back(current_->files_[level][0]);
        }
    } else if (seek_compaction) {
        level = current_->file_to_compact_level_;
        c = new Compaction(options_, level);
        c->inputs_[0].push_back(current_->file_to_compact_);
    } else {
        return nullptr;
    }

    c->input_version_ = current_;
    c->input_version_->Ref();

    // Files in level 0 may overlap each other, so pick up all overlapping ones
    if (level == 0) {
        InternalKey smallest, largest;
        // Simple bounding box for the single file picked
        smallest = c->inputs_[0][0]->smallest;
        largest = c->inputs_[0][0]->largest;
        
        c->inputs_[0].clear(); // rebuild
        Slice user_smallest = smallest.user_key();
        Slice user_largest = largest.user_key();
        
        for (size_t i = 0; i < current_->files_[level].size(); i++) {
            FileMetaData* f = current_->files_[level][i];
            if (icmp_.user_comparator()->Compare(f->largest.user_key(), user_smallest) >= 0 &&
                icmp_.user_comparator()->Compare(f->smallest.user_key(), user_largest) <= 0) {
                c->inputs_[0].push_back(f);
            }
        }
    }

    SetupOtherInputs(c);
    return c;
}

void VersionSet::SetupOtherInputs(Compaction* c) {
    const int level = c->level();
    InternalKey smallest, largest;
    
    // Find overarching smallest and largest
    smallest = c->inputs_[0][0]->smallest;
    largest = c->inputs_[0][0]->largest;
    for (size_t i = 1; i < c->inputs_[0].size(); i++) {
        FileMetaData* f = c->inputs_[0][i];
        if (icmp_.Compare(f->smallest.Encode(), smallest.Encode()) < 0) {
            smallest = f->smallest;
        }
        if (icmp_.Compare(f->largest.Encode(), largest.Encode()) > 0) {
            largest = f->largest;
        }
    }
    
    // Find all overlapping files in next level
    Slice user_smallest = smallest.user_key();
    Slice user_largest = largest.user_key();
    for (size_t i = 0; i < current_->files_[level + 1].size(); i++) {
        FileMetaData* f = current_->files_[level + 1][i];
        if (icmp_.user_comparator()->Compare(f->largest.user_key(), user_smallest) >= 0 &&
            icmp_.user_comparator()->Compare(f->smallest.user_key(), user_largest) <= 0) {
            c->inputs_[1].push_back(f);
        }
    }
    
    // Get entire range covered by compaction
    for (size_t i = 0; i < c->inputs_[1].size(); i++) {
        FileMetaData* f = c->inputs_[1][i];
        if (icmp_.Compare(f->smallest.Encode(), smallest.Encode()) < 0) {
            smallest = f->smallest;
        }
        if (icmp_.Compare(f->largest.Encode(), largest.Encode()) > 0) {
            largest = f->largest;
        }
    }
    
    user_smallest = smallest.user_key();
    user_largest = largest.user_key();
    
    if (level + 2 < lsm::Options::kNumLevels) {
        for (size_t i = 0; i < current_->files_[level + 2].size(); i++) {
            FileMetaData* f = current_->files_[level + 2][i];
            if (icmp_.user_comparator()->Compare(f->largest.user_key(), user_smallest) >= 0 &&
                icmp_.user_comparator()->Compare(f->smallest.user_key(), user_largest) <= 0) {
                c->grandparents_.push_back(f);
            }
        }
    }
    
    // Update the place where we will do the next compaction for this level.
    // We update this immediately instead of waiting for the VersionEdit
    // to be applied so that if the compaction fails, we will try a different
    // key range next time.
    compact_pointer_[level] = largest.Encode().ToString();
    c->edit_.SetCompactPointer(level, largest);
}

}  // namespace lsm
