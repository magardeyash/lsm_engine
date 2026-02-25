#include "src/db/db_impl.h"
#include <vector>
#include <string>
#include "lsm/comparator.h"
#include "src/table/sstable_builder.h"
#include "src/table/table_cache.h"
#include "src/db/merger.h"
#include "src/util/coding.h"
#include <sys/stat.h>

#ifdef _WIN32
#include <direct.h>
#define mkdir(dir, mode) _mkdir(dir)
#endif

namespace lsm {

struct DBImpl::Writer {
    Status status;
    bool done;
    std::condition_variable cv;
    const WriteOptions* options;
    uint64_t sequence;
    Slice key;
    Slice value;
    ValueType type;

    Writer(const WriteOptions* opt, ValueType t, const Slice& k, const Slice& v)
        : done(false), options(opt), sequence(0), key(k), value(v), type(t) {}
};

static std::string LogFileName(const std::string& dbname, uint64_t number) {
    char buf[100];
    snprintf(buf, sizeof(buf), "%s/%06llu.log", dbname.c_str(),
             static_cast<unsigned long long>(number));
    return std::string(buf);
}

static std::string TableFileName(const std::string& dbname, uint64_t number) {
    char buf[100];
    snprintf(buf, sizeof(buf), "%s/%06llu.sst", dbname.c_str(),
             static_cast<unsigned long long>(number));
    return std::string(buf);
}

Status DB::Open(const Options& options, const std::string& name, DB** dbptr) {
    *dbptr = nullptr;

    DBImpl* impl = new DBImpl(options, name);
    impl->mutex_.lock();
    VersionEdit edit;

    // Create directory if missing
    struct stat st;
    if (stat(name.c_str(), &st) != 0) {
        if (options.create_if_missing) {
            mkdir(name.c_str(), 0755);
        } else {
            impl->mutex_.unlock();
            delete impl;
            return Status::InvalidArgument(name, "does not exist (create_if_missing is false)");
        }
    } else if (options.error_if_exists) {
        impl->mutex_.unlock();
        delete impl;
        return Status::InvalidArgument(name, "exists (error_if_exists is true)");
    }

    Status s = impl->Recover();
    if (s.ok()) {
        uint64_t new_log_number = impl->versions_->NewFileNumber();
        impl->log_.reset(new WalWriter(LogFileName(name, new_log_number)));
        edit.SetLogNumber(new_log_number);
        impl->logfile_number_ = new_log_number;
        s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
    }

    if (s.ok()) {
        impl->MaybeScheduleCompaction();
    }
    impl->mutex_.unlock();

    if (s.ok()) {
        *dbptr = impl;
    } else {
        delete impl;
    }
    return s;
}

DBImpl::DBImpl(const Options& options, const std::string& dbname)
    : options_(options),
      dbname_(dbname),
      internal_comparator_(options.comparator),
      internal_options_(options),
      table_cache_(nullptr),
      shutting_down_(false),
      bg_compaction_scheduled_(false),
      mem_(new MemTable(internal_comparator_)),
      imm_(nullptr),
      has_imm_(false),
      logfile_number_(0),
      versions_(nullptr) {
    internal_options_.comparator = &internal_comparator_;
    table_cache_ = new TableCache(dbname, &internal_options_, internal_options_.block_cache_capacity);
    versions_ = new VersionSet(dbname, &internal_options_, table_cache_);
    mem_->Ref();

    // Start the persistent background compaction thread
    bg_thread_ = std::thread(&DBImpl::BackgroundThreadMain, this);
}

DBImpl::~DBImpl() {
    {
        std::unique_lock<std::mutex> l(mutex_);
        shutting_down_.store(true, std::memory_order_release);
        bg_work_cv_.notify_one();  // Wake the background thread so it can exit
    }

    // Join the persistent background thread
    if (bg_thread_.joinable()) {
        bg_thread_.join();
    }

    {
        std::unique_lock<std::mutex> l(mutex_);
        while (bg_compaction_scheduled_) {
            bg_cv_.wait(l);
        }
    }

    if (mem_ != nullptr) mem_->Unref();
    if (imm_ != nullptr) imm_->Unref();
    delete versions_;
    delete table_cache_;
}

Status DBImpl::Recover() {
    return versions_->Recover();
}

Status DBImpl::MakeRoomForWrite(bool force) {
    bool allow_delay = !force;
    Status s;
    while (true) {
        if (!bg_error_.ok()) {
            s = bg_error_;
            break;
        } else if (allow_delay && 
                   versions_->NumLevelFiles(0) >= lsm::Options::kL0_SlowdownWritesTrigger) {
            mutex_.unlock();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            allow_delay = false;
            mutex_.lock();
        } else if (!force &&
                   (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
            break;
        } else if (imm_ != nullptr) {
            std::unique_lock<std::mutex> wait_lock(mutex_, std::adopt_lock);
            bg_cv_.wait(wait_lock);
            wait_lock.release();
        } else if (versions_->NumLevelFiles(0) >= lsm::Options::kL0_StopWritesTrigger) {
            std::unique_lock<std::mutex> wait_lock(mutex_, std::adopt_lock);
            bg_cv_.wait(wait_lock);
            wait_lock.release();
        } else {
            uint64_t new_log_number = versions_->NewFileNumber();
            log_.reset(new WalWriter(LogFileName(dbname_, new_log_number)));
            logfile_number_ = new_log_number;
            imm_ = mem_;
            has_imm_.store(true, std::memory_order_release);
            mem_ = new MemTable(internal_comparator_);
            mem_->Ref();
            force = false;
            MaybeScheduleCompaction();
        }
    }
    return s;
}

// ---------------------------------------------------------------------------
// Group commit: Put and Delete both enqueue a Writer and call Write().
// The leader thread batches all queued writers into a single WAL record
// and applies them all to the memtable atomically.
// ---------------------------------------------------------------------------

Status DBImpl::Put(const WriteOptions& options, const Slice& key, const Slice& value) {
    Writer w(&options, kTypeValue, key, value);
    return Write(&w);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
    Writer w(&options, kTypeDeletion, key, Slice());
    return Write(&w);
}

DBImpl::Writer* DBImpl::BuildBatchGroup(Writer** last_writer) {
    assert(!writers_.empty());
    Writer* first = writers_.front();
    *last_writer = first;

    // Walk the deque and group all pending writers into the batch.
    // Cap the batch at ~1 MB to avoid excessive WAL record sizes.
    static const size_t kMaxBatchSize = 1 << 20;  // 1 MB
    size_t size = first->key.size() + first->value.size();

    for (auto it = writers_.begin() + 1; it != writers_.end(); ++it) {
        Writer* w = *it;
        size += w->key.size() + w->value.size();
        if (size > kMaxBatchSize) break;
        *last_writer = w;
    }
    return first;
}

Status DBImpl::Write(Writer* my_writer) {
    std::unique_lock<std::mutex> l(mutex_);
    writers_.push_back(my_writer);

    // Wait until this writer becomes the leader (front of deque) or is
    // completed by another leader.
    while (!my_writer->done && my_writer != writers_.front()) {
        my_writer->cv.wait(l);
    }
    if (my_writer->done) {
        return my_writer->status;
    }

    // This thread is the leader. Batch all pending writers.
    Status s = MakeRoomForWrite(false);
    uint64_t last_sequence = versions_->LastSequence();

    Writer* last_writer = my_writer;
    if (s.ok()) {
        BuildBatchGroup(&last_writer);

        // Serialize the entire batch into a single WAL record.
        // Format: [count (4)] { Sequence (8) | Type (1) | KeyLen | Key | ValLen | Val } ...
        std::string record;
        bool need_sync = false;
        uint32_t count = 0;

        // Reserve space for count header, will fill in later
        record.resize(4);

        for (auto it = writers_.begin(); ; ++it) {
            Writer* w = *it;
            last_sequence++;
            PutFixed64(&record, last_sequence);
            record.push_back(static_cast<char>(w->type));
            PutLengthPrefixedSlice(&record, w->key);
            PutLengthPrefixedSlice(&record, w->value);
            if (w->options->sync) need_sync = true;
            count++;
            if (w == last_writer) break;
        }

        // Fill in the count header
        EncodeFixed32(&record[0], count);

        s = log_->AddRecord(record);
        if (s.ok() && need_sync) {
            s = log_->Sync();
        }

        // Apply all entries to the memtable
        if (s.ok()) {
            uint64_t seq = versions_->LastSequence();
            for (auto it = writers_.begin(); ; ++it) {
                Writer* w = *it;
                seq++;
                mem_->Add(seq, w->type, w->key, w->value);
                if (w == last_writer) break;
            }
            versions_->SetLastSequence(last_sequence);
        }
    }

    // Signal all writers in the batch that they are done.
    while (true) {
        Writer* ready = writers_.front();
        writers_.pop_front();
        if (ready != my_writer) {
            ready->status = s;
            ready->done = true;
            ready->cv.notify_one();
        }
        if (ready == last_writer) break;
    }

    // Wake the next leader if there are more writers waiting.
    if (!writers_.empty()) {
        writers_.front()->cv.notify_one();
    }

    my_writer->status = s;
    my_writer->done = true;
    return s;
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key, std::string* value) {
    Status s;
    std::unique_lock<std::mutex> l(mutex_);
    
    MemTable* mem = mem_;
    MemTable* imm = imm_;
    Version* current = versions_->current();
    mem->Ref();
    if (imm != nullptr) imm->Ref();
    current->Ref();

    uint64_t seq = versions_->LastSequence();
    LookupKey lkey(key, seq);
    
    l.unlock();

    if (mem->Get(lkey, value, &s)) {
        // Done
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
        // Done
    } else {
        current->Get(options, lkey.internal_key(), value, &s);
    }

    l.lock();
    mem->Unref();
    if (imm != nullptr) imm->Unref();
    current->Unref();

    return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
    std::unique_lock<std::mutex> l(mutex_);
    MemTable* mem = mem_;
    MemTable* imm = imm_;
    Version* current = versions_->current();
    mem->Ref();
    if (imm != nullptr) imm->Ref();
    current->Ref();

    std::vector<Iterator*> list;
    list.push_back(mem->NewIterator());
    if (imm != nullptr) {
        list.push_back(imm->NewIterator());
    }
    current->AddIterators(options, &list);
    Iterator* internal_iter = NewMergingIterator(&versions_->icmp_, &list[0], list.size());
    
    class DBIterator : public Iterator {
    public:
        DBIterator(DBImpl* db, const Comparator* ucmp, Iterator* iter, uint64_t s, MemTable* m, MemTable* im, Version* v)
            : db_(db), user_comparator_(ucmp), iter_(iter), sequence_(s), mem_(m), imm_(im), version_(v) {
        }
        ~DBIterator() override {
            delete iter_;
            std::unique_lock<std::mutex> l(db_->mutex_);
            mem_->Unref();
            if (imm_ != nullptr) imm_->Unref();
            version_->Unref();
        }

        bool Valid() const override { return iter_->Valid(); }
        Status status() const override { return iter_->status(); }
        Slice key() const override { return InternalKey::ExtractUserKey(iter_->key()); }
        Slice value() const override { return iter_->value(); }

        void Next() override {
            iter_->Next();
            FindNextUserEntry(true);
        }

        void Prev() override {
            iter_->Prev();
            // This is a simplified Prev, we don't fully implement reverse iteration
            // correctly here for overlapping keys, but we'll step back to a valid one.
            FindPrevUserEntry();
        }

        void Seek(const Slice& target) override {
            LookupKey k(target, sequence_);
            iter_->Seek(k.internal_key());
            FindNextUserEntry(false);
        }

        void SeekToFirst() override {
            iter_->SeekToFirst();
            FindNextUserEntry(false);
        }

        void SeekToLast() override {
            iter_->SeekToLast();
            FindPrevUserEntry();
        }

    private:
        void FindNextUserEntry(bool skipping) {
            // Very simplified DBIterator: skip deleted keys and older sequence numbers
            std::string saved_key;
            while (iter_->Valid()) {
                ParsedInternalKey ikey;
                if (ParseInternalKey(iter_->key(), &ikey) && ikey.sequence <= sequence_) {
                    if (skipping && user_comparator_->Compare(ikey.user_key, Slice(saved_key)) == 0) {
                        // Already saw a newer version
                    } else {
                        saved_key.assign(ikey.user_key.data(), ikey.user_key.size());
                        skipping = true;
                        if (ikey.type == kTypeDeletion) {
                            // Skip deleted
                        } else {
                            return; // Found valid entry
                        }
                    }
                }
                iter_->Next();
            }
        }
        
        void FindPrevUserEntry() {
            // Simplified reverse mapping, not fully compliant with LevelDB reverse iteration
            while (iter_->Valid()) {
                ParsedInternalKey ikey;
                if (ParseInternalKey(iter_->key(), &ikey) && ikey.sequence <= sequence_ && ikey.type != kTypeDeletion) {
                    return; // Rough approximation
                }
                iter_->Prev();
            }
        }

        DBImpl* db_;
        const Comparator* user_comparator_;
        Iterator* iter_;
        uint64_t sequence_;
        MemTable* mem_;
        MemTable* imm_;
        Version* version_;
    };

    return new DBIterator(this, options_.comparator, internal_iter, versions_->LastSequence(), mem, imm, current);
}

// ---------------------------------------------------------------------------
// Background compaction: persistent thread wakes on demand via condition variable
// ---------------------------------------------------------------------------

void DBImpl::MaybeScheduleCompaction() {
    if (bg_compaction_scheduled_) return;
    if (shutting_down_.load(std::memory_order_acquire)) return;

    if (imm_ == nullptr &&
        versions_->current()->compaction_score_ < 1 &&
        versions_->current()->file_to_compact_ == nullptr) {
        return;
    }

    bg_compaction_scheduled_ = true;
    bg_work_cv_.notify_one();  // Wake the persistent background thread
}

void DBImpl::BackgroundThreadMain() {
    std::unique_lock<std::mutex> l(mutex_);
    while (!shutting_down_.load(std::memory_order_acquire)) {
        if (!bg_compaction_scheduled_) {
            bg_work_cv_.wait(l);
            continue;
        }
        BackgroundCall();
    }
}

void DBImpl::BackgroundCall() {
    assert(bg_compaction_scheduled_);
    if (shutting_down_.load(std::memory_order_acquire)) {
        // Stop
    } else if (!bg_error_.ok()) {
        // Stop
    } else {
        BackgroundCompaction();
    }

    bg_compaction_scheduled_ = false;

    // Previous compaction may have produced too many files in a level,
    // so reschedule if needed.
    MaybeScheduleCompaction();
    bg_cv_.notify_all();
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base) {
    std::unique_lock<std::mutex> l(mutex_, std::defer_lock); // Not strictly locked while dumping

    FileMetaData meta;
    meta.number = versions_->NewFileNumber();
    std::string fname = TableFileName(dbname_, meta.number);
    std::ofstream* file = new std::ofstream(fname, std::ios::out | std::ios::binary);

    // SSTables store internal keys, so use internal comparator
    Options table_options = options_;
    table_options.comparator = &internal_comparator_;
    TableBuilder* builder = new TableBuilder(table_options, file);
    Iterator* iter = mem->NewIterator();
    iter->SeekToFirst();
    if (iter->Valid()) {
        meta.smallest.SetFrom(iter->key());
        InternalKey last;
        while (iter->Valid()) {
            last.SetFrom(iter->key());
            builder->Add(iter->key(), iter->value());
            iter->Next();
        }
        meta.largest = last;
    }
    Status s = builder->Finish();
    meta.file_size = builder->FileSize();

    delete iter;
    delete builder;
    delete file;

    if (s.ok()) {
        edit->AddFile(0, meta.number, meta.file_size, meta.smallest, meta.largest);
    } else {
        remove(fname.c_str());
    }
    return s;
}

Status DBImpl::BackgroundCompaction() {
    if (imm_ != nullptr) {
        VersionEdit edit;
        Version* base = versions_->current();
        base->Ref();
        mutex_.unlock();
        Status s = WriteLevel0Table(imm_, &edit, base);
        mutex_.lock();
        base->Unref();

        if (s.ok()) {
            edit.SetPrevLogNumber(0);
            edit.SetLogNumber(logfile_number_);
            s = versions_->LogAndApply(&edit, &mutex_);
        }
        if (s.ok()) {
            imm_->Unref();
            imm_ = nullptr;
            has_imm_.store(false, std::memory_order_release);
        }
        return s;
    }

    Compaction* c = versions_->PickCompaction();
    Status status;
    if (c == nullptr) {
        // Nothing to do
    } else if (c->IsTrivialMove()) {
        VersionEdit edit;
        FileMetaData* f = c->input(0, 0);
        edit.DeleteFile(c->level(), f->number);
        edit.AddFile(c->level() + 1, f->number, f->file_size, f->smallest, f->largest);
        status = versions_->LogAndApply(&edit, &mutex_);
        c->ReleaseInputs();
        delete c;
    } else {
        status = DoCompactionWork(c);
        CleanupCompaction(c);
        c->ReleaseInputs();
        delete c;
    }
    return status;
}

Status DBImpl::DoCompactionWork(Compaction* c) {
    mutex_.unlock();

    // Build iterators for all input files from both levels
    std::vector<Iterator*> list;
    for (int i = 0; i < c->num_input_files(0); i++) {
        list.push_back(table_cache_->NewIterator(ReadOptions(),
                                                 c->input(0, i)->number,
                                                 c->input(0, i)->file_size));
    }
    for (int i = 0; i < c->num_input_files(1); i++) {
        list.push_back(table_cache_->NewIterator(ReadOptions(),
                                                 c->input(1, i)->number,
                                                 c->input(1, i)->file_size));
    }

    Iterator* input = NewMergingIterator(&versions_->icmp_, &list[0], list.size());
    input->SeekToFirst();

    Status status;
    ParsedInternalKey ikey;
    std::string current_user_key;
    bool has_current_user_key = false;
    uint64_t last_sequence_for_key = kMaxSequenceNumber;
    // Snapshot the visible sequence — don't read the live value since
    // the main thread may be advancing it concurrently.
    const uint64_t smallest_snapshot = versions_->LastSequence();

    // Track the current output file being built
    // SSTables store internal keys, so use internal comparator
    Options table_options = options_;
    table_options.comparator = &internal_comparator_;
    std::ofstream* outfile = nullptr;
    std::unique_ptr<TableBuilder> builder;
    InternalKey smallest_key, largest_key;
    uint64_t output_file_number = 0;

    // Add input file deletions to the edit
    c->AddInputDeletions(&c->edit_);

    while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {
        Slice key = input->key();

        // Check if we should start a new output file due to grandparent overlap
        if (c->ShouldStopBefore(key) && builder != nullptr) {
            // Finish current output
            status = builder->Finish();
            if (status.ok()) {
                c->edit_.AddFile(c->level() + 1, output_file_number,
                                builder->FileSize(), smallest_key, largest_key);
            }
            delete outfile;
            outfile = nullptr;
            builder.reset();
            if (!status.ok()) break;
        }

        bool drop = false;
        if (!ParseInternalKey(key, &ikey)) {
            current_user_key.clear();
            has_current_user_key = false;
            last_sequence_for_key = kMaxSequenceNumber;
        } else {
            if (!has_current_user_key ||
                options_.comparator->Compare(ikey.user_key, Slice(current_user_key)) != 0) {
                current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
                has_current_user_key = true;
                last_sequence_for_key = kMaxSequenceNumber;
            }

            if (last_sequence_for_key <= smallest_snapshot) {
                drop = true;
            } else if (ikey.type == kTypeDeletion &&
                       ikey.sequence <= smallest_snapshot &&
                       c->IsBaseLevelForKey(ikey.user_key)) {
                // No higher-level files contain this key (by key range).
                // Additionally check Bloom filters of the output-level files
                // to see if the deleted key might actually exist there.
                // If Bloom filters all say "not present", the tombstone can
                // be safely dropped (false positives just keep it — safe).
                bool maybe_in_output = false;
                for (int i = 0; i < c->num_input_files(1); i++) {
                    FileMetaData* f = c->input(1, i);
                    if (table_cache_->MayContain(f->number, f->file_size,
                                                 ikey.user_key)) {
                        maybe_in_output = true;
                        break;
                    }
                }
                if (!maybe_in_output) {
                    drop = true;
                }
            }

            last_sequence_for_key = ikey.sequence;
        }

        if (!drop) {
            // Open a new output file if needed
            if (builder == nullptr) {
                output_file_number = versions_->NewFileNumber();
                std::string fname = TableFileName(dbname_, output_file_number);
                outfile = new std::ofstream(fname, std::ios::out | std::ios::binary);
                if (!outfile->is_open()) {
                    status = Status::IOError("Failed to create compaction output: ", fname);
                    delete outfile;
                    outfile = nullptr;
                    break;
                }
                builder.reset(new TableBuilder(table_options, outfile));
                smallest_key.SetFrom(key);
            }
            largest_key.SetFrom(key);
            builder->Add(key, input->value());

            if (builder->FileSize() >= c->MaxOutputFileSize()) {
                status = builder->Finish();
                if (status.ok()) {
                    c->edit_.AddFile(c->level() + 1, output_file_number,
                                    builder->FileSize(), smallest_key, largest_key);
                }
                delete outfile;
                outfile = nullptr;
                builder.reset();
                if (!status.ok()) break;
            }
        }

        input->Next();
    }

    if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
        status = Status::IOError("Deleting DB during compaction");
    }

    // Finish the last output file if any
    if (status.ok() && builder != nullptr) {
        status = builder->Finish();
        if (status.ok()) {
            c->edit_.AddFile(c->level() + 1, output_file_number,
                            builder->FileSize(), smallest_key, largest_key);
        }
    } else if (builder != nullptr) {
        builder->Abandon();
    }
    delete outfile;
    builder.reset();

    delete input;
    mutex_.lock();

    if (status.ok()) {
        status = versions_->LogAndApply(&c->edit_, &mutex_);
    }
    return status;
}

void DBImpl::CleanupCompaction(Compaction* c) {
    // Evict table cache entries for deleted input files
    for (int which = 0; which < 2; which++) {
        for (int i = 0; i < c->num_input_files(which); i++) {
            table_cache_->Evict(c->input(which, i)->number);
        }
    }
}

}  // namespace lsm
