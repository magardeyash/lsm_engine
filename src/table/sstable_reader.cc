#include "src/table/sstable_reader.h"
#include <cassert>
#include "src/table/format.h"
#include "src/db/memtable.h"
#include "lsm/comparator.h"
#include "src/util/coding.h"
#include "src/util/crc32.h"
#include "src/util/bloom.h"
#include <fstream>
#include <mutex>
#include <vector>

#ifdef LSM_HAVE_ZSTD
#include <zstd.h>
#endif

namespace lsm {

static inline const char* DecodeEntry(const char* p, const char* limit,
                                      uint32_t* shared,
                                      uint32_t* non_shared,
                                      uint32_t* value_length) {
    if (limit - p < 3) return nullptr;
    *shared = reinterpret_cast<const uint8_t*>(p)[0];
    *non_shared = reinterpret_cast<const uint8_t*>(p)[1];
    *value_length = reinterpret_cast<const uint8_t*>(p)[2];
    if ((*shared | *non_shared | *value_length) < 128) {
        p += 3;
    } else {
        if ((p = GetVarint32Ptr(p, limit, shared)) == nullptr) return nullptr;
        if ((p = GetVarint32Ptr(p, limit, non_shared)) == nullptr) return nullptr;
        if ((p = GetVarint32Ptr(p, limit, value_length)) == nullptr) return nullptr;
    }

    if (static_cast<uint32_t>(limit - p) < (*non_shared + *value_length)) {
        return nullptr;
    }
    return p;
}

class Block {
public:
    // Initialize the block with the specified contents.
    explicit Block(const Slice& contents)
        : data_(contents.data()),
          size_(contents.size()),
          restart_offset_(0) {
        if (size_ < sizeof(uint32_t)) {
            size_ = 0;
            return;
        }
        size_t max_restarts_allowed = (size_ - sizeof(uint32_t)) / sizeof(uint32_t);
        if (NumRestarts() > max_restarts_allowed) {
            size_ = 0;
            return;
        }
        // NumRestarts() is read from the end of the block
        restart_offset_ = size_ - (1 + NumRestarts()) * sizeof(uint32_t);
    }

    ~Block() { delete[] data_; }

    Block(const Block&) = delete;
    Block& operator=(const Block&) = delete;

    size_t size() const { return size_; }
    Iterator* NewIterator(const Comparator* comparator);

private:
    uint32_t NumRestarts() const {
        assert(size_ >= sizeof(uint32_t));
        return DecodeFixed32(data_ + size_ - sizeof(uint32_t));
    }

    const char* data_;
    size_t size_;
    uint32_t restart_offset_;
    friend class BlockIter;
    friend class Table;
};

class BlockIter : public Iterator {
public:
    BlockIter(const Comparator* comparator,
              const char* data,
              uint32_t restarts,
              uint32_t num_restarts)
        : comparator_(comparator),
          data_(data),
          restarts_(restarts),
          num_restarts_(num_restarts),
          current_(restarts_),
          restart_index_(num_restarts_) {
        assert(num_restarts_ > 0);
    }

    bool Valid() const override {
        return current_ < restarts_;
    }

    Status status() const override {
        return status_;
    }

    Slice key() const override {
        assert(Valid());
        return key_;
    }

    Slice value() const override {
        assert(Valid());
        return value_;
    }

    void Next() override {
        assert(Valid());
        ParseNextKey();
    }

    void Prev() override {
        assert(Valid());
        const uint32_t original = current_;
        while (GetRestartPoint(restart_index_) >= original) {
            if (restart_index_ == 0) {
                current_ = restarts_;
                restart_index_ = num_restarts_;
                return;
            }
            restart_index_--;
        }

        SeekToRestartPoint(restart_index_);
        do {
        } while (ParseNextKey() && NextEntryOffset() < original);
    }

    void Seek(const Slice& target) override {
        uint32_t left = 0;
        uint32_t right = num_restarts_ - 1;
        while (left < right) {
            uint32_t mid = (left + right + 1) / 2;
            uint32_t region_offset = GetRestartPoint(mid);
            uint32_t shared, non_shared, value_length;
            const char* key_ptr = DecodeEntry(data_ + region_offset,
                                              data_ + restarts_,
                                              &shared, &non_shared, &value_length);
            if (key_ptr == nullptr || (shared != 0)) {
                CorruptionError();
                return;
            }
            Slice mid_key(key_ptr, non_shared);
            if (comparator_->Compare(mid_key, target) < 0) {
                left = mid;
            } else {
                right = mid - 1;
            }
        }

        SeekToRestartPoint(left);
        while (true) {
            if (!ParseNextKey()) {
                return;
            }
            if (comparator_->Compare(key_, target) >= 0) {
                return;
            }
        }
    }

    void SeekToFirst() override {
        SeekToRestartPoint(0);
        ParseNextKey();
    }

    void SeekToLast() override {
        SeekToRestartPoint(num_restarts_ - 1);
        while (ParseNextKey() && NextEntryOffset() < restarts_) {
        }
    }

private:
    void CorruptionError() {
        current_ = restarts_;
        restart_index_ = num_restarts_;
        status_ = Status::Corruption("bad entry in block");
        key_.clear();
        value_.clear();
    }

    bool ParseNextKey() {
        current_ = NextEntryOffset();
        const char* p = data_ + current_;
        const char* limit = data_ + restarts_;
        if (p >= limit) {
            current_ = restarts_;
            restart_index_ = num_restarts_;
            return false;
        }

        uint32_t shared, non_shared, value_length;
        p = DecodeEntry(p, limit, &shared, &non_shared, &value_length);
        if (p == nullptr || key_.size() < shared) {
            CorruptionError();
            return false;
        } else {
            key_.resize(shared);
            key_.append(p, non_shared);
            value_ = Slice(p + non_shared, value_length);
            while (restart_index_ + 1 < num_restarts_ &&
                   GetRestartPoint(restart_index_ + 1) < current_) {
                ++restart_index_;
            }
            return true;
        }
    }

    uint32_t GetRestartPoint(uint32_t index) {
        assert(index < num_restarts_);
        return DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
    }

    void SeekToRestartPoint(uint32_t index) {
        key_.clear();
        restart_index_ = index;
        uint32_t offset = GetRestartPoint(index);
        value_ = Slice(data_ + offset, 0);
    }

    inline uint32_t NextEntryOffset() const {
        return (value_.data() + value_.size()) - data_;
    }

    const Comparator* const comparator_;
    const char* const data_;
    uint32_t const restarts_;
    uint32_t const num_restarts_;

    uint32_t current_;
    uint32_t restart_index_;
    std::string key_;
    Slice value_;
    Status status_;
};

Iterator* Block::NewIterator(const Comparator* comparator) {
    if (size_ < sizeof(uint32_t)) {
        return NewErrorIterator(Status::Corruption("bad block contents"));
    }
    const uint32_t num_restarts = NumRestarts();
    if (num_restarts == 0) {
        return NewEmptyIterator();
    } else {
        return new BlockIter(comparator, data_, restart_offset_, num_restarts);
    }
}

// ReadBlock: reads a block from the persistent file handle stored in Table::Rep.
// Protected by file_mutex_ to serialize concurrent reads on the same stream.
static Status ReadBlockFromHandle(std::ifstream& file, std::mutex& file_mutex,
                                  const ReadOptions& options,
                                  const BlockHandle& handle, Block** result) {
    *result = nullptr;

    const size_t n = static_cast<size_t>(handle.size());
    char* buf = new char[n + kBlockTrailerSize];

    {
        std::lock_guard<std::mutex> lock(file_mutex);
        file.clear();
        file.seekg(handle.offset(), std::ios::beg);
        file.read(buf, n + kBlockTrailerSize);
        if (file.gcount() != static_cast<std::streamsize>(n + kBlockTrailerSize)) {
            delete[] buf;
            return Status::IOError("truncated block read");
        }
    }

    if (options.verify_checksums) {
        const uint32_t crc = crc32c::Unmask(DecodeFixed32(buf + n + 1));
        const uint32_t actual = crc32c::Value(buf, n + 1);
        if (crc != actual) {
            delete[] buf;
            return Status::Corruption("block checksum mismatch");
        }
    }

    switch (buf[n]) {
        case Options::kNoCompression:
            *result = new Block(Slice(buf, n));
            break;

        case Options::kZstdCompression: {
#ifdef LSM_HAVE_ZSTD
            unsigned long long uncompressed_size = ZSTD_getFrameContentSize(buf, n);
            if (uncompressed_size == ZSTD_CONTENTSIZE_ERROR || 
                uncompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
                delete[] buf;
                return Status::Corruption("bad zstd compressed block");
            }
            char* ubuf = new char[uncompressed_size];
            size_t actual_size = ZSTD_decompress(ubuf, uncompressed_size, buf, n);
            delete[] buf;
            if (ZSTD_isError(actual_size) || actual_size != uncompressed_size) {
                delete[] ubuf;
                return Status::Corruption("bad zstd compressed block");
            }
            *result = new Block(Slice(ubuf, actual_size));
#else
            delete[] buf;
            return Status::NotSupported("zstd compression not built in");
#endif
            break;
        }
        default:
            delete[] buf;
            return Status::Corruption("bad block type");
    }
    return Status::OK();
}

struct Table::Rep {
    ~Rep() {
        delete filter;
        delete[] const_cast<char*>(filter_data);
        delete index_block;
    }

    Options options;
    Status status;
    std::string filename;
    uint64_t file_size;
    
    // Persistent file handle — opened once, reused for all reads.
    // Protected by file_mutex_ for thread-safe concurrent access.
    std::ifstream file_;
    std::mutex file_mutex_;

    Footer footer;
    Block* index_block;
    const char* filter_data;
    size_t filter_data_size;
    BloomFilterPolicy* filter;

    BlockHandle metaindex_handle;
};

Status Table::Open(const Options& options,
                   const std::string& filename,
                   uint64_t file_size,
                   Table** table) {
    *table = nullptr;
    if (file_size < Footer::kEncodedLength) {
        return Status::Corruption("file is too short to be an sstable");
    }

    Rep* rep = new Table::Rep;
    rep->file_.open(filename, std::ios::in | std::ios::binary);
    if (!rep->file_.is_open()) {
        delete rep;
        return Status::IOError("Failed to open SSTable: ", filename);
    }

    char footer_input[Footer::kEncodedLength];
    {
        std::lock_guard<std::mutex> lock(rep->file_mutex_);
        rep->file_.seekg(file_size - Footer::kEncodedLength, std::ios::beg);
        rep->file_.read(footer_input, Footer::kEncodedLength);
        if (rep->file_.gcount() != Footer::kEncodedLength) {
            delete rep;
            return Status::IOError("Failed to read footer from SSTable.");
        }
    }

    Footer footer;
    Slice footer_input_slice(footer_input, Footer::kEncodedLength);
    Status s = footer.DecodeFrom(&footer_input_slice);
    if (!s.ok()) {
        delete rep;
        return s;
    }

    // Read the index block using the persistent handle.
    Block* index_block = nullptr;
    ReadOptions opt;
    if (options.paranoid_checks) {
        opt.verify_checksums = true;
    }
    s = ReadBlockFromHandle(rep->file_, rep->file_mutex_, opt,
                            footer.index_handle(), &index_block);

    if (s.ok()) {
        rep->options = options;
        rep->filename = filename;
        rep->file_size = file_size;
        rep->metaindex_handle = footer.metaindex_handle();
        rep->index_block = index_block;
        rep->filter_data = nullptr;
        rep->filter = nullptr;
        *table = new Table(rep);
        (*table)->ReadMeta(footer);
    } else {
        delete index_block;
        delete rep;
    }

    return s;
}

void Table::ReadMeta(const Footer& footer) {
    if (rep_->options.bloom_bits_per_key == 0) {
        return;
    }

    ReadOptions opt;
    if (rep_->options.paranoid_checks) {
        opt.verify_checksums = true;
    }
    Block* meta = nullptr;
    Status s = ReadBlockFromHandle(rep_->file_, rep_->file_mutex_, opt,
                                   footer.metaindex_handle(), &meta);
    if (!s.ok()) {
        return;
    }

    std::unique_ptr<Block> meta_guard(meta);
    Iterator* iter = meta->NewIterator(BytewiseComparator());
    std::string key = "filter.";
    key.append(BloomFilterPolicy(0).Name());
    iter->Seek(key);
    if (iter->Valid() && iter->key() == Slice(key)) {
        ReadFilter(iter->value());
    }
    delete iter;
}

void Table::ReadFilter(const Slice& filter_handle_value) {
    Slice v = filter_handle_value;
    BlockHandle filter_handle;
    if (!filter_handle.DecodeFrom(&v).ok()) {
        return;
    }

    size_t n = static_cast<size_t>(filter_handle.size());
    char* buf = new char[n];

    {
        std::lock_guard<std::mutex> lock(rep_->file_mutex_);
        rep_->file_.clear();
        rep_->file_.seekg(filter_handle.offset());
        rep_->file_.read(buf, n);
        if (rep_->file_.fail()) {
            delete[] buf;
            return;
        }
    }

    rep_->filter_data = buf;
    rep_->filter_data_size = n;
    rep_->filter = new BloomFilterPolicy(rep_->options.bloom_bits_per_key);
}

Table::~Table() {
    delete rep_;
}

Iterator* Table::BlockReader(void* arg, const ReadOptions& options, const Slice& index_value) {
    Table* table = reinterpret_cast<Table*>(arg);
    BlockHandle handle;
    Slice input = index_value;
    Status s = handle.DecodeFrom(&input);
    
    if (!s.ok()) {
        return NewErrorIterator(s);
    }

    Block* block = nullptr;
    s = ReadBlockFromHandle(table->rep_->file_, table->rep_->file_mutex_,
                            options, handle, &block);
    if (s.ok()) {
        Iterator* iter = block->NewIterator(table->rep_->options.comparator);
        class BlockIterWrapper : public Iterator {
        public:
            BlockIterWrapper(Iterator* i, Block* b) : iter(i), block(b) {}
            ~BlockIterWrapper() { delete iter; delete block; }
            bool Valid() const override { return iter->Valid(); }
            void SeekToFirst() override { iter->SeekToFirst(); }
            void SeekToLast() override { iter->SeekToLast(); }
            void Seek(const Slice& target) override { iter->Seek(target); }
            void Next() override { iter->Next(); }
            void Prev() override { iter->Prev(); }
            Slice key() const override { return iter->key(); }
            Slice value() const override { return iter->value(); }
            Status status() const override { return iter->status(); }
        private:
            Iterator* iter;
            Block* block;
        };
        return new BlockIterWrapper(iter, block);
    }

    return NewErrorIterator(s);
}

class TwoLevelIterator : public Iterator {
public:
    TwoLevelIterator(
        Iterator* index_iter,
        Iterator* (*block_function)(void* arg, const ReadOptions& options, const Slice& index_value),
        void* arg,
        const ReadOptions& options)
        : index_iter_(index_iter),
          block_function_(block_function),
          arg_(arg),
          options_(options),
          data_iter_(nullptr) {}

    ~TwoLevelIterator() override {
        delete index_iter_;
        delete data_iter_;
    }

    void Seek(const Slice& target) override {
        index_iter_->Seek(target);
        InitDataBlock();
        if (data_iter_ != nullptr) data_iter_->Seek(target);
        SkipEmptyDataBlocksForward();
    }

    void SeekToFirst() override {
        index_iter_->SeekToFirst();
        InitDataBlock();
        if (data_iter_ != nullptr) data_iter_->SeekToFirst();
        SkipEmptyDataBlocksForward();
    }

    void SeekToLast() override {
        index_iter_->SeekToLast();
        InitDataBlock();
        if (data_iter_ != nullptr) data_iter_->SeekToLast();
        SkipEmptyDataBlocksBackward();
    }

    void Next() override {
        assert(Valid());
        data_iter_->Next();
        SkipEmptyDataBlocksForward();
    }

    void Prev() override {
        assert(Valid());
        data_iter_->Prev();
        SkipEmptyDataBlocksBackward();
    }

    bool Valid() const override {
        return data_iter_ != nullptr && data_iter_->Valid();
    }

    Slice key() const override {
        assert(Valid());
        return data_iter_->key();
    }

    Slice value() const override {
        assert(Valid());
        return data_iter_->value();
    }

    Status status() const override {
        if (!index_iter_->status().ok()) {
            return index_iter_->status();
        } else if (data_iter_ != nullptr && !data_iter_->status().ok()) {
            return data_iter_->status();
        } else {
            return status_;
        }
    }

private:
    void SaveError(const Status& s) {
        if (status_.ok() && !s.ok()) status_ = s;
    }

    void SkipEmptyDataBlocksForward() {
        while (data_iter_ == nullptr || !data_iter_->Valid()) {
            if (!index_iter_->Valid()) {
                SetDataIterator(nullptr);
                return;
            }
            index_iter_->Next();
            InitDataBlock();
            if (data_iter_ != nullptr) data_iter_->SeekToFirst();
        }
    }

    void SkipEmptyDataBlocksBackward() {
        while (data_iter_ == nullptr || !data_iter_->Valid()) {
            if (!index_iter_->Valid()) {
                SetDataIterator(nullptr);
                return;
            }
            index_iter_->Prev();
            InitDataBlock();
            if (data_iter_ != nullptr) data_iter_->SeekToLast();
        }
    }

    void SetDataIterator(Iterator* data_iter) {
        if (data_iter_ != nullptr) SaveError(data_iter_->status());
        delete data_iter_;
        data_iter_ = data_iter;
    }

    void InitDataBlock() {
        if (!index_iter_->Valid()) {
            SetDataIterator(nullptr);
            return;
        }
        Slice handle = index_iter_->value();
        if (data_iter_ != nullptr && handle.compare(data_block_handle_) == 0) {
        } else {
            Iterator* iter = (*block_function_)(arg_, options_, handle);
            data_block_handle_.assign(handle.data(), handle.size());
            SetDataIterator(iter);
        }
    }

    Iterator* index_iter_;
    Iterator* (*block_function_)(void*, const ReadOptions&, const Slice&);
    void* arg_;
    const ReadOptions options_;
    Iterator* data_iter_;
    std::string data_block_handle_;
    Status status_;
};

Iterator* Table::NewIterator(const ReadOptions& options) const {
    return new TwoLevelIterator(rep_->index_block->NewIterator(rep_->options.comparator),
                                &Table::BlockReader, const_cast<Table*>(this), options);
}

Iterator* NewTwoLevelIterator(Iterator* index_iter,
                              Iterator* (*block_function)(void* arg, const ReadOptions& options, const Slice& index_value),
                              void* arg,
                              const ReadOptions& options) {
    return new TwoLevelIterator(index_iter, block_function, arg, options);
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k,
                          void* arg, void (*handle_result)(void*, const Slice&, const Slice&)) {
    Status s;
    std::unique_ptr<Iterator> iiter(rep_->index_block->NewIterator(rep_->options.comparator));
    iiter->Seek(k);
    if (iiter->Valid()) {
        Slice handle_value = iiter->value();
        if (rep_->filter != nullptr) {
            Slice filter_key = (k.size() >= 8) ? InternalKey::ExtractUserKey(k) : k;
            if (!rep_->filter->KeyMayMatch(filter_key, Slice(rep_->filter_data, rep_->filter_data_size))) {
                return s;
            }
        }

        Iterator* block_iter = BlockReader(this, options, handle_value);
        block_iter->Seek(k);
        if (block_iter->Valid()) {
            (*handle_result)(arg, block_iter->key(), block_iter->value());
        }
        s = block_iter->status();
        delete block_iter;
    }
    if (s.ok()) {
        s = iiter->status();
    }
    return s;
}

bool Table::MayContain(const Slice& user_key) const {
    if (rep_->filter == nullptr) {
        return true;
    }
    return rep_->filter->KeyMayMatch(user_key, Slice(rep_->filter_data, rep_->filter_data_size));
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
    std::unique_ptr<Iterator> index_iter(rep_->index_block->NewIterator(rep_->options.comparator));
    index_iter->Seek(key);
    if (index_iter->Valid()) {
        BlockHandle handle;
        Slice input = index_iter->value();
        Status s = handle.DecodeFrom(&input);
        if (s.ok()) {
            return handle.offset();
        }
    }
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    return rep_->metaindex_handle.offset();
}

}
