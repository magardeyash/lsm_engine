#include "src/table/table_cache.h"
#include "src/util/coding.h"
#include "lsm/db.h"
#include <memory>

namespace lsm {

// Ownership model:
// - Each cached Table is wrapped in a std::shared_ptr<Table>.
// - TableAndFile holds the shared_ptr.  When the LRU cache evicts an entry,
//   DeleteEntry releases the shared_ptr.  If no iterators hold a copy,
//   the Table is destroyed.  If iterators still hold a copy, the Table
//   stays alive until the last iterator is destroyed.
struct TableAndFile {
    std::shared_ptr<Table> table;
};

static void DeleteEntry(const Slice& key, void* value) {
    TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
    delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
    Cache* cache = reinterpret_cast<Cache*>(arg1);
    Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
    cache->Release(h);
}

TableCache::TableCache(const std::string& dbname, const Options* options, int entries)
    : dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {
}

TableCache::~TableCache() {
    delete cache_;
}

// Convert file number into a string for cache key
static std::string TableFileName(const std::string& dbname, uint64_t number) {
    char buf[100];
    snprintf(buf, sizeof(buf), "%s/%06llu.sst", dbname.c_str(),
             static_cast<unsigned long long>(number));
    return std::string(buf);
}

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle** handle) {
    Status s;
    char buf[sizeof(file_number)];
    EncodeFixed64(buf, file_number);
    Slice key(buf, sizeof(buf));
    *handle = cache_->Lookup(key);
    if (*handle == nullptr) {
        std::string fname = TableFileName(dbname_, file_number);
        Table* raw_table = nullptr;
        s = Table::Open(*options_, fname, file_size, &raw_table);
        if (s.ok()) {
            TableAndFile* tf = new TableAndFile;
            tf->table.reset(raw_table);
            *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
        }
    }
    return s;
}

static Iterator* GetTableIterator(void* table, const ReadOptions& options) {
    return reinterpret_cast<Table*>(table)->NewIterator(options);
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr) {
    if (tableptr != nullptr) {
        *tableptr = nullptr;
    }

    Cache::Handle* handle = nullptr;
    Status s = FindTable(file_number, file_size, &handle);
    if (!s.ok()) {
        return NewErrorIterator(s);
    }

    Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table.get();
    // Capture a shared_ptr copy so the Table stays alive even if evicted
    // from the cache while this iterator is in use.
    std::shared_ptr<Table> table_ref = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    Iterator* result = table->NewIterator(options);

    // Wrap the iterator to release the cache handle and hold a shared_ptr
    // reference to the Table, ensuring it outlives the iterator.
    class TableCacheIteratorWrapper : public Iterator {
    public:
        TableCacheIteratorWrapper(Iterator* iter, Cache* cache, Cache::Handle* handle,
                                  std::shared_ptr<Table> table_ref)
            : iter_(iter), cache_(cache), handle_(handle), table_ref_(std::move(table_ref)) {}
        ~TableCacheIteratorWrapper() override {
            delete iter_;
            cache_->Release(handle_);
        }
        bool Valid() const override { return iter_->Valid(); }
        void SeekToFirst() override { iter_->SeekToFirst(); }
        void SeekToLast() override { iter_->SeekToLast(); }
        void Seek(const Slice& target) override { iter_->Seek(target); }
        void Next() override { iter_->Next(); }
        void Prev() override { iter_->Prev(); }
        Slice key() const override { return iter_->key(); }
        Slice value() const override { return iter_->value(); }
        Status status() const override { return iter_->status(); }
    private:
        Iterator* iter_;
        Cache* cache_;
        Cache::Handle* handle_;
        std::shared_ptr<Table> table_ref_;  // Prevents Table destruction
    };
    
    Iterator* wrapped = new TableCacheIteratorWrapper(result, cache_, handle, table_ref);
    
    if (tableptr != nullptr) {
        *tableptr = table;
    }
    return wrapped;
}

Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,
                       const Slice& k,
                       void* arg,
                       void (*handle_result)(void*, const Slice&, const Slice&)) {
    Cache::Handle* handle = nullptr;
    Status s = FindTable(file_number, file_size, &handle);
    if (s.ok()) {
        Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table.get();
        // The InternalGet is protected by friendship or public exposure
        // We'll expose InternalGet or use NewIterator. Table::InternalGet is better for speed.
        s = t->InternalGet(options, k, arg, handle_result);
        cache_->Release(handle);
    }
    return s;
}

void TableCache::Evict(uint64_t file_number) {
    char buf[sizeof(file_number)];
    EncodeFixed64(buf, file_number);
    cache_->Erase(Slice(buf, sizeof(buf)));
}

bool TableCache::MayContain(uint64_t file_number, uint64_t file_size,
                            const Slice& user_key) {
    Cache::Handle* handle = nullptr;
    Status s = FindTable(file_number, file_size, &handle);
    if (!s.ok()) {
        return true;  // Conservative: assume key may be present on error
    }
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table.get();
    bool result = t->MayContain(user_key);
    cache_->Release(handle);
    return result;
}

}  // namespace lsm
