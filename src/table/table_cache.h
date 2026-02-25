#pragma once

#include <string>
#include <stdint.h>
#include "lsm/db.h"
#include "src/util/cache.h"
#include "lsm/options.h"
#include "src/table/sstable_reader.h"

namespace lsm {

class TableCache {
public:
    TableCache(const std::string& dbname, const Options* options, int entries);
    ~TableCache();

    TableCache(const TableCache&) = delete;
    TableCache& operator=(const TableCache&) = delete;

    // Return an iterator for the specified file number (the corresponding
    // file length must be exactly "file_size" bytes).  If "tableptr" is
    // non-nullptr, also sets "*tableptr" to point to the Table object
    // underlying the returned iterator, or to nullptr if no Table object
    // underlies the returned iterator.  The returned "*tableptr" object is owned
    // by the cache and should not be deleted, and is valid for as long as the
    // returned iterator is live.
    Iterator* NewIterator(const ReadOptions& options,
                          uint64_t file_number,
                          uint64_t file_size,
                          Table** tableptr = nullptr);

    // If a seek to internal key "k" in specified file finds an entry,
    // call (*handle_result)(arg, found_key, found_value).
    Status Get(const ReadOptions& options,
               uint64_t file_number,
               uint64_t file_size,
               const Slice& k,
               void* arg,
               void (*handle_result)(void*, const Slice&, const Slice&));

    // Evict any entry for the specified file number
    void Evict(uint64_t file_number);

    // Query the Bloom filter of the specified file.  Returns true if the
    // key might be present (conservative if no filter exists).
    bool MayContain(uint64_t file_number, uint64_t file_size,
                    const Slice& user_key);

private:
    Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle** handle);

    const std::string dbname_;
    const Options* const options_;
    Cache* cache_;
};

}  // namespace lsm
