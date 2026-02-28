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

    // Returns iterator for file. If tableptr is non-null, sets *tableptr
    // to the underlying Table (cache-owned, valid as long as iterator is live).
    Iterator* NewIterator(const ReadOptions& options,
                          uint64_t file_number,
                          uint64_t file_size,
                          Table** tableptr = nullptr);

    Status Get(const ReadOptions& options,
               uint64_t file_number,
               uint64_t file_size,
               const Slice& k,
               void* arg,
               void (*handle_result)(void*, const Slice&, const Slice&));

    void Evict(uint64_t file_number);

    bool MayContain(uint64_t file_number, uint64_t file_size,
                    const Slice& user_key);

private:
    Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle** handle);

    const std::string dbname_;
    const Options* const options_;
    Cache* cache_;
};

}
