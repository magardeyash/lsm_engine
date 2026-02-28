#pragma once

#include <cstdint>
#include <string>
#include <memory>
#include "lsm/options.h"
#include "lsm/status.h"
#include "lsm/iterator.h"

namespace lsm {

class Block;
class BlockHandle;
class Footer;

// Immutable persistent sorted map. Thread-safe.
class Table {
public:
    // Opens table from file. On success sets *table (caller must delete).
    // Returns non-ok status and nullptr on failure. *file must outlive Table.
    static Status Open(const Options& options,
                       const std::string& filename,
                       uint64_t file_size,
                       Table** table);

    Table(const Table&) = delete;
    Table& operator=(const Table&) = delete;

    ~Table();

    // Returns a new iterator. Must Seek before use.
    Iterator* NewIterator(const ReadOptions& options) const;

    bool MayContain(const Slice& user_key) const;

    // Approximate file byte offset of the data for key.
    uint64_t ApproximateOffsetOf(const Slice& key) const;

private:
    struct Rep;
    Rep* rep_;

    explicit Table(Rep* rep) : rep_(rep) {}

    static Iterator* BlockReader(void*, const ReadOptions&, const Slice&);
    
    friend class TableCache;
    Status InternalGet(const ReadOptions&, const Slice& key,
                       void* arg,
                       void (*handle_result)(void* arg, const Slice& k, const Slice& v));

    void ReadMeta(const Footer& footer);
    void ReadFilter(const Slice& filter_handle_value);
};

Iterator* NewTwoLevelIterator(Iterator* index_iter,
                              Iterator* (*block_function)(void* arg, const ReadOptions& options, const Slice& index_value),
                              void* arg,
                              const ReadOptions& options);

}
