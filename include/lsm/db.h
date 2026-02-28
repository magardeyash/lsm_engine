#pragma once

#include <string>
#include "options.h"
#include "iterator.h"
#include "status.h"

namespace lsm {

// A persistent ordered map from keys to values. Thread-safe.
class DB {
public:
    // Opens the database. On success stores heap-allocated DB in *dbptr.
    // Caller owns *dbptr and must delete it when done.
    static Status Open(const Options& options,
                       const std::string& name,
                       DB** dbptr);

    DB() = default;
    DB(const DB&) = delete;
    DB& operator=(const DB&) = delete;

    virtual ~DB() = default;

    virtual Status Put(const WriteOptions& options,
                       const Slice& key,
                       const Slice& value) = 0;

    // Not an error if "key" does not exist.
    virtual Status Delete(const WriteOptions& options, const Slice& key) = 0;

    // Returns IsNotFound() status if key does not exist.
    virtual Status Get(const ReadOptions& options,
                       const Slice& key, std::string* value) = 0;

    // Returns a heap-allocated iterator. Must Seek before use.
    // Delete the iterator before deleting the DB.
    virtual Iterator* NewIterator(const ReadOptions& options) = 0;
};

Status DestroyDB(const std::string& name, const Options& options);

}
