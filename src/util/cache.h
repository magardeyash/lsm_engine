#pragma once

#include <cstdint>
#include <memory>
#include "lsm/slice.h"

namespace lsm {

class Cache;

Cache* NewLRUCache(size_t capacity);

class Cache {
public:
    Cache() = default;

    virtual ~Cache() = default;

    Cache(const Cache&) = delete;
    Cache& operator=(const Cache&) = delete;

    // Opaque handle to an entry stored in the cache.
    struct Handle {};

    // Inserts key->value with given charge. Caller must Release() the handle.
    // The deleter is called when the entry is evicted.
    virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                           void (*deleter)(const Slice& key, void* value)) = 0;

    // Returns a handle for the key, or nullptr. Caller must Release() when done.
    virtual Handle* Lookup(const Slice& key) = 0;

    // REQUIRES: handle not yet released; returned by a method on *this.
    virtual void Release(Handle* handle) = 0;

    // REQUIRES: handle not yet released; returned by a method on *this.
    virtual void* Value(Handle* handle) = 0;

    // Erases key; underlying entry kept alive until all handles released.
    virtual void Erase(const Slice& key) = 0;

    // Returns a new numeric id for partitioning shared cache key spaces.
    virtual uint64_t NewId() = 0;

    // Removes all unused entries. Default implementation is a no-op.
    virtual void Prune() {}

    virtual size_t TotalCharge() const = 0;
};

}
