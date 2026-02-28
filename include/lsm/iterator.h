#pragma once

#include "slice.h"
#include "status.h"

namespace lsm {

class Iterator {
public:
    Iterator() {}
    virtual ~Iterator() {}

    virtual bool Valid() const = 0;

    virtual void SeekToFirst() = 0;

    virtual void SeekToLast() = 0;

    // Position at the first key >= target.
    virtual void Seek(const Slice& target) = 0;

    virtual void Next() = 0;

    virtual void Prev() = 0;

    // REQUIRES: Valid(). Returned Slice is valid until next modification.
    virtual Slice key() const = 0;
    virtual Slice value() const = 0;

    virtual Status status() const = 0;

private:
    Iterator(const Iterator&) = delete;
    Iterator& operator=(const Iterator&) = delete;
};

Iterator* NewEmptyIterator();

Iterator* NewErrorIterator(const Status& status);

}
