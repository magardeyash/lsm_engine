#pragma once

#include <string>

namespace lsm {

class Slice;

// Provides a total order across Slices used as keys. Must be thread-safe.
class Comparator {
public:
    virtual ~Comparator() = default;

    // Three-way comparison: <0 if a<b, 0 if a==b, >0 if a>b.
    virtual int Compare(const Slice& a, const Slice& b) const = 0;

    // Name used to detect comparator mismatches across DB opens.
    // Change when key ordering changes. Names starting with "lsm." are reserved.
    virtual const char* Name() const = 0;

    // If *start < limit, changes *start to a short string in [start, limit).
    // A no-op implementation is valid.
    virtual void FindShortestSeparator(
        std::string* start,
        const Slice& limit) const = 0;

    // Changes *key to a short string >= *key. A no-op implementation is valid.
    virtual void FindShortSuccessor(std::string* key) const = 0;
};

// Returns the builtin lexicographic byte-wise comparator. Do not delete.
const Comparator* BytewiseComparator();

}
