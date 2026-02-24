#pragma once

#include <vector>
#include <string>
#include "lsm/slice.h"

namespace lsm {

// A bloom filter policy class that defines how to create and read bloom filters
class BloomFilterPolicy {
public:
    explicit BloomFilterPolicy(int bits_per_key);
    ~BloomFilterPolicy() = default;

    // Return the name of this policy.
    const char* Name() const;

    // keys[0,n-1] contains a list of keys (potentially with duplicates)
    // that are to be added to the filter.
    //
    // Append a filter that summarizes keys[0,n-1] to *dst.
    //
    // Warning: do not change the initial contents of *dst.  Instead,
    // append the newly constructed filter to *dst.
    void CreateFilter(const Slice* keys, int n, std::string* dst) const;

    // "filter" contains the data appended by a preceding call to
    // CreateFilter() on this class.  This method must return true if
    // the key was in the list of keys passed to CreateFilter().
    // This method may return true or false if the key was not on the
    // list, but it should aim to return false with a high probability.
    bool KeyMayMatch(const Slice& key, const Slice& filter) const;

private:
    size_t bits_per_key_;
    size_t k_;
};

}  // namespace lsm
