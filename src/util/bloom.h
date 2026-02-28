#pragma once

#include <vector>
#include <string>
#include "lsm/slice.h"

namespace lsm {

class BloomFilterPolicy {
public:
    explicit BloomFilterPolicy(int bits_per_key);
    ~BloomFilterPolicy() = default;

    const char* Name() const;

    // Appends a filter for keys[0,n-1] to *dst.
    void CreateFilter(const Slice* keys, int n, std::string* dst) const;

    // Returns true if key was probably in the set passed to CreateFilter().
    bool KeyMayMatch(const Slice& key, const Slice& filter) const;

private:
    size_t bits_per_key_;
    size_t k_;
};

}
