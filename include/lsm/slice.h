#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <cstring>

namespace lsm {

// Slice is a simple structure containing a pointer into some external storage
// and a size. The user of a Slice must ensure that the slice is not used after
// the corresponding external storage has been deallocated.
class Slice {
public:
    // Create an empty slice.
    Slice() : data_(""), size_(0) {}

    // Create a slice that refers to d[0,n-1].
    Slice(const char* d, size_t n) : data_(d), size_(n) {}

    // Create a slice that refers to the contents of "s"
    Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}

    // Create a slice that refers to s[0,strlen(s)-1]
    Slice(const char* s) : data_(s), size_(std::strlen(s)) {}

    // Create a slice from std::string_view
    Slice(std::string_view sv) : data_(sv.data()), size_(sv.size()) {}

    // Return a pointer to the beginning of the referenced data
    const char* data() const { return data_; }

    // Return the length (in bytes) of the referenced data
    size_t size() const { return size_; }

    // Return true iff the length of the referenced data is zero
    bool empty() const { return size_ == 0; }

    // Return the ith byte in the referenced data.
    // REQUIRES: n < size()
    char operator[](size_t n) const { return data_[n]; }

    // Change this slice to refer to an empty array
    void clear() {
        data_ = "";
        size_ = 0;
    }

    // Drop the first "n" bytes from this slice.
    void remove_prefix(size_t n) {
        data_ += n;
        size_ -= n;
    }

    // Return a string that contains the copy of the referenced data.
    std::string ToString() const { return std::string(data_, size_); }
    
    // Explicit conversion to std::string_view
    explicit operator std::string_view() const {
        return std::string_view(data_, size_);
    }

    // Three-way comparison.  Returns value:
    //   <  0 iff "*this" <  "b",
    //   == 0 iff "*this" == "b",
    //   >  0 iff "*this" >  "b"
    int compare(const Slice& b) const {
        const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
        int r = std::memcmp(data_, b.data_, min_len);
        if (r == 0) {
            if (size_ < b.size_) r = -1;
            else if (size_ > b.size_) r = +1;
        }
        return r;
    }

    // Return true iff "x" is a prefix of "*this"
    bool starts_with(const Slice& x) const {
        return ((size_ >= x.size_) && (std::memcmp(data_, x.data_, x.size_) == 0));
    }

private:
    const char* data_;
    size_t size_;
};

inline bool operator==(const Slice& x, const Slice& y) {
    return ((x.size() == y.size()) &&
            (std::memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const Slice& x, const Slice& y) {
    return !(x == y);
}

inline bool operator<(const Slice& x, const Slice& y) {
    return x.compare(y) < 0;
}

inline bool operator>(const Slice& x, const Slice& y) {
    return x.compare(y) > 0;
}

inline bool operator<=(const Slice& x, const Slice& y) {
    return x.compare(y) <= 0;
}

inline bool operator>=(const Slice& x, const Slice& y) {
    return x.compare(y) >= 0;
}

} // namespace lsm
