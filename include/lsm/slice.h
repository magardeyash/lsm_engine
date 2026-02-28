#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <cstring>

namespace lsm {

// Non-owning reference to external data. Data must outlive the Slice.
class Slice {
public:
    Slice() : data_(""), size_(0) {}

    Slice(const char* d, size_t n) : data_(d), size_(n) {}

    Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}

    Slice(const char* s) : data_(s), size_(std::strlen(s)) {}

    Slice(std::string_view sv) : data_(sv.data()), size_(sv.size()) {}

    const char* data() const { return data_; }

    size_t size() const { return size_; }

    bool empty() const { return size_ == 0; }

    char operator[](size_t n) const { return data_[n]; }

    void clear() {
        data_ = "";
        size_ = 0;
    }

    void remove_prefix(size_t n) {
        data_ += n;
        size_ -= n;
    }

    std::string ToString() const { return std::string(data_, size_); }
    
    explicit operator std::string_view() const {
        return std::string_view(data_, size_);
    }

    // Three-way comparison: <0, 0, or >0.
    int compare(const Slice& b) const {
        const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
        int r = std::memcmp(data_, b.data_, min_len);
        if (r == 0) {
            if (size_ < b.size_) r = -1;
            else if (size_ > b.size_) r = +1;
        }
        return r;
    }

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

}
