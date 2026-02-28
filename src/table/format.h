#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include "lsm/slice.h"
#include "lsm/status.h"
#include "lsm/options.h"

namespace lsm {

// Points to a data block or meta block extent within a file.
class BlockHandle {
public:
    BlockHandle();

    // The offset of the block in the file.
    uint64_t offset() const { return offset_; }
    void set_offset(uint64_t offset) { offset_ = offset; }

    // The size of the stored block
    uint64_t size() const { return size_; }
    void set_size(uint64_t size) { size_ = size; }

    void EncodeTo(std::string* dst) const;
    Status DecodeFrom(Slice* input);

    // Maximum encoding length of a BlockHandle
    enum { kMaxEncodedLength = 10 + 10 };

private:
    uint64_t offset_;
    uint64_t size_;
};

// Encapsulates the fixed-size footer at the tail of every table file.
class Footer {
public:
    Footer() = default;

    // The block handle for the metaindex block of the table
    const BlockHandle& metaindex_handle() const { return metaindex_handle_; }
    void set_metaindex_handle(const BlockHandle& h) { metaindex_handle_ = h; }

    // The block handle for the index block of the table
    const BlockHandle& index_handle() const { return index_handle_; }
    void set_index_handle(const BlockHandle& h) {
        index_handle_ = h;
    }

    void EncodeTo(std::string* dst) const;
    Status DecodeFrom(Slice* input);

    // Encoded footer size: 2 block handles + 8-byte magic number.
    enum { kEncodedLength = 2 * BlockHandle::kMaxEncodedLength + 8 };

private:
    BlockHandle metaindex_handle_;
    BlockHandle index_handle_;
};

static const uint64_t kTableMagicNumber = 0xdb4775248b80fb57ull;

static const size_t kBlockTrailerSize = 5;

// Builds blocks with prefix-compressed keys and restart points for binary search.

class BlockBuilder {
public:
    explicit BlockBuilder(const Options* options);

    BlockBuilder(const BlockBuilder&) = delete;
    BlockBuilder& operator=(const BlockBuilder&) = delete;

    // Reset the contents as if the BlockBuilder was just constructed.
    void Reset();

    // REQUIRES: Finish() has not been called since the last call to Reset().
    // REQUIRES: key is larger than any previously added key
    void Add(const Slice& key, const Slice& value);

    // Finish building the block and return a slice that refers to the
    // block contents.  The returned slice will remain valid for the
    // life of this builder or until Reset() is called.
    Slice Finish();

    // Returns an estimate of the current (uncompressed) size of the block
    // we are building.
    size_t CurrentSizeEstimate() const;

    bool empty() const {
        return buffer_.empty();
    }

private:
    const Options* options_;
    std::string buffer_;
    std::vector<uint32_t> restarts_;
    int counter_;
    bool finished_;
    std::string last_key_;
};

}
