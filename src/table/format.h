#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include "lsm/slice.h"
#include "lsm/status.h"
#include "lsm/options.h"

namespace lsm {

// BlockHandle is a pointer to the extent of a file that stores a data
// block or a meta block.
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

// Footer encapsulates the fixed information stored at the tail
// end of every table file.
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

    // Encoded length of a Footer.  Note that the serialization of a
    // Footer will always occupy exactly this many bytes.  It consists
    // of two block handles and a magic number.
    enum { kEncodedLength = 2 * BlockHandle::kMaxEncodedLength + 8 };

private:
    BlockHandle metaindex_handle_;
    BlockHandle index_handle_;
};

// kTableMagicNumber was picked by running
//    echo http://code.google.com/p/leveldb/ | sha1sum
// and taking the leading 64 bits.
static const uint64_t kTableMagicNumber = 0xdb4775248b80fb57ull;

// 1-byte type + 32-bit crc
static const size_t kBlockTrailerSize = 5;

// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.

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

    // Return true iff no entries have been added since the last Reset()
    bool empty() const {
        return buffer_.empty();
    }

private:
    const Options* options_;
    std::string buffer_;              // Destination buffer
    std::vector<uint32_t> restarts_;  // Restart points
    int counter_;                   // Number of entries emitted since restart
    bool finished_;                 // Has Finish() been called?
    std::string last_key_;
};

}  // namespace lsm
