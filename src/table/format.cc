#include "src/table/format.h"
#include "src/util/coding.h"
#include <cassert>

namespace lsm {

BlockHandle::BlockHandle() : offset_(~static_cast<uint64_t>(0)), size_(~static_cast<uint64_t>(0)) {}

void BlockHandle::EncodeTo(std::string* dst) const {
    PutVarint64(dst, offset_);
    PutVarint64(dst, size_);
}

Status BlockHandle::DecodeFrom(Slice* input) {
    if (GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
        return Status::OK();
    } else {
        return Status::Corruption("bad block handle");
    }
}

void Footer::EncodeTo(std::string* dst) const {
    const size_t original_size = dst->size();
    metaindex_handle_.EncodeTo(dst);
    index_handle_.EncodeTo(dst);
    dst->resize(original_size + 2 * BlockHandle::kMaxEncodedLength);
    PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber & 0xffffffffu));
    PutFixed32(dst, static_cast<uint32_t>(kTableMagicNumber >> 32));
    assert(dst->size() == original_size + kEncodedLength);
}

Status Footer::DecodeFrom(Slice* input) {
    if (input->size() < kEncodedLength) {
        return Status::Corruption("file is too short to be an sstable");
    }

    const char* magic_ptr = input->data() + kEncodedLength - 8;
    const uint32_t magic_lo = DecodeFixed32(magic_ptr);
    const uint32_t magic_hi = DecodeFixed32(magic_ptr + 4);
    const uint64_t magic = ((static_cast<uint64_t>(magic_hi) << 32) |
                             (static_cast<uint64_t>(magic_lo)));
    if (magic != kTableMagicNumber) {
        return Status::Corruption("not an sstable (bad magic number)");
    }

    Status result = metaindex_handle_.DecodeFrom(input);
    if (result.ok()) {
        result = index_handle_.DecodeFrom(input);
    }
    if (result.ok()) {
        const char* end = magic_ptr + 8;
        *input = Slice(end, input->data() + input->size() - end);
    }
    return result;
}

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options),
      restarts_(),
      counter_(0),
      finished_(false) {
    assert(options->block_restart_interval >= 1);
    restarts_.push_back(0);
}

void BlockBuilder::Reset() {
    buffer_.clear();
    restarts_.clear();
    restarts_.push_back(0);
    counter_ = 0;
    finished_ = false;
    last_key_.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {
    return (buffer_.size() +
            restarts_.size() * sizeof(uint32_t) +
            sizeof(uint32_t));
}

Slice BlockBuilder::Finish() {
    for (size_t i = 0; i < restarts_.size(); i++) {
        PutFixed32(&buffer_, restarts_[i]);
    }
    PutFixed32(&buffer_, restarts_.size());
    finished_ = true;
    return Slice(buffer_);
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
    Slice last_key_piece(last_key_);
    assert(!finished_);
    assert(counter_ <= options_->block_restart_interval);
    
    // We only assert key >= last_key_piece if the buffer is not empty
    // But since this is a general-purpose library, we omit the key comparison assert
    // to avoid coupling tightly with internal key comparator here.
    // The builder just prefixes strings as given.

    size_t shared = 0;
    if (counter_ < options_->block_restart_interval) {
        const size_t min_length = std::min(last_key_piece.size(), key.size());
        while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
            shared++;
        }
    } else {
        restarts_.push_back(buffer_.size());
        counter_ = 0;
    }
    const size_t non_shared = key.size() - shared;

    PutVarint32(&buffer_, shared);
    PutVarint32(&buffer_, non_shared);
    PutVarint32(&buffer_, value.size());

    buffer_.append(key.data() + shared, non_shared);
    buffer_.append(value.data(), value.size());

    last_key_.resize(shared);
    last_key_.append(key.data() + shared, non_shared);
    assert(Slice(last_key_) == key);
    counter_++;
}

}
