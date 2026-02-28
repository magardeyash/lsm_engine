#include "src/table/sstable_builder.h"
#include <cassert>
#include "src/table/format.h"
#include "src/db/memtable.h"
#include "lsm/comparator.h"
#include "lsm/options.h"
#include "src/util/coding.h"
#include "src/util/crc32.h"
#include "src/util/bloom.h"

#ifdef LSM_HAVE_ZSTD
#include <zstd.h>
#endif

namespace lsm {

struct TableBuilder::Rep {
    Options options;
    Options index_block_options;
    std::ofstream* file;
    uint64_t offset;
    Status status;
    BlockBuilder data_block;
    BlockBuilder index_block;
    std::string last_key;
    int64_t num_entries;
    bool closed;
    
    // We do not implement the FilterBlockBuilder in full generality
    // for this simplified engine - we'll just gather all keys in memory
    // and build a single bloom filter for the whole table.
    std::string filter_data;
    std::vector<uint32_t> filter_offsets;
    std::vector<std::string> keys;
    
    bool pending_index_entry;
    BlockHandle pending_handle;

    std::string compressed_output;

    Rep(const Options& opt, std::ofstream* f)
        : options(opt),
          index_block_options(opt),
          file(f),
          offset(0),
          data_block(&options),
          index_block(&index_block_options),
          num_entries(0),
          closed(false),
          pending_index_entry(false) {
        index_block_options.block_restart_interval = 1;
    }
};

TableBuilder::TableBuilder(const Options& options, std::ofstream* file)
    : rep_(new Rep(options, file)) {
}

TableBuilder::~TableBuilder() {
    assert(rep_->closed);
    delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
    if (options.comparator != rep_->options.comparator) {
        return Status::InvalidArgument("changing comparator while building table");
    }
    rep_->options = options;
    rep_->index_block_options = options;
    rep_->index_block_options.block_restart_interval = 1;
    return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
    Rep* r = rep_;
    assert(!r->closed);
    if (!ok()) return;
    if (r->num_entries > 0) {
        assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
    }

    if (r->pending_index_entry) {
        assert(r->data_block.empty());
        r->options.comparator->FindShortestSeparator(&r->last_key, key);
        std::string handle_encoding;
        r->pending_handle.EncodeTo(&handle_encoding);
        r->index_block.Add(r->last_key, Slice(handle_encoding));
        r->pending_index_entry = false;
    }

    if (r->options.bloom_bits_per_key > 0) {
        if (key.size() >= 8) {
            r->keys.push_back(InternalKey::ExtractUserKey(key).ToString());
        } else {
            r->keys.push_back(key.ToString());
        }
    }

    r->last_key.assign(key.data(), key.size());
    r->num_entries++;
    r->data_block.Add(key, value);

    const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
    if (estimated_block_size >= r->options.block_size) {
        Flush();
    }
}

void TableBuilder::Flush() {
    Rep* r = rep_;
    assert(!r->closed);
    if (!ok()) return;
    if (r->data_block.empty()) return;
    assert(!r->pending_index_entry);
    WriteBlock(&r->data_block, &r->pending_handle);
    if (ok()) {
        r->pending_index_entry = true;
        r->file->flush();
    }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
    // File format contains a sequence of blocks where each block has:
    //    block_data: uint8[n]
    //    type: uint8
    //    crc: uint32
    assert(ok());
    Rep* r = rep_;
    Slice raw = block->Finish();

    Slice block_contents;
    Options::CompressionType type = r->options.compression;

    switch (type) {
        case Options::kNoCompression:
            block_contents = raw;
            break;

        case Options::kZstdCompression: {
#ifdef LSM_HAVE_ZSTD
            size_t max_compressed_size = ZSTD_compressBound(raw.size());
            r->compressed_output.resize(max_compressed_size);
            size_t compressed_size = ZSTD_compress(
                &r->compressed_output[0], max_compressed_size,
                raw.data(), raw.size(), 1 /* default level */);
            if (ZSTD_isError(compressed_size)) {
                block_contents = raw;
                type = Options::kNoCompression;
            } else {
                r->compressed_output.resize(compressed_size);
                block_contents = Slice(r->compressed_output);
            }
#else
            block_contents = raw;
            type = Options::kNoCompression;
#endif
            break;
        }
    }
    WriteRawBlock(block_contents, type, handle);
    r->compressed_output.clear();
    block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 Options::CompressionType type,
                                 BlockHandle* handle) {
    Rep* r = rep_;
    handle->set_offset(r->offset);
    handle->set_size(block_contents.size());
    r->file->write(block_contents.data(), block_contents.size());
    
    if (r->file->fail()) {
        r->status = Status::IOError("Failed to write raw block");
        return;
    }

    char trailer[kBlockTrailerSize];
    trailer[0] = static_cast<char>(type);
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    
    r->file->write(trailer, kBlockTrailerSize);
    
    if (r->file->fail()) {
        r->status = Status::IOError("Failed to write block trailer");
        return;
    }

    r->offset += block_contents.size() + kBlockTrailerSize;
}

Status TableBuilder::status() const {
    return rep_->status;
}

Status TableBuilder::Finish() {
    Rep* r = rep_;
    Flush();
    assert(!r->closed);
    r->closed = true;

    BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

    // Write bloom filter block
    if (ok() && r->options.bloom_bits_per_key > 0) {
        BloomFilterPolicy policy(r->options.bloom_bits_per_key);
        std::vector<Slice> slice_keys;
        slice_keys.reserve(r->keys.size());
        for (const auto& k : r->keys) {
            slice_keys.push_back(Slice(k));
        }

        std::string filter_content;
        policy.CreateFilter(slice_keys.data(), slice_keys.size(), &filter_content);
        
        WriteRawBlock(Slice(filter_content), Options::kNoCompression,
                      &filter_block_handle);
    }

    // Write metaindex block
    if (ok()) {
        BlockBuilder meta_index_block(&r->options);
        if (r->options.bloom_bits_per_key > 0) {
            std::string key = "filter.";
            key.append((BloomFilterPolicy(0)).Name());
            std::string handle_encoding;
            filter_block_handle.EncodeTo(&handle_encoding);
            meta_index_block.Add(key, handle_encoding);
        }
        WriteBlock(&meta_index_block, &metaindex_block_handle);
    }

    // Write index block
    if (ok()) {
        if (r->pending_index_entry) {
            r->options.comparator->FindShortSuccessor(&r->last_key);
            std::string handle_encoding;
            r->pending_handle.EncodeTo(&handle_encoding);
            r->index_block.Add(r->last_key, Slice(handle_encoding));
            r->pending_index_entry = false;
        }
        WriteBlock(&r->index_block, &index_block_handle);
    }

    if (ok()) {
        Footer footer;
        footer.set_metaindex_handle(metaindex_block_handle);
        footer.set_index_handle(index_block_handle);
        std::string footer_encoding;
        footer.EncodeTo(&footer_encoding);
        r->file->write(footer_encoding.data(), footer_encoding.size());
        if (r->file->fail()) {
            r->status = Status::IOError("Failed to write footer");
        }
        r->offset += footer_encoding.size();
    }
    return r->status;
}

void TableBuilder::Abandon() {
    Rep* r = rep_;
    assert(!r->closed);
    r->closed = true;
}

uint64_t TableBuilder::NumEntries() const {
    return rep_->num_entries;
}

uint64_t TableBuilder::FileSize() const {
    return rep_->offset;
}

}
