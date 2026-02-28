#pragma once

#include <cstdint>
#include <string>
#include <fstream>
#include "lsm/options.h"
#include "lsm/status.h"

namespace lsm {

class BlockBuilder;
class BlockHandle;

// Builds an immutable sorted SSTable from key-value pairs. Thread-safe for const methods.
class TableBuilder {
public:
    // Caller must close *file after Finish() returns.
    TableBuilder(const Options& options, std::ofstream* file);

    TableBuilder(const TableBuilder&) = delete;
    TableBuilder& operator=(const TableBuilder&) = delete;

    // REQUIRES: Either Finish() or Abandon() has been called.
    ~TableBuilder();

    Status ChangeOptions(const Options& options);

    // REQUIRES: key > any previously added key; Finish()/Abandon() not called.
    void Add(const Slice& key, const Slice& value);

    void Flush();

    Status status() const;

    // REQUIRES: Finish()/Abandon() not yet called.
    Status Finish();

    // Must call Abandon() if not calling Finish().
    void Abandon();

    uint64_t NumEntries() const;

    uint64_t FileSize() const;

private:
    bool ok() const { return status().ok(); }
    void WriteBlock(BlockBuilder* block, BlockHandle* handle);
    void WriteRawBlock(const Slice& data, Options::CompressionType type, BlockHandle* handle);

    struct Rep;
    Rep* rep_;
};

}
