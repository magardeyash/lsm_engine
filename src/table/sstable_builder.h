#pragma once

#include <cstdint>
#include <string>
#include <fstream>
#include "lsm/options.h"
#include "lsm/status.h"

namespace lsm {

class BlockBuilder;
class BlockHandle;

// TableBuilder provides the interface used to build a Table
// (an immutable and sorted map from keys to values).
//
// Multiple threads can invoke const methods on a TableBuilder without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same TableBuilder must use
// external synchronization.
class TableBuilder {
public:
    // Create a builder that will store the contents of the table it is
    // building in *file.  Does not close the file.  It is up to the
    // caller to close the file after calling Finish().
    TableBuilder(const Options& options, std::ofstream* file);

    TableBuilder(const TableBuilder&) = delete;
    TableBuilder& operator=(const TableBuilder&) = delete;

    // REQUIRES: Either Finish() or Abandon() has been called.
    ~TableBuilder();

    // Change the options used by this builder.  Note: only some of the
    // option fields can be changed after construction.  If a field is
    // specified here that conflicts with the construction options AND
    // cannot be changed dynamically, this method will return an error
    // without changing any fields.
    Status ChangeOptions(const Options& options);

    // Add key,value to the table being constructed.
    // REQUIRES: key is after any previously added key according to comparator.
    // REQUIRES: Finish(), Abandon() have not been called
    void Add(const Slice& key, const Slice& value);

    // Advanced operation: flush any buffered key/value pairs to file.
    // Can be used to ensure that two adjacent entries never live in
    // the same data block.  Most clients should not need to use this method.
    // REQUIRES: Finish(), Abandon() have not been called
    void Flush();

    // Return non-ok iff some error has been detected.
    Status status() const;

    // Finish building the table.  Stops using the file passed to the
    // constructor after this function returns.
    // REQUIRES: Finish(), Abandon() have not been called
    Status Finish();

    // Indicate that the contents of this builder should be abandoned.  Stops
    // using the file passed to the constructor after this function returns.
    // If the caller is not going to call Finish(), it must call Abandon()
    // before destroying this builder.
    // REQUIRES: Finish(), Abandon() have not been called
    void Abandon();

    // Number of calls to Add() so far.
    uint64_t NumEntries() const;

    // Size of the file generated so far.  If invoked after a successful
    // Finish() call, returns the size of the final generated file.
    uint64_t FileSize() const;

private:
    bool ok() const { return status().ok(); }
    void WriteBlock(BlockBuilder* block, BlockHandle* handle);
    void WriteRawBlock(const Slice& data, Options::CompressionType type, BlockHandle* handle);

    struct Rep;
    Rep* rep_;
};

}  // namespace lsm
