#pragma once

#include <cstdint>
#include <fstream>
#include <string>
#include "lsm/slice.h"
#include "lsm/status.h"

namespace lsm {

// Format of a WAL record:
//   Checksum: uint32_t (CRC32 of type and data)
//   Length:   uint16_t
//   Type:     uint8_t
//   Data:     uint8_t[Length]

class WalWriter {
public:
    // Create a writer that will append data to "filename".
    // "filename" will be created if it does not exist.
    explicit WalWriter(const std::string& filename);
    ~WalWriter();

    WalWriter(const WalWriter&) = delete;
    WalWriter& operator=(const WalWriter&) = delete;

    Status AddRecord(const Slice& slice);
    
    // Flush underlying file to disk
    Status Sync();

private:
    std::string filename_;
    std::ofstream file_;
    Status status_;
};

class WalReader {
public:
    // Create a reader that will return log records from "filename".
    explicit WalReader(const std::string& filename);
    ~WalReader();

    WalReader(const WalReader&) = delete;
    WalReader& operator=(const WalReader&) = delete;

    // Read the next record into *record.  Returns true if read
    // successfully, false if we hit end of the input or a corruption.
    // The contents of *record will be valid until the next mutating
    // operation on this reader or until the reader is destroyed.
    bool ReadRecord(Slice* record, std::string* scratch);

    // Returns the status of the reader.
    Status status() const { return status_; }

private:
    std::string filename_;
    std::ifstream file_;
    Status status_;
    
    // Report a corruption error
    void ReportCorruption(size_t bytes, const char* reason);
};

}  // namespace lsm
