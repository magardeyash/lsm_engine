#pragma once

#include <cstdint>
#include <fstream>
#include <string>
#include "lsm/slice.h"
#include "lsm/status.h"

namespace lsm {

// WAL record format: [CRC32:4][Length:2][Type:1][Data:Length]

class WalWriter {
public:
    explicit WalWriter(const std::string& filename);
    ~WalWriter();

    WalWriter(const WalWriter&) = delete;
    WalWriter& operator=(const WalWriter&) = delete;

    Status AddRecord(const Slice& slice);
    
    Status Sync();

private:
    std::string filename_;
    std::ofstream file_;
    Status status_;
};

class WalReader {
public:
    explicit WalReader(const std::string& filename);
    ~WalReader();

    WalReader(const WalReader&) = delete;
    WalReader& operator=(const WalReader&) = delete;

    // Reads the next record into *record. Returns false on EOF or corruption.
    bool ReadRecord(Slice* record, std::string* scratch);

    Status status() const { return status_; }

private:
    std::string filename_;
    std::ifstream file_;
    Status status_;
    
    void ReportCorruption(size_t bytes, const char* reason);
};

}
