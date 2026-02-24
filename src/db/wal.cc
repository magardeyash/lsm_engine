#include "src/db/wal.h"
#include "src/util/crc32.h"
#include "src/util/coding.h"
#include <cstring>

namespace lsm {

static const int kHeaderSize = 4 + 2 + 1; // crc (4), len (2), type (1)
static const uint8_t kRecordTypeFull = 1;

WalWriter::WalWriter(const std::string& filename)
    : filename_(filename),
      file_(filename, std::ios::out | std::ios::binary | std::ios::app) {
    if (!file_.is_open()) {
        status_ = Status::IOError("Failed to open WAL file for writing: ", filename);
    }
}

WalWriter::~WalWriter() {
    if (file_.is_open()) {
        file_.close();
    }
}

Status WalWriter::AddRecord(const Slice& slice) {
    if (!status_.ok()) {
        return status_;
    }

    // Header layout: crc (4 bytes) | length (2 bytes) | type (1 byte)
    // The crc is computed over length, type, and data.
    // Guard against records larger than uint16_t can represent
    if (slice.size() > 65535) {
        return Status::NotSupported("WAL record too large (max 65535 bytes)");
    }

    uint32_t crc;
    uint16_t length = static_cast<uint16_t>(slice.size());
    uint8_t type = kRecordTypeFull; // Simplified: all records are FULL

    char header[kHeaderSize];
    header[4] = static_cast<char>(length & 0xff);
    header[5] = static_cast<char>(length >> 8);
    header[6] = type;

    // Compute CRC
    crc = crc32c::Value(header + 4, 3);
    crc = crc32c::Extend(crc, slice.data(), slice.size());
    crc = crc32c::Mask(crc);
    
    header[0] = static_cast<char>(crc & 0xff);
    header[1] = static_cast<char>((crc >> 8) & 0xff);
    header[2] = static_cast<char>((crc >> 16) & 0xff);
    header[3] = static_cast<char>((crc >> 24) & 0xff);

    file_.write(header, kHeaderSize);
    file_.write(slice.data(), slice.size());

    // Flush to OS, but not necessarily disk
    file_.flush();

    if (file_.fail()) {
        status_ = Status::IOError("Failed to write to WAL");
    }

    return status_;
}

Status WalWriter::Sync() {
    if (!status_.ok()) return status_;

    // Not truly cross platform fsync in std::ofstream, but flush is good enough for demonstration.
    file_.flush();
    if (file_.fail()) {
        status_ = Status::IOError("Failed to sync WAL");
    }
    return status_;
}

// ------------------------------------------------------------------------

WalReader::WalReader(const std::string& filename)
    : filename_(filename),
      file_(filename, std::ios::in | std::ios::binary) {
    if (!file_.is_open()) {
        // Since WAL doesn't always exist on new DB, this might be OK depending on caller.
        status_ = Status::NotFound("WAL file not found", filename);
    }
}

WalReader::~WalReader() {
    if (file_.is_open()) {
        file_.close();
    }
}

void WalReader::ReportCorruption(size_t bytes, const char* reason) {
    status_ = Status::Corruption("WAL corruption", reason);
}

bool WalReader::ReadRecord(Slice* record, std::string* scratch) {
    if (!status_.ok() && !status_.IsNotFound()) {
        return false;
    }
    if (status_.IsNotFound()) {
        // Clear it if we are just now reading. But file isn't open
        status_ = Status::OK();
        if (!file_.is_open()) return false;
    }

    while (true) {
        char header[kHeaderSize];
        file_.read(header, kHeaderSize);
        if (file_.eof()) {
            return false;
        }
        if (file_.gcount() < kHeaderSize) {
            ReportCorruption(file_.gcount(), "Truncated WAL record header");
            return false;
        }

        uint32_t expected_crc = DecodeFixed32(header);
        uint16_t length = static_cast<uint8_t>(header[4]) | (static_cast<uint8_t>(header[5]) << 8);
        uint8_t type = header[6];

        scratch->resize(length);
        file_.read(&(*scratch)[0], length);

        if (file_.gcount() < length) {
            ReportCorruption(file_.gcount(), "Truncated WAL record data");
            return false;
        }

        uint32_t actual_crc = crc32c::Value(header + 4, 3);
        actual_crc = crc32c::Extend(actual_crc, scratch->data(), length);

        if (expected_crc != crc32c::Mask(actual_crc)) {
            ReportCorruption(length, "Checksum mismatch in WAL record");
            return false;
        }

        if (type == kRecordTypeFull) {
            *record = Slice(*scratch);
            return true;
        } else {
            // Unrecognized record type, possibly fragmented
            ReportCorruption(length, "Unrecognized record type or unsupported fragmentation");
            return false;
        }
    }
}

}  // namespace lsm
