#pragma once

#include <cstdint>
#include <cstddef>

namespace lsm {
namespace crc32c {

// Extends init_crc with data[0,n-1] (used to incrementally compute CRC of a stream).
uint32_t Extend(uint32_t init_crc, const char* data, size_t n);

inline uint32_t Value(const char* data, size_t n) {
    return Extend(0, data, n);
}

static const uint32_t kMaskDelta = 0xa282ead8ul;

// Masked CRC (avoids issues with CRCs embedded inside CRC'd data).
inline uint32_t Mask(uint32_t crc) {
    return ((crc >> 15) | (crc << 17)) + kMaskDelta;
}

inline uint32_t Unmask(uint32_t masked_crc) {
    uint32_t rot = masked_crc - kMaskDelta;
    return ((rot >> 17) | (rot << 15));
}

}
}
