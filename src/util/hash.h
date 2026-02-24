#pragma once

#include <cstdint>
#include <cstddef>

namespace lsm {

// Return the MurmurHash3 (32-bit variant) of data[0,n-1]
uint32_t Hash(const char* data, size_t n, uint32_t seed);

}  // namespace lsm
