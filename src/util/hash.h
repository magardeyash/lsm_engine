#pragma once

#include <cstdint>
#include <cstddef>

namespace lsm {

uint32_t Hash(const char* data, size_t n, uint32_t seed);

}
