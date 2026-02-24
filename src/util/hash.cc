#include "hash.h"
#include "coding.h"
#include <cstring>

namespace lsm {

// MurmurHash3_x86_32 implementation
uint32_t Hash(const char* data, size_t n, uint32_t seed) {
    const uint32_t m = 0xcc9e2d51;
    const uint32_t n_const = 0x1b873593;
    uint32_t h = seed;

    const uint32_t* data32 = reinterpret_cast<const uint32_t*>(data);
    const uint32_t* end = data32 + (n / 4);

    while (data32 != end) {
        uint32_t k;
        std::memcpy(&k, data32, sizeof(uint32_t));
        data32++;
        
        // This takes care of little-endian / big-endian issues for murmurhash
        // if DecodeFixed32 is implemented correctly for the platform.
        // Actually, let's keep it simple and just use the bytes.
        k = DecodeFixed32(reinterpret_cast<const char*>(&k));

        k *= m;
        k = (k << 15) | (k >> 17);
        k *= n_const;

        h ^= k;
        h = (h << 13) | (h >> 19);
        h = h * 5 + 0xe6546b64;
    }

    const uint8_t* tail = reinterpret_cast<const uint8_t*>(end);
    uint32_t k1 = 0;

    switch (n & 3) {
        case 3:
            k1 ^= tail[2] << 16;
            [[fallthrough]];
        case 2:
            k1 ^= tail[1] << 8;
            [[fallthrough]];
        case 1:
            k1 ^= tail[0];
            k1 *= m;
            k1 = (k1 << 15) | (k1 >> 17);
            k1 *= n_const;
            h ^= k1;
    }

    h ^= n;
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;

    return h;
}

}  // namespace lsm
