#include <gtest/gtest.h>
#include "src/util/bloom.h"

using namespace lsm;

TEST(BloomTest, EmptyFilter) {
    const BloomFilterPolicy policy(10);
    std::string filter;
    policy.CreateFilter(nullptr, 0, &filter);
    
    // An empty filter should have some small size
    ASSERT_GT(filter.size(), 0);
    
    // Should not match anything
    ASSERT_FALSE(policy.KeyMayMatch("hello", filter));
    ASSERT_FALSE(policy.KeyMayMatch("world", filter));
}

TEST(BloomTest, Matches) {
    const BloomFilterPolicy policy(10);
    std::string filter;
    
    std::vector<Slice> keys = { "hello", "world", "lsm", "engine" };
    policy.CreateFilter(&keys[0], keys.size(), &filter);
    
    // Must match added keys
    ASSERT_TRUE(policy.KeyMayMatch("hello", filter));
    ASSERT_TRUE(policy.KeyMayMatch("world", filter));
    ASSERT_TRUE(policy.KeyMayMatch("lsm", filter));
    ASSERT_TRUE(policy.KeyMayMatch("engine", filter));
    
    // Might occasionally match other keys (false positive), but not likely
    // for just these 4 keys with 10 bits per key. Let's test some misses.
    int false_positives = 0;
    for (int i = 0; i < 10000; i++) {
        if (policy.KeyMayMatch("missing_" + std::to_string(i), filter)) {
            false_positives++;
        }
    }
    
    // Expected FP rate is around 1% for 10 bits/key, so < 200 is very safe
    ASSERT_LT(false_positives, 200);
}
