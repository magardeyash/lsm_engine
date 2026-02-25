#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <string>
#include <random>
#include <atomic>
#include "lsm/db.h"
#include "lsm/options.h"
#include "lsm/status.h"

using namespace lsm;

class ConcurrencyTest : public ::testing::Test {
protected:
    std::string dbname_ = "test_concurrency_dir";
    DB* db_ = nullptr;

    void SetUp() override {
        #ifdef _WIN32
        system(("rmdir /S /Q " + dbname_).c_str());
        #else
        system(("rm -rf " + dbname_).c_str());
        #endif

        Options options;
        options.create_if_missing = true;
        // Small write buffer to force compaction during the stress test
        options.write_buffer_size = 10 * 1024;  // 10 KB
        Status s = DB::Open(options, dbname_, &db_);
        ASSERT_TRUE(s.ok()) << s.ToString();
    }

    void TearDown() override {
        delete db_;
        #ifdef _WIN32
        system(("rmdir /S /Q " + dbname_).c_str());
        #else
        system(("rm -rf " + dbname_).c_str());
        #endif
    }
};

// Stress test: concurrent reads, writes, and deletes while compaction runs.
TEST_F(ConcurrencyTest, StressReadWriteDelete) {
    const int kNumWriters = 2;
    const int kNumReaders = 2;
    const int kWriteOps = 500;
    const int kReadOps = 500;
    std::atomic<bool> done{false};
    std::atomic<int> write_errors{0};

    // Writer threads: insert keys
    auto writer = [&](int thread_id) {
        WriteOptions wo;
        for (int i = 0; i < kWriteOps; i++) {
            std::string key = "key_" + std::to_string(i % 200);
            std::string val(100, 'A' + (thread_id % 26));
            Status s = db_->Put(wo, key, val);
            if (!s.ok()) write_errors++;
        }
    };

    // Reader threads: read random keys
    auto reader = [&](int thread_id) {
        ReadOptions ro;
        std::mt19937 gen(thread_id * 42);
        std::uniform_int_distribution<> dis(0, 199);
        for (int i = 0; i < kReadOps && !done.load(); i++) {
            std::string key = "key_" + std::to_string(dis(gen));
            std::string value;
            db_->Get(ro, key, &value);
            // Don't check result — key may or may not exist
        }
    };

    // Deleter thread: delete some keys
    auto deleter = [&]() {
        WriteOptions wo;
        for (int i = 0; i < 100; i++) {
            std::string key = "key_" + std::to_string(i);
            db_->Delete(wo, key);
        }
    };

    std::vector<std::thread> threads;

    // Start writers
    for (int i = 0; i < kNumWriters; i++) {
        threads.emplace_back(writer, i);
    }
    // Start readers
    for (int i = 0; i < kNumReaders; i++) {
        threads.emplace_back(reader, i);
    }
    // Start deleter
    threads.emplace_back(deleter);

    for (auto& t : threads) {
        t.join();
    }
    done.store(true);

    ASSERT_EQ(0, write_errors.load());

    // Verify database consistency — iterate through all entries
    ReadOptions ro;
    Iterator* iter = db_->NewIterator(ro);
    int count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        count++;
    }
    ASSERT_TRUE(iter->status().ok());
    delete iter;

    // Just checking that we can still read without crashing
    EXPECT_GT(count, 0);
}

// Test iterator stability during concurrent writes.
TEST_F(ConcurrencyTest, IteratorDuringWrites) {
    WriteOptions wo;
    ReadOptions ro;

    // Pre-populate
    for (int i = 0; i < 100; i++) {
        ASSERT_TRUE(db_->Put(wo, "init_" + std::to_string(i), std::string(50, 'x')).ok());
    }

    // Take a snapshot via iterator
    Iterator* iter = db_->NewIterator(ro);
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());

    // Write more keys concurrently while iterating
    std::thread writer([&]() {
        WriteOptions wo2;
        for (int i = 0; i < 200; i++) {
            db_->Put(wo2, "new_" + std::to_string(i), std::string(50, 'y'));
        }
    });

    // Continue iterating through the original snapshot
    int iter_count = 0;
    while (iter->Valid()) {
        iter_count++;
        iter->Next();
    }
    ASSERT_TRUE(iter->status().ok());

    writer.join();
    delete iter;

    // Verify all new keys are also visible in a new iterator
    Iterator* iter2 = db_->NewIterator(ro);
    int total_count = 0;
    for (iter2->SeekToFirst(); iter2->Valid(); iter2->Next()) {
        total_count++;
    }
    ASSERT_TRUE(iter2->status().ok());
    ASSERT_GE(total_count, iter_count);
    delete iter2;
}
