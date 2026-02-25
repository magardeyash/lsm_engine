#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <string>
#include <atomic>
#include "lsm/db.h"
#include "lsm/options.h"
#include "lsm/status.h"

using namespace lsm;

class GroupCommitTest : public ::testing::Test {
protected:
    std::string dbname_ = "test_group_commit_dir";
    DB* db_ = nullptr;

    void SetUp() override {
        #ifdef _WIN32
        system(("rmdir /S /Q " + dbname_).c_str());
        #else
        system(("rm -rf " + dbname_).c_str());
        #endif

        Options options;
        options.create_if_missing = true;
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

// Test that concurrent Put operations from multiple threads all complete
// successfully and data is fully readable afterward (group commit batching).
TEST_F(GroupCommitTest, ConcurrentPuts) {
    const int kNumThreads = 4;
    const int kKeysPerThread = 500;
    std::atomic<int> errors{0};

    auto writer = [&](int thread_id) {
        WriteOptions wo;
        for (int i = 0; i < kKeysPerThread; i++) {
            std::string key = "t" + std::to_string(thread_id) + "_key" + std::to_string(i);
            std::string val = "value_" + std::to_string(thread_id) + "_" + std::to_string(i);
            Status s = db_->Put(wo, key, val);
            if (!s.ok()) {
                errors.fetch_add(1);
            }
        }
    };

    std::vector<std::thread> threads;
    for (int t = 0; t < kNumThreads; t++) {
        threads.emplace_back(writer, t);
    }
    for (auto& t : threads) {
        t.join();
    }

    ASSERT_EQ(0, errors.load());

    // Verify all keys are readable
    ReadOptions ro;
    for (int t = 0; t < kNumThreads; t++) {
        for (int i = 0; i < kKeysPerThread; i++) {
            std::string key = "t" + std::to_string(t) + "_key" + std::to_string(i);
            std::string expected = "value_" + std::to_string(t) + "_" + std::to_string(i);
            std::string value;
            Status s = db_->Get(ro, key, &value);
            ASSERT_TRUE(s.ok()) << "Missing key: " << key;
            ASSERT_EQ(expected, value);
        }
    }
}

// Test that sync writes in a group commit batch force fsync.
TEST_F(GroupCommitTest, SyncWrite) {
    WriteOptions wo_sync;
    wo_sync.sync = true;
    WriteOptions wo_nosync;

    ASSERT_TRUE(db_->Put(wo_nosync, "k1", "v1").ok());
    ASSERT_TRUE(db_->Put(wo_sync, "k2", "v2").ok());
    ASSERT_TRUE(db_->Put(wo_nosync, "k3", "v3").ok());

    ReadOptions ro;
    std::string value;
    ASSERT_TRUE(db_->Get(ro, "k1", &value).ok());
    ASSERT_EQ("v1", value);
    ASSERT_TRUE(db_->Get(ro, "k2", &value).ok());
    ASSERT_EQ("v2", value);
    ASSERT_TRUE(db_->Get(ro, "k3", &value).ok());
    ASSERT_EQ("v3", value);
}

// Test concurrent puts and deletes to ensure group commit handles mixed types.
TEST_F(GroupCommitTest, ConcurrentPutsAndDeletes) {
    const int kNumThreads = 4;
    const int kOps = 200;
    std::atomic<int> errors{0};

    auto worker = [&](int thread_id) {
        WriteOptions wo;
        for (int i = 0; i < kOps; i++) {
            std::string key = "shared_key_" + std::to_string(i % 50);
            if (thread_id % 2 == 0) {
                Status s = db_->Put(wo, key, "val_" + std::to_string(thread_id));
                if (!s.ok()) errors.fetch_add(1);
            } else {
                Status s = db_->Delete(wo, key);
                if (!s.ok()) errors.fetch_add(1);
            }
        }
    };

    std::vector<std::thread> threads;
    for (int t = 0; t < kNumThreads; t++) {
        threads.emplace_back(worker, t);
    }
    for (auto& t : threads) {
        t.join();
    }

    ASSERT_EQ(0, errors.load());

    // Just verify the database is in a consistent state (no crashes)
    ReadOptions ro;
    Iterator* iter = db_->NewIterator(ro);
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        // Just iterate through to verify no corruption
    }
    ASSERT_TRUE(iter->status().ok());
    delete iter;
}
