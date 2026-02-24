#include <gtest/gtest.h>
#include "lsm/db.h"
#include "lsm/options.h"
#include "lsm/status.h"
#include <string>

using namespace lsm;

class CompactionTest : public ::testing::Test {
protected:
    std::string dbname_ = "test_compaction_dir";
    DB* db_ = nullptr;

    void SetUp() override {
        #ifdef _WIN32
        system(("rmdir /S /Q " + dbname_).c_str());
        #else
        system(("rm -rf " + dbname_).c_str());
        #endif

        Options options;
        options.create_if_missing = true;
        // Make write buffer small to force frequent memtable flushes and L0 compactions
        options.write_buffer_size = 10 * 1024; // 10 KB
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

TEST_F(CompactionTest, BulkWriteTrigger) {
    WriteOptions wo;
    ReadOptions ro;
    std::string value;

    // Write enough data to force multiple flushes and compactions
    for (int i = 0; i < 2000; ++i) {
        std::string key = "key" + std::to_string(i);
        std::string val(200, 'x'); // 200 bytes
        ASSERT_TRUE(db_->Put(wo, key, val).ok());
    }

    // After many writes, data should still be fully readable
    for (int i = 0; i < 2000; ++i) {
        std::string key = "key" + std::to_string(i);
        Status s = db_->Get(ro, key, &value);
        ASSERT_TRUE(s.ok()) << "Missing key: " << key;
        ASSERT_EQ(200, value.size());
    }
}
