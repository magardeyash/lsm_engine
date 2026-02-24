#include <gtest/gtest.h>
#include "lsm/db.h"
#include "lsm/options.h"
#include "lsm/status.h"

using namespace lsm;

class DBTest : public ::testing::Test {
protected:
    std::string dbname_ = "test_db_dir";
    DB* db_ = nullptr;

    void SetUp() override {
        // Clean up from previous run if any
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

TEST_F(DBTest, Empty) {
    ReadOptions ro;
    std::string value;
    Status s = db_->Get(ro, "foo", &value);
    ASSERT_TRUE(s.IsNotFound());
}

TEST_F(DBTest, ReadWrite) {
    WriteOptions wo;
    ReadOptions ro;
    std::string value;

    ASSERT_TRUE(db_->Put(wo, "foo", "v1").ok());
    ASSERT_TRUE(db_->Get(ro, "foo", &value).ok());
    ASSERT_EQ("v1", value);

    ASSERT_TRUE(db_->Put(wo, "bar", "v2").ok());
    ASSERT_TRUE(db_->Put(wo, "foo", "v3").ok());
    
    ASSERT_TRUE(db_->Get(ro, "foo", &value).ok());
    ASSERT_EQ("v3", value);
    
    ASSERT_TRUE(db_->Get(ro, "bar", &value).ok());
    ASSERT_EQ("v2", value);
}

TEST_F(DBTest, Delete) {
    WriteOptions wo;
    ReadOptions ro;
    std::string value;

    ASSERT_TRUE(db_->Put(wo, "foo", "v1").ok());
    ASSERT_TRUE(db_->Delete(wo, "foo").ok());
    ASSERT_TRUE(db_->Get(ro, "foo", &value).IsNotFound());
}

TEST_F(DBTest, Iterator) {
    WriteOptions wo;
    ReadOptions ro;

    db_->Put(wo, "a", "va");
    db_->Put(wo, "b", "vb");
    db_->Put(wo, "c", "vc");

    Iterator* iter = db_->NewIterator(ro);
    iter->SeekToFirst();
    
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a", iter->key().ToString());
    ASSERT_EQ("va", iter->value().ToString());
    
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("b", iter->key().ToString());
    
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("c", iter->key().ToString());

    iter->Next();
    ASSERT_FALSE(iter->Valid());

    delete iter;
}
