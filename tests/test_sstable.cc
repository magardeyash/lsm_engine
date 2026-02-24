#include <gtest/gtest.h>
#include "lsm/options.h"
#include "src/table/sstable_builder.h"
#include "src/table/sstable_reader.h"
#include <fstream>

using namespace lsm;

TEST(SSTableTest, BuildAndRead) {
    Options options;
    std::string fname = "test_sstable.sst";

    // Build SSTable
    std::ofstream* outfile = new std::ofstream(fname, std::ios::out | std::ios::binary);
    TableBuilder builder(options, outfile);
    builder.Add("key1", "val1");
    builder.Add("key2", "val2");
    builder.Add("key3", "val3");
    
    Status s = builder.Finish();
    ASSERT_TRUE(s.ok());
    
    uint64_t size = builder.FileSize();
    delete outfile; // Close file

    // Read SSTable
    Table* table = nullptr;
    s = Table::Open(options, fname, size, &table);
    ASSERT_TRUE(s.ok());

    ReadOptions ro;
    Iterator* iter = table->NewIterator(ro);
    
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("key1", iter->key().ToString());
    ASSERT_EQ("val1", iter->value().ToString());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("key2", iter->key().ToString());
    ASSERT_EQ("val2", iter->value().ToString());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("key3", iter->key().ToString());
    ASSERT_EQ("val3", iter->value().ToString());

    iter->Seek("key2");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("key2", iter->key().ToString());

    iter->Seek("key0");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("key1", iter->key().ToString()); // First element >= key0

    iter->Seek("key4");
    ASSERT_FALSE(iter->Valid()); // No element >= key4

    delete iter;
    delete table;
    remove(fname.c_str());
}
