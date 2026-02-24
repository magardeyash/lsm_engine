#include <gtest/gtest.h>
#include "src/db/memtable.h"
#include "lsm/comparator.h"
#include <string>

using namespace lsm;

TEST(MemTableTest, Simple) {
    const Comparator* cmp = BytewiseComparator();
    InternalKeyComparator icmp(cmp);
    MemTable* mem = new MemTable(icmp);
    mem->Ref();

    std::string val;
    Status s;

    // Check empty
    LookupKey lkey1("foo", 1);
    ASSERT_FALSE(mem->Get(lkey1, &val, &s));

    // Add elements
    mem->Add(1, kTypeValue, "foo", "v1");
    mem->Add(2, kTypeValue, "bar", "v2");

    // Get
    LookupKey lkey2("foo", 3);
    ASSERT_TRUE(mem->Get(lkey2, &val, &s));
    ASSERT_EQ("v1", val);

    LookupKey lkey3("bar", 3);
    ASSERT_TRUE(mem->Get(lkey3, &val, &s));
    ASSERT_EQ("v2", val);

    // Update
    mem->Add(4, kTypeValue, "foo", "v3");
    LookupKey lkey4("foo", 5);
    ASSERT_TRUE(mem->Get(lkey4, &val, &s));
    ASSERT_EQ("v3", val);

    // Delete
    mem->Add(6, kTypeDeletion, "foo", "");
    LookupKey lkey5("foo", 7);
    ASSERT_TRUE(mem->Get(lkey5, &val, &s));
    ASSERT_TRUE(s.IsNotFound());

    mem->Unref();
}

TEST(MemTableTest, Iterator) {
    const Comparator* cmp = BytewiseComparator();
    InternalKeyComparator icmp(cmp);
    MemTable* mem = new MemTable(icmp);
    mem->Ref();

    mem->Add(1, kTypeValue, "a", "va");
    mem->Add(2, kTypeValue, "b", "vb");

    Iterator* iter = mem->NewIterator();
    iter->SeekToFirst();
    
    ASSERT_TRUE(iter->Valid());
    
    ParsedInternalKey ikey1;
    ParseInternalKey(iter->key(), &ikey1);
    ASSERT_EQ("a", ikey1.user_key.ToString());
    ASSERT_EQ("va", iter->value().ToString());

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ParsedInternalKey ikey2;
    ParseInternalKey(iter->key(), &ikey2);
    ASSERT_EQ("b", ikey2.user_key.ToString());
    ASSERT_EQ("vb", iter->value().ToString());

    iter->Next();
    ASSERT_FALSE(iter->Valid());

    delete iter;
    mem->Unref();
}
