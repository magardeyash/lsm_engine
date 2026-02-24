#pragma once

#include <string>
#include "lsm/slice.h"
#include "lsm/iterator.h"
#include "lsm/comparator.h"
#include "skiplist.h"
#include "src/util/coding.h"

namespace lsm {

// Type of an entry in the memtable/sstable
enum ValueType {
    kTypeDeletion = 0x0,
    kTypeValue = 0x1
};

// Internal key format:
// | User key (varlen) | Sequence Number (7 bytes) | ValueType (1 byte) |
// Sequence Number and ValueType are packed into a single 64-bit word.
inline constexpr uint64_t kMaxSequenceNumber = ((1ull << 56) - 1);

// Pack sequence number and value type into a 64-bit representation
inline uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
    assert(seq <= kMaxSequenceNumber);
    assert(t <= kTypeValue);
    return (seq << 8) | t;
}

// Helper class for extracting information from an internal key
class InternalKey {
public:
    InternalKey() {}
    InternalKey(const Slice& user_key, uint64_t s, ValueType t) {
        rep_.assign(user_key.data(), user_key.size());
        PutFixed64(&rep_, PackSequenceAndType(s, t));
    }

    Slice Encode() const {
        return Slice(rep_);
    }

    Slice user_key() const {
        return ExtractUserKey(rep_);
    }

    void SetFrom(const Slice& s) {
        rep_.assign(s.data(), s.size());
    }

    void Clear() {
        rep_.clear();
    }

    std::string DebugString() const;

    // Static helpers
    static Slice ExtractUserKey(const Slice& internal_key) {
        assert(internal_key.size() >= 8);
        return Slice(internal_key.data(), internal_key.size() - 8);
    }
    
    static uint64_t ExtractSequenceAndType(const Slice& internal_key) {
        assert(internal_key.size() >= 8);
        return DecodeFixed64(internal_key.data() + internal_key.size() - 8);
    }

private:
    std::string rep_;
};

// A helper class that provides a comparator for internal keys.
// Compares User Keys using the provided user comparator.
// If User Keys are equal, compares Sequence Number in descending order.
class InternalKeyComparator : public Comparator {
public:
    explicit InternalKeyComparator(const Comparator* c) : user_comparator_(c) {}
    ~InternalKeyComparator() override = default;

    const char* Name() const override { return "lsm.InternalKeyComparator"; }

    int Compare(const Slice& a, const Slice& b) const override;
    
    void FindShortestSeparator(std::string* start, const Slice& limit) const override;
    
    void FindShortSuccessor(std::string* key) const override;

    const Comparator* user_comparator() const { return user_comparator_; }

    int Compare(const InternalKey& a, const InternalKey& b) const {
        return Compare(a.Encode(), b.Encode());
    }

private:
    const Comparator* user_comparator_;
};

class LookupKey {
public:
    LookupKey(const Slice& user_key, uint64_t sequence);
    ~LookupKey();

    Slice memtable_key() const { return Slice(start_, end_ - start_); }
    Slice internal_key() const { return Slice(kstart_, end_ - kstart_); }
    Slice user_key() const { return Slice(kstart_, end_ - kstart_ - 8); }

private:
    const char* start_;
    const char* kstart_;
    const char* end_;
    char space_[200];
};

struct ParsedInternalKey {
    Slice user_key;
    uint64_t sequence;
    ValueType type;
};

inline bool ParseInternalKey(const Slice& internal_key, ParsedInternalKey* result) {
    if (internal_key.size() < 8) return false;
    uint64_t num = InternalKey::ExtractSequenceAndType(internal_key);
    result->type = static_cast<ValueType>(num & 0xff);
    result->sequence = num >> 8;
    result->user_key = Slice(internal_key.data(), internal_key.size() - 8);
    return true;
}

class MemTableIterator;

class MemTable {
public:
    // MemTables are reference counted.  The initial reference count
    // is zero and the caller must call Ref() at least once.
    explicit MemTable(const InternalKeyComparator& comparator);

    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;

    // Increase reference count.
    void Ref() { ++refs_; }

    // Drop reference count.  Delete if no more references exist.
    void Unref() {
        --refs_;
        assert(refs_ >= 0);
        if (refs_ <= 0) {
            delete this;
        }
    }

    // Returns an estimate of the number of bytes of data in use by this
    // data structure.
    size_t ApproximateMemoryUsage() const;

    // Return an iterator that yields the contents of the memtable.
    //
    // The caller must ensure that the underlying MemTable remains live
    // while the returned iterator is live.  The keys returned by this
    // iterator are internal keys encoded by AppendInternalKey in the
    // database code.
    Iterator* NewIterator();

    // Add an entry into memtable that maps key to value at the
    // specified sequence number and with the specified type.
    // Typically value will be empty if type==kTypeDeletion.
    void Add(uint64_t seq, ValueType type,
             const Slice& key,
             const Slice& value);

    // If memtable contains a value for key, store it in *value and return true.
    // If memtable contains a deletion for key, store a NotFound() error
    // in *status and return true.
    // Else, return false.
    bool Get(const LookupKey& key, std::string* value, Status* s);

private:
    ~MemTable();  // Private since only Unref() should be used to delete it

    struct KeyComparator {
        const InternalKeyComparator comparator;
        explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
        int operator()(const char* a, const char* b) const;
    };

    friend class MemTableIterator;
    friend class MemTableBackwardIterator;

    typedef SkipList<const char*, KeyComparator> Table;

    KeyComparator comparator_;
    int refs_;
    Table table_;
    
    // Note: To simplify the implementation and guarantee memory stability
    // for keys and values inserted into the SkipList, we allocate a single
    // flat char buffer for each entry, combining the internal key and value.
    // An arena allocator would be more efficient, but we will use new char[]
    // locally inside Add(). The SkipList will take ownership of the memory.
    
    // Keeps track of approximate memory used by entries
    std::atomic<size_t> memory_usage_;
};

}  // namespace lsm
