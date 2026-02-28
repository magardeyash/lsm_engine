#pragma once

#include <string>
#include "lsm/slice.h"
#include "lsm/iterator.h"
#include "lsm/comparator.h"
#include "skiplist.h"
#include "src/util/coding.h"

namespace lsm {

enum ValueType {
    kTypeDeletion = 0x0,
    kTypeValue = 0x1
};

// Internal key format: | User key (varlen) | Sequence Number (7 bytes) | ValueType (1 byte) |
// Sequence and Type are packed into a single 64-bit word.
inline constexpr uint64_t kMaxSequenceNumber = ((1ull << 56) - 1);

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

// Comparator for internal keys: sorts by user key asc, then sequence number desc.
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
    explicit MemTable(const InternalKeyComparator& comparator);

    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;

    void Ref() { ++refs_; }

    void Unref() {
        --refs_;
        assert(refs_ >= 0);
        if (refs_ <= 0) {
            delete this;
        }
    }

    size_t ApproximateMemoryUsage() const;

    // Returns an iterator over memtable contents (internal keys).
    // Caller must keep MemTable alive for the iterator's lifetime.
    Iterator* NewIterator();

    // Adds key->value with given sequence number and type.
    // value is typically empty for kTypeDeletion.
    void Add(uint64_t seq, ValueType type,
             const Slice& key,
             const Slice& value);

    // Returns true if key is found (stores value or NotFound status).
    // Returns false if key is absent.
    bool Get(const LookupKey& key, std::string* value, Status* s);

private:
    ~MemTable();

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
    
    std::atomic<size_t> memory_usage_;
};

}
