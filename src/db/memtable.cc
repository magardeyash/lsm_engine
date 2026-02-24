#include "lsm/comparator.h"
#include "lsm/iterator.h"
#include "src/util/coding.h"
#include "src/db/memtable.h"
#include <cstring>

namespace lsm {

// Extract the length of the internal key and return the key Slice
static Slice GetLengthPrefixedSliceHelper(const char* data) {
    uint32_t len;
    const char* p = data;
    p = GetVarint32Ptr(p, p + 5, &len);  // +5 for max varint32
    return Slice(p, len);
}

// Compare internal keys.
int InternalKeyComparator::Compare(const Slice& a, const Slice& b) const {
    // Expected format: user_key (varlen) + sequence (7 bytes) + type (1 byte)
    // Here `a` and `b` are full internal keys, e.g. from internal storage

    // If they aren't long enough to have an 8-byte suffix, fall back
    if (a.size() < 8 || b.size() < 8) {
        return user_comparator_->Compare(a, b);
    }
    
    Slice a_user(a.data(), a.size() - 8);
    Slice b_user(b.data(), b.size() - 8);

    int r = user_comparator_->Compare(a_user, b_user);
    if (r == 0) {
        // Sort sequence numbers in descending order.
        uint64_t a_seq_type = DecodeFixed64(a.data() + a.size() - 8);
        uint64_t b_seq_type = DecodeFixed64(b.data() + b.size() - 8);
        
        // Note: higher sequence numbers should sort before lower ones
        // so we negate the customary less-than comparison.
        if (a_seq_type > b_seq_type) r = -1;
        else if (a_seq_type < b_seq_type) r = +1;
    }
    return r;
}

// These are essentially no-ops for internal keys because
// shortening could mess up the sequence numbers.
void InternalKeyComparator::FindShortestSeparator(std::string* start, const Slice& limit) const {}
void InternalKeyComparator::FindShortSuccessor(std::string* key) const {}

std::string InternalKey::DebugString() const {
    std::string result;
    if (rep_.size() >= 8) {
        Slice key_slice(rep_.data(), rep_.size() - 8);
        uint64_t seq_type = DecodeFixed64(rep_.data() + rep_.size() - 8);
        
        result.append("'");
        result.append(key_slice.data(), key_slice.size());
        result.append("' @ ");
        result.append(std::to_string(seq_type >> 8));
        result.append(" : ");
        result.append((seq_type & 0xff) == kTypeValue ? "Val" : "Del");
    }
    return result;
}

// Extract internal key and compare.
int MemTable::KeyComparator::operator()(const char* a, const char* b) const {
    Slice a_slice = GetLengthPrefixedSliceHelper(a);
    Slice b_slice = GetLengthPrefixedSliceHelper(b);
    return comparator.Compare(a_slice, b_slice);
}

class MemTableIterator : public Iterator {
public:
    explicit MemTableIterator(MemTable::Table* table) 
        : iter_(table) {}

    ~MemTableIterator() override = default;

    bool Valid() const override { return iter_.Valid(); }
    
    void Seek(const Slice& target) override { iter_.Seek(EncodeKey(&tmp_, target)); }
    
    void SeekToFirst() override { iter_.SeekToFirst(); }
    
    void SeekToLast() override { iter_.SeekToLast(); }
    
    void Next() override { iter_.Next(); }
    
    void Prev() override { iter_.Prev(); }

    Slice key() const override { return GetLengthPrefixedSliceHelper(iter_.key()); }

    Slice value() const override {
        // The value follows the encoded key
        Slice key_slice = GetLengthPrefixedSliceHelper(iter_.key());
        const char* val_ptr = key_slice.data() + key_slice.size();
        return GetLengthPrefixedSliceHelper(val_ptr);
    }

    Status status() const override { return Status::OK(); }

private:
    MemTable::Table::Iterator iter_;
    std::string tmp_;       // For passing to Seek

    const char* EncodeKey(std::string* scratch, const Slice& target) {
        scratch->clear();
        PutVarint32(scratch, target.size());
        scratch->append(target.data(), target.size());
        return scratch->data();
    }
};

MemTable::MemTable(const InternalKeyComparator& comparator)
    : comparator_(comparator),
      refs_(0),
      table_(comparator_),
      memory_usage_(0) {
}

MemTable::~MemTable() {
    assert(refs_ == 0);
    // Since SkipList nodes are individually heap-allocated in our implementation,
    // we must iterate over the SkipList and free the memory for our custom key/val blocks
    
    Iterator* iter = NewIterator();
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        Slice k = iter->key();
        Slice v = iter->value();
        
        // Recover the start of the buffer
        const char* entry = k.data() - VarintLength(k.size());
        
        // This memory was allocated using new[] in Add(), we must delete[] it here.
        delete[] entry;
    }
    delete iter;
}

size_t MemTable::ApproximateMemoryUsage() const {
    return memory_usage_.load(std::memory_order_relaxed);
}

Iterator* MemTable::NewIterator() {
    return new MemTableIterator(&table_);
}

void MemTable::Add(uint64_t seq, ValueType type,
                   const Slice& key, const Slice& value) {
    // Format of an entry is concatenation of:
    //  key_size     : varint32 of internal_key.size()
    //  key bytes    : char[internal_key.size()]
    //  value_size   : varint32 of value.size()
    //  value bytes  : char[value.size()]
    
    size_t key_size = key.size();
    size_t val_size = value.size();
    
    // Total internal key includes the 8 bytes for sequence and type.
    size_t internal_key_size = key_size + 8;
    
    size_t encoded_len = VarintLength(internal_key_size) + internal_key_size +
                         VarintLength(val_size) + val_size;
                         
    char* buf = new char[encoded_len];
    char* p = EncodeVarint32(buf, internal_key_size);
    std::memcpy(p, key.data(), key_size);
    p += key_size;
    EncodeFixed64(p, PackSequenceAndType(seq, type));
    p += 8;
    
    p = EncodeVarint32(p, val_size);
    std::memcpy(p, value.data(), val_size);
    
    assert(p + val_size == buf + encoded_len);
    
    // Insert into SkipList. Skiplist only stores char* pointers.
    table_.Insert(buf);
    
    // Add memory usage estimate (including skip list node overhead)
    size_t overhead = sizeof(void*) * 8; // Assuming average height of ~4, each node has array of pointers
    memory_usage_.fetch_add(encoded_len + overhead, std::memory_order_relaxed);
}

bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
    Slice memkey = key.internal_key();
    MemTableIterator iter(&table_);
    iter.Seek(memkey);

    if (iter.Valid()) {
        Slice matched_internal_key = iter.key();
        if (matched_internal_key.size() >= 8) {
            Slice user_key = InternalKey::ExtractUserKey(matched_internal_key);
            
            // Re-compare just the user key to ensure it matches
            if (comparator_.comparator.user_comparator()->Compare(user_key, key.user_key()) == 0) {
                // Determine whether it was a put or delete
                uint64_t seq_type = InternalKey::ExtractSequenceAndType(matched_internal_key);
                ValueType type = static_cast<ValueType>(seq_type & 0xff);
                
                if (type == kTypeValue) {
                    Slice v = iter.value();
                    value->assign(v.data(), v.size());
                    return true;
                } else if (type == kTypeDeletion) {
                    *s = Status::NotFound(Slice());
                    return true;
                }
            }
        }
    }
    return false;
}

LookupKey::LookupKey(const Slice& user_key, uint64_t sequence) {
    size_t usize = user_key.size();
    size_t needed = usize + 13;  // A conservative estimate
    char* dst;
    if (needed <= sizeof(space_)) {
        dst = space_;
    } else {
        dst = new char[needed];
    }
    start_ = dst;
    dst = EncodeVarint32(dst, usize + 8);
    kstart_ = dst;
    std::memcpy(dst, user_key.data(), usize);
    dst += usize;
    EncodeFixed64(dst, PackSequenceAndType(sequence, kTypeValue));
    dst += 8;
    end_ = dst;
}

LookupKey::~LookupKey() {
    if (start_ != space_) delete[] start_;
}

}  // namespace lsm
