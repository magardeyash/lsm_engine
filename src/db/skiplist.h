#pragma once

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <memory>

namespace lsm {

// Thread safety: writes require external synchronization; reads require only
// that the list is not destroyed. Nodes are never deleted once inserted.

template <typename Key, class Comparator>
class SkipList {
private:
    struct Node;

public:
    explicit SkipList(Comparator cmp);
    ~SkipList();

    SkipList(const SkipList&) = delete;
    SkipList& operator=(const SkipList&) = delete;

    // Insert key into the list.
    // REQUIRES: nothing that compares equal to key is currently in the list.
    void Insert(const Key& key);

    bool Contains(const Key& key) const;

    class Iterator {
    public:
        explicit Iterator(const SkipList* list);

        bool Valid() const;

        // Returns the key at the current position.
        // REQUIRES: Valid()
        const Key& key() const;

        // Advances to the next position.
        // REQUIRES: Valid()
        void Next();

        // Advances to the previous position.
        // REQUIRES: Valid()
        void Prev();

        void Seek(const Key& target);

        void SeekToFirst();

        void SeekToLast();

    private:
        const SkipList* list_;
        Node* node_;
    };

private:
    enum { kMaxHeight = 12 };

    int GetMaxHeight() const {
        return max_height_.load(std::memory_order_relaxed);
    }

    Node* NewNode(const Key& key, int height);
    int RandomHeight();
    bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

    bool KeyIsAfterNode(const Key& key, Node* n) const;

    // Returns earliest node >= key. Fills prev[] if non-null.
    Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

    Node* FindLessThan(const Key& key) const;

    Node* FindLast() const;

    Comparator compare_;

    Node* const head_;

    std::atomic<int> max_height_;

    uint32_t rnd_;
};

template <typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node {
    explicit Node(const Key& k) : key(k) {}

    Key const key;

    Node* Next(int n) {
        assert(n >= 0);
        return next_[n].load(std::memory_order_acquire);
    }
    void SetNext(int n, Node* x) {
        assert(n >= 0);
        next_[n].store(x, std::memory_order_release);
    }

    Node* NoBarrier_Next(int n) {
        assert(n >= 0);
        return next_[n].load(std::memory_order_relaxed);
    }
    void NoBarrier_SetNext(int n, Node* x) {
        assert(n >= 0);
        next_[n].store(x, std::memory_order_relaxed);
    }

private:
    std::atomic<Node*> next_[1];
};

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::NewNode(const Key& key, int height) {
    char* mem = new char[sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1)];
    return new (mem) Node(key);
}

template <typename Key, class Comparator>
inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList* list) {
    list_ = list;
    node_ = nullptr;
}

template <typename Key, class Comparator>
inline bool SkipList<Key, Comparator>::Iterator::Valid() const {
    return node_ != nullptr;
}

template <typename Key, class Comparator>
inline const Key& SkipList<Key, Comparator>::Iterator::key() const {
    assert(Valid());
    return node_->key;
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Next() {
    assert(Valid());
    node_ = node_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Prev() {
    assert(Valid());
    node_ = list_->FindLessThan(node_->key);
    if (node_ == list_->head_) {
        node_ = nullptr;
    }
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Seek(const Key& target) {
    node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToFirst() {
    node_ = list_->head_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToLast() {
    node_ = list_->FindLast();
    if (node_ == list_->head_) {
        node_ = nullptr;
    }
}

template <typename Key, class Comparator>
int SkipList<Key, Comparator>::RandomHeight() {
    static const unsigned int kBranching = 4;
    int height = 1;
    while (height < kMaxHeight && ((rnd_ = (rnd_ * 1103515245 + 12345) & 0x7fffffff) % kBranching) == 0) {
        height++;
    }
    assert(height > 0);
    assert(height <= kMaxHeight);
    return height;
}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
    return (n != nullptr) && (compare_(n->key, key) < 0);
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key, Node** prev) const {
    Node* x = head_;
    int level = GetMaxHeight() - 1;
    while (true) {
        Node* next = x->Next(level);
        if (KeyIsAfterNode(key, next)) {
            x = next;
        } else {
            if (prev != nullptr) prev[level] = x;
            if (level == 0) {
                return next;
            } else {
                level--;
            }
        }
    }
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindLessThan(const Key& key) const {
    Node* x = head_;
    int level = GetMaxHeight() - 1;
    while (true) {
        assert(x == head_ || compare_(x->key, key) < 0);
        Node* next = x->Next(level);
        if (next == nullptr || compare_(next->key, key) >= 0) {
            if (level == 0) {
                return x;
            } else {
                level--;
            }
        } else {
            x = next;
        }
    }
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindLast() const {
    Node* x = head_;
    int level = GetMaxHeight() - 1;
    while (true) {
        Node* next = x->Next(level);
        if (next == nullptr) {
            if (level == 0) {
                return x;
            } else {
                level--;
            }
        } else {
            x = next;
        }
    }
}

template <typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp)
    : compare_(cmp),
      head_(NewNode(0 /* any key will do */, kMaxHeight)),
      max_height_(1),
      rnd_(0xdeadbeef) {
    for (int i = 0; i < kMaxHeight; i++) {
        head_->SetNext(i, nullptr);
    }
}

template <typename Key, class Comparator>
SkipList<Key, Comparator>::~SkipList() {
    Node* node = head_;
    while (node != nullptr) {
        Node* next = node->NoBarrier_Next(0);
        node->~Node();
        delete[] reinterpret_cast<char*>(node);
        node = next;
    }
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key& key) {
    Node* prev[kMaxHeight];
    Node* x = FindGreaterOrEqual(key, prev);

    assert(x == nullptr || !Equal(key, x->key));

    int height = RandomHeight();
    if (height > GetMaxHeight()) {
        for (int i = GetMaxHeight(); i < height; i++) {
            prev[i] = head_;
        }
        // max_height_ update is safe: concurrent readers see either the old
        // nullptr level pointers (dropping to next level) or the new node.
        max_height_.store(height, std::memory_order_relaxed);
    }

    x = NewNode(key, height);
    for (int i = 0; i < height; i++) {
        x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
        prev[i]->SetNext(i, x);
    }
}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::Contains(const Key& key) const {
    Node* x = FindGreaterOrEqual(key, nullptr);
    if (x != nullptr && Equal(key, x->key)) {
        return true;
    } else {
        return false;
    }
}

}
