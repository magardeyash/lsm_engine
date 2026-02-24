#include "src/db/merger.h"
#include <cassert>
#include "lsm/comparator.h"
#include <vector>

namespace lsm {

namespace {

class MergingIterator : public Iterator {
public:
    MergingIterator(const Comparator* comparator, Iterator** children, int n)
        : comparator_(comparator),
          children_(new IteratorWrapper[n]),
          n_(n),
          current_(nullptr),
          direction_(kForward) {
        for (int i = 0; i < n; i++) {
            children_[i].Set(children[i]);
        }
    }

    ~MergingIterator() override {
        delete[] children_;
    }

    bool Valid() const override {
        return (current_ != nullptr);
    }

    void SeekToFirst() override {
        for (int i = 0; i < n_; i++) {
            children_[i].SeekToFirst();
        }
        FindSmallest();
        direction_ = kForward;
    }

    void SeekToLast() override {
        for (int i = 0; i < n_; i++) {
            children_[i].SeekToLast();
        }
        FindLargest();
        direction_ = kReverse;
    }

    void Seek(const Slice& target) override {
        for (int i = 0; i < n_; i++) {
            children_[i].Seek(target);
        }
        FindSmallest();
        direction_ = kForward;
    }

    void Next() override {
        assert(Valid());
        // Ensure that all children are positioned after key().
        // If we are moving in the forward direction, it is already
        // true for all of the non-current_ children since current_ is
        // the smallest child and key() == current_->key().  Otherwise,
        // we explicitly position the non-current_ children.
        if (direction_ != kForward) {
            for (int i = 0; i < n_; i++) {
                IteratorWrapper* child = &children_[i];
                if (child != current_) {
                    child->Seek(key());
                    if (child->Valid() &&
                        comparator_->Compare(key(), child->key()) == 0) {
                        child->Next();
                    }
                }
            }
            direction_ = kForward;
        }

        current_->Next();
        FindSmallest();
    }

    void Prev() override {
        assert(Valid());
        // Ensure that all children are positioned before key().
        // If we are moving in the reverse direction, it is already
        // true for all of the non-current_ children since current_ is
        // the largest child and key() == current_->key().  Otherwise,
        // we explicitly position the non-current_ children.
        if (direction_ != kReverse) {
            for (int i = 0; i < n_; i++) {
                IteratorWrapper* child = &children_[i];
                if (child != current_) {
                    child->Seek(key());
                    if (child->Valid()) {
                        // Child is at first entry >= key().  Step back one to be < key()
                        child->Prev();
                    } else {
                        // Child has no entries >= key().  Position at last entry.
                        child->SeekToLast();
                    }
                }
            }
            direction_ = kReverse;
        }

        current_->Prev();
        FindLargest();
    }

    Slice key() const override {
        assert(Valid());
        return current_->key();
    }

    Slice value() const override {
        assert(Valid());
        return current_->value();
    }

    Status status() const override {
        Status status;
        for (int i = 0; i < n_; i++) {
            status = children_[i].status();
            if (!status.ok()) {
                return status;
            }
        }
        return status;
    }

private:
    class IteratorWrapper {
    public:
        IteratorWrapper() : iter_(nullptr), valid_(false) {}
        void Set(Iterator* iter) {
            delete iter_;
            iter_ = iter;
            if (iter_ == nullptr) {
                valid_ = false;
            } else {
                Update();
            }
        }
        ~IteratorWrapper() { delete iter_; }
        Iterator* iter() const { return iter_; }

        bool Valid() const { return valid_; }
        Slice key() const { assert(Valid()); return key_; }
        Slice value() const { assert(Valid()); return iter_->value(); }
        Status status() const { assert(iter_); return iter_->status(); }
        void Next() { assert(iter_); iter_->Next(); Update(); }
        void Prev() { assert(iter_); iter_->Prev(); Update(); }
        void Seek(const Slice& k) { assert(iter_); iter_->Seek(k); Update(); }
        void SeekToFirst() { assert(iter_); iter_->SeekToFirst(); Update(); }
        void SeekToLast() { assert(iter_); iter_->SeekToLast(); Update(); }

    private:
        void Update() {
            valid_ = iter_->Valid();
            if (valid_) {
                key_ = iter_->key();
            }
        }
        Iterator* iter_;
        bool valid_;
        Slice key_;
    };

    void FindSmallest() {
        IteratorWrapper* smallest = nullptr;
        for (int i = 0; i < n_; i++) {
            IteratorWrapper* child = &children_[i];
            if (child->Valid()) {
                if (smallest == nullptr) {
                    smallest = child;
                } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
                    smallest = child;
                }
            }
        }
        current_ = smallest;
    }

    void FindLargest() {
        IteratorWrapper* largest = nullptr;
        for (int i = n_ - 1; i >= 0; i--) {
            IteratorWrapper* child = &children_[i];
            if (child->Valid()) {
                if (largest == nullptr) {
                    largest = child;
                } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
                    largest = child;
                }
            }
        }
        current_ = largest;
    }

    const Comparator* comparator_;
    IteratorWrapper* children_;
    int n_;
    IteratorWrapper* current_;
    enum Direction {
        kForward,
        kReverse
    };
    Direction direction_;
};

}  // namespace

Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children, int n) {
    if (n == 0) {
        return NewEmptyIterator();
    } else if (n == 1) {
        return children[0];
    } else {
        return new MergingIterator(comparator, children, n);
    }
}

}  // namespace lsm
