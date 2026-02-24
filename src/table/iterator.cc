#include "lsm/iterator.h"
#include "lsm/status.h"

namespace lsm {

namespace {

class EmptyIterator : public Iterator {
public:
    EmptyIterator(const Status& s) : status_(s) {}
    ~EmptyIterator() override = default;

    bool Valid() const override { return false; }
    void SeekToFirst() override {}
    void SeekToLast() override {}
    void Seek(const Slice& target) override {}
    void Next() override {}
    void Prev() override {}
    Slice key() const override { return Slice(); }
    Slice value() const override { return Slice(); }
    Status status() const override { return status_; }

private:
    Status status_;
};

}  // namespace

Iterator* NewEmptyIterator() {
    return new EmptyIterator(Status::OK());
}

Iterator* NewErrorIterator(const Status& status) {
    return new EmptyIterator(status);
}

}  // namespace lsm
