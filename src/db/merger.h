#pragma once

#include "lsm/iterator.h"
#include "lsm/comparator.h"
#include <vector>

namespace lsm {

// Return an iterator that provided the union of the data in
// children[0,n-1].  Takes ownership of the child iterators and
// will delete them when the result iterator is deleted.
//
// The result does no duplicate suppression.  I.e., if a particular
// key is present in K child iterators, it will be yielded K times.
//
// REQUIRES: n >= 0
Iterator* NewMergingIterator(const Comparator* comparator,
                             Iterator** children,
                             int n);

}  // namespace lsm
