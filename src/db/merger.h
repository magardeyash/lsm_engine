#pragma once

#include "lsm/iterator.h"
#include "lsm/comparator.h"
#include <vector>

namespace lsm {

// Returns an iterator over the union of children[0,n-1].
// Takes ownership of child iterators. No duplicate suppression.
Iterator* NewMergingIterator(const Comparator* comparator,
                             Iterator** children,
                             int n);

}
