#include "lsm/options.h"
#include "lsm/comparator.h"

namespace lsm {

Options::Options()
    : comparator(BytewiseComparator()) {
}

}
