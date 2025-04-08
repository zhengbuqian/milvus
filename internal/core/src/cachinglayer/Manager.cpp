#include "cachinglayer/Manager.h"

#include <memory>

#include "cachinglayer/Utils.h"

namespace milvus::cachinglayer {

Manager&
Manager::GetInstance() {
    static Manager instance;
    return instance;
}

Manager::Manager() {
    // TODO(tiered storage 1): config resource limit
    ResourceUsage limit{std::numeric_limits<int64_t>::max(),
                        std::numeric_limits<int64_t>::max()};
    eviction_manager_ = std::make_unique<EvictionManager>(limit);
}

Manager::~Manager() {
}

size_t
Manager::memory_overhead() const {
    // TODO(tiered storage 2): calculate memory overhead
    return 0;
}

}  // namespace milvus::cachinglayer
