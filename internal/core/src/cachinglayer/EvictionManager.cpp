#include "cachinglayer/EvictionManager.h"

namespace milvus::cachinglayer {

EvictionManager::EvictionManager(ResourceUsage max_size)
    : dlist_(max_size, {std::chrono::seconds(10)}) {
}

internal::DList*
EvictionManager::dlist() {
    return &dlist_;
}

}  // namespace milvus::cachinglayer
