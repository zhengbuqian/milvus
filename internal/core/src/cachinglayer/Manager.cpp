#include "cachinglayer/Manager.h"

#include <memory>

#include "cachinglayer/Utils.h"
#include "log/Log.h"

namespace milvus::cachinglayer {

Manager&
Manager::GetInstance() {
    static Manager instance;
    return instance;
}

void
Manager::ConfigureTieredStorage(bool enabled_globally, int64_t memory_limit_bytes, int64_t disk_limit_bytes) {
    Manager& manager = GetInstance();
    if (enabled_globally) {
        if (manager.dlist_ != nullptr) {
            manager.dlist_->UpdateLimit({memory_limit_bytes, disk_limit_bytes});
        } else {
            ResourceUsage limit{memory_limit_bytes, disk_limit_bytes};
            internal::DList::TouchConfig touch_config{std::chrono::seconds(10)};
            manager.dlist_ = std::make_unique<internal::DList>(limit, touch_config);
        }
        LOG_INFO(
            "Configured Tiered Storage manager with memory limit: {} bytes ({:.2f} GB), disk "
            "limit: {} bytes ({:.2f} GB)",
            memory_limit_bytes,
            memory_limit_bytes / (1024.0 * 1024.0 * 1024.0),
            disk_limit_bytes,
            disk_limit_bytes / (1024.0 * 1024.0 * 1024.0));
    } else {
        manager.dlist_ = nullptr;
        LOG_INFO("Tiered Storage is disabled");
    }
}

size_t
Manager::memory_overhead() const {
    // TODO(tiered storage 2): calculate memory overhead
    return 0;
}

}  // namespace milvus::cachinglayer
