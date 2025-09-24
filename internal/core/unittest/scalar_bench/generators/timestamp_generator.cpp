#include "timestamp_generator.h"
#include <stdexcept>
#include <algorithm>

namespace milvus {
namespace scalar_bench {

TimestampGenerator::TimestampGenerator(const FieldConfig& config)
    : config_(config) {
    if (config_.generator != FieldGeneratorType::TIMESTAMP) {
        throw std::runtime_error("Invalid generator type for TimestampGenerator");
    }

    const auto& ts_config = config_.timestamp_config;
    hotspot_total_weight_ = 0.0;
    for (const auto& hotspot : ts_config.hotspots) {
        if (hotspot.weight < 0.0) {
            throw std::runtime_error("Hotspot weight cannot be negative");
        }
        hotspot_total_weight_ += hotspot.weight;
    }

    constexpr double epsilon = 1e-9;
    if (hotspot_total_weight_ > 1.0 + epsilon) {
        throw std::runtime_error("Total hotspot weight cannot exceed 1.0");
    }
}

proto::schema::FieldData
TimestampGenerator::Generate(size_t num_rows, RandomContext& ctx) {
    auto timestamps = GenerateEpochValues(num_rows, ctx);
    ApplyHotspots(timestamps, ctx);
    ApplyJitter(timestamps, ctx);
    proto::schema::FieldData field_data;
    field_data.set_field_name(config_.field_name);
    field_data.set_type(proto::schema::DataType::Int64);
    auto long_data = field_data.mutable_scalars()->mutable_long_data();
    for (const auto ts : timestamps) {
        long_data->add_data(ts);
    }
    return field_data;
}

std::vector<int64_t> TimestampGenerator::GenerateEpochValues(size_t num_rows, RandomContext& ctx) {
    const auto& ts_config = config_.timestamp_config;
    std::vector<int64_t> result;
    result.reserve(num_rows);

    int64_t start = ts_config.range.start;
    int64_t end = ts_config.range.end;

    // Generate uniformly distributed timestamps
    for (size_t i = 0; i < num_rows; i++) {
        result.push_back(ctx.UniformInt(start, end));
    }

    // Sort to make them monotonic if needed (common for time-series data)
    // This is optional and could be controlled by a config flag
    // std::sort(result.begin(), result.end());

    return result;
}

void TimestampGenerator::ApplyHotspots(std::vector<int64_t>& timestamps, RandomContext& ctx) {
    const auto& ts_config = config_.timestamp_config;

    if (ts_config.hotspots.empty()) {
        return;
    }

    if (hotspot_total_weight_ <= 0.0) {
        return;
    }

    // Redistribute timestamps according to hotspot weights
    std::vector<std::vector<int64_t>> hotspot_buckets(ts_config.hotspots.size());
    std::vector<int64_t> regular_bucket;

    // Determine how many timestamps go to each hotspot
    for (size_t i = 0; i < timestamps.size(); i++) {
        double r = ctx.UniformReal(0, 1);

        bool in_hotspot = false;
        if (r < hotspot_total_weight_) {
            // This timestamp should go to a hotspot
            double cumsum = 0;
            for (size_t h = 0; h < ts_config.hotspots.size(); h++) {
                cumsum += ts_config.hotspots[h].weight;
                if (r < cumsum) {
                    // Generate within this hotspot window
                    const auto& window = ts_config.hotspots[h].window;
                    int64_t ts = ctx.UniformInt(window.start, window.end);
                    hotspot_buckets[h].push_back(ts);
                    in_hotspot = true;
                    break;
                }
            }
        }

        if (!in_hotspot) {
            regular_bucket.push_back(timestamps[i]);
        }
    }

    // Combine all buckets
    timestamps.clear();
    for (const auto& bucket : hotspot_buckets) {
        timestamps.insert(timestamps.end(), bucket.begin(), bucket.end());
    }
    timestamps.insert(timestamps.end(), regular_bucket.begin(), regular_bucket.end());

    // Shuffle to mix hotspot and regular timestamps
    std::shuffle(timestamps.begin(), timestamps.end(), ctx.GetRNG());
}

void TimestampGenerator::ApplyJitter(std::vector<int64_t>& timestamps, RandomContext& ctx) {
    const auto& ts_config = config_.timestamp_config;

    if (ts_config.jitter <= 0) {
        return;
    }

    int64_t jitter_range = ts_config.jitter;

    for (auto& ts : timestamps) {
        int64_t jitter = ctx.UniformInt(-jitter_range, jitter_range);
        ts += jitter;

        // Ensure we stay within the original range
        ts = std::max(ts_config.range.start, std::min(ts_config.range.end, ts));
    }
}

} // namespace scalar_bench
} // namespace milvus