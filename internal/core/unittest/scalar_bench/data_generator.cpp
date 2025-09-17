// Copyright (C) 2019-2024 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "data_generator.h"
#include <algorithm>
#include <numeric>
#include <sstream>
#include <iomanip>
#include <cmath>
#include <cctype>

namespace milvus {
namespace scalar_bench {

const std::vector<std::string> DataGenerator::english_words_ = {
    "the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog",
    "data", "system", "query", "index", "search", "filter", "result",
    "user", "service", "request", "response", "process", "memory",
    "performance", "benchmark", "test", "analysis", "report", "metric",
    "database", "storage", "network", "compute", "cloud", "server",
    "client", "application", "interface", "protocol", "security", "access"
};

DataGenerator::DataGenerator(uint32_t seed) : rng_(seed) {
}

std::vector<int64_t>
DataGenerator::GenerateIntData(int64_t size,
                                Distribution dist,
                                int64_t min_val,
                                int64_t max_val,
                                int cardinality) {
    std::vector<int64_t> data;
    data.reserve(size);

    if (dist == Distribution::UNIFORM) {
        std::uniform_int_distribution<int64_t> uniform_dist(min_val, max_val);
        for (int64_t i = 0; i < size; ++i) {
            data.push_back(uniform_dist(rng_));
        }
    } else if (dist == Distribution::NORMAL) {
        // 正态分布
        double mean = (min_val + max_val) / 2.0;
        double stddev = (max_val - min_val) / 6.0;  // 99.7% 在范围内
        std::normal_distribution<double> normal_dist(mean, stddev);

        for (int64_t i = 0; i < size; ++i) {
            double val = normal_dist(rng_);
            // 截断到范围内
            val = std::max(static_cast<double>(min_val), std::min(static_cast<double>(max_val), val));
            data.push_back(static_cast<int64_t>(val));
        }
    } else if (dist == Distribution::SEQUENTIAL) {
        // 顺序数据
        for (int64_t i = 0; i < size; ++i) {
            data.push_back(min_val + i);
        }
    } else if (dist == Distribution::ZIPF) {
        // Zipf分布
        int actual_cardinality = (cardinality > 0) ? cardinality : 1000;
        auto indices = GenerateZipfIndices(size, actual_cardinality);

        // 生成唯一值
        std::vector<int64_t> unique_values;
        int64_t step = (max_val - min_val) / actual_cardinality;
        for (int i = 0; i < actual_cardinality; ++i) {
            unique_values.push_back(min_val + i * step);
        }

        // 根据Zipf分布分配值
        for (int64_t i = 0; i < size; ++i) {
            data.push_back(unique_values[indices[i]]);
        }
    }

    // 应用基数限制
    if (cardinality > 0) {
        ApplyCardinality(data, cardinality);
    }

    return data;
}

std::vector<double>
DataGenerator::GenerateFloatData(int64_t size,
                                  Distribution dist,
                                  double min_val,
                                  double max_val) {
    std::vector<double> data;
    data.reserve(size);

    if (dist == Distribution::UNIFORM) {
        std::uniform_real_distribution<double> uniform_dist(min_val, max_val);
        for (int64_t i = 0; i < size; ++i) {
            data.push_back(uniform_dist(rng_));
        }
    } else if (dist == Distribution::NORMAL) {
        double mean = (min_val + max_val) / 2.0;
        double stddev = (max_val - min_val) / 6.0;
        std::normal_distribution<double> normal_dist(mean, stddev);

        for (int64_t i = 0; i < size; ++i) {
            double val = normal_dist(rng_);
            val = std::max(min_val, std::min(max_val, val));
            data.push_back(val);
        }
    }

    return data;
}

std::vector<std::string>
DataGenerator::GenerateStringData(int64_t size,
                                   const StringGenConfig& config) {
    std::vector<std::string> result;

    switch (config.pattern) {
        case StringGenConfig::Pattern::UUID_LIKE:
            result = GenerateUUIDLikeData(size, config);
            break;
        case StringGenConfig::Pattern::TEMPLATE:
            result = GenerateTemplateData(size, config);
            break;
        case StringGenConfig::Pattern::SENTENCE:
            result = GenerateSentenceData(size, config);
            break;
        case StringGenConfig::Pattern::MIXED:
            result = GenerateMixedData(size, config);
            break;
    }

    // 应用基数限制
    if (config.cardinality > 0) {
        ApplyCardinality(result, config.cardinality);
    }

    // 应用分布
    if (config.distribution == Distribution::ZIPF) {
        ApplyDistribution(result, config.distribution);
    }

    // 应用重复率
    if (config.duplicate_ratio > 0) {
        int duplicate_count = size * config.duplicate_ratio;
        std::uniform_int_distribution<int> dist(0, size - 1);
        for (int i = 0; i < duplicate_count; ++i) {
            int src = dist(rng_);
            int dst = dist(rng_);
            result[dst] = result[src];
        }
    }

    return result;
}

std::vector<bool>
DataGenerator::GenerateBoolData(int64_t size, double true_ratio) {
    std::vector<bool> data;
    data.reserve(size);

    std::bernoulli_distribution dist(true_ratio);
    for (int64_t i = 0; i < size; ++i) {
        data.push_back(dist(rng_));
    }

    return data;
}

std::vector<bool>
DataGenerator::GenerateNullMask(int64_t size, double null_ratio) {
    return GenerateBoolData(size, null_ratio);
}

std::vector<std::string>
DataGenerator::GenerateUUIDLikeData(int64_t size,
                                     const StringGenConfig& config) {
    std::vector<std::string> result;
    result.reserve(size);

    const auto& uuid_cfg = config.uuid_config;
    std::uniform_int_distribution<> len_dist(uuid_cfg.min_length, uuid_cfg.max_length);
    std::uniform_int_distribution<> char_dist(0, uuid_cfg.charset.size() - 1);

    for (int64_t i = 0; i < size; ++i) {
        int length = len_dist(rng_);
        std::string uuid;
        uuid.reserve(length + 4);  // 预留连字符空间

        for (int j = 0; j < length; ++j) {
            // 标准UUID格式的连字符位置
            if (uuid_cfg.use_hyphens && (j == 8 || j == 12 || j == 16 || j == 20)) {
                uuid += '-';
            }
            uuid += uuid_cfg.charset[char_dist(rng_)];
        }

        result.push_back(std::move(uuid));
    }

    return result;
}

std::vector<std::string>
DataGenerator::GenerateTemplateData(int64_t size,
                                     const StringGenConfig& config) {
    std::vector<std::string> result;
    result.reserve(size);

    const auto& tmpl_cfg = config.template_config;

    for (int64_t i = 0; i < size; ++i) {
        int number = tmpl_cfg.sequence_start + i;

        // 格式化数字部分
        std::ostringstream oss;
        if (tmpl_cfg.zero_padding) {
            oss << std::setfill('0') << std::setw(tmpl_cfg.numeric_digits) << number;
        } else {
            oss << number;
        }

        // 组合最终字符串
        std::string final_str = tmpl_cfg.prefix + oss.str() + tmpl_cfg.suffix;
        result.push_back(std::move(final_str));
    }

    return result;
}

std::vector<std::string>
DataGenerator::GenerateSentenceData(int64_t size,
                                     const StringGenConfig& config) {
    std::vector<std::string> result;
    result.reserve(size);

    const auto& sent_cfg = config.sentence_config;
    std::uniform_int_distribution<> word_count_dist(sent_cfg.min_words, sent_cfg.max_words);
    std::uniform_int_distribution<> word_idx_dist(0, english_words_.size() - 1);

    for (int64_t i = 0; i < size; ++i) {
        int word_count = word_count_dist(rng_);
        std::string sentence;

        for (int j = 0; j < word_count; ++j) {
            if (j > 0) {
                sentence += " ";
            }

            std::string word = english_words_[word_idx_dist(rng_)];

            // 首字母大写
            if (j == 0 && sent_cfg.capitalize_first && !word.empty()) {
                word[0] = std::toupper(word[0]);
            }

            sentence += word;
        }

        // 添加标点
        if (sent_cfg.add_punctuation) {
            sentence += ".";
        }

        result.push_back(std::move(sentence));
    }

    return result;
}

std::vector<std::string>
DataGenerator::GenerateMixedData(int64_t size,
                                  const StringGenConfig& config) {
    std::vector<std::string> result;
    int64_t third = size / 3;

    // 1/3 UUID
    auto uuid_data = GenerateUUIDLikeData(third, config);
    result.insert(result.end(), uuid_data.begin(), uuid_data.end());

    // 1/3 模板
    auto template_data = GenerateTemplateData(third, config);
    result.insert(result.end(), template_data.begin(), template_data.end());

    // 剩余部分句子
    auto sentence_data = GenerateSentenceData(size - 2 * third, config);
    result.insert(result.end(), sentence_data.begin(), sentence_data.end());

    // 打乱顺序
    std::shuffle(result.begin(), result.end(), rng_);

    return result;
}

std::vector<int>
DataGenerator::GenerateZipfIndices(int size, int cardinality, double alpha) {
    std::vector<int> indices;
    indices.reserve(size);

    // 计算Zipf分布的归一化常数
    double c = 0;
    for (int i = 1; i <= cardinality; ++i) {
        c += 1.0 / std::pow(i, alpha);
    }
    c = 1.0 / c;

    // 生成累积分布
    std::vector<double> cumulative(cardinality);
    double sum = 0;
    for (int i = 0; i < cardinality; ++i) {
        sum += c / std::pow(i + 1, alpha);
        cumulative[i] = sum;
    }

    // 生成索引
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    for (int i = 0; i < size; ++i) {
        double r = dist(rng_);
        // 二分搜索找到对应的索引
        auto it = std::lower_bound(cumulative.begin(), cumulative.end(), r);
        int idx = std::distance(cumulative.begin(), it);
        indices.push_back(idx);
    }

    return indices;
}

} // namespace scalar_bench
} // namespace milvus