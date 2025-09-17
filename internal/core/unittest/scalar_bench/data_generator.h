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

#pragma once

#include <vector>
#include <string>
#include <random>
#include <memory>
#include "scalar_filter_benchmark.h"

namespace milvus {
namespace scalar_bench {

// 字符串生成配置
struct StringGenConfig {
    enum class Pattern {
        UUID_LIKE,    // UUID格式字符串
        TEMPLATE,     // 模板格式（前缀+数字+后缀）
        SENTENCE,     // 英文句子
        MIXED        // 混合模式
    } pattern = Pattern::UUID_LIKE;

    // UUID配置
    struct {
        int min_length = 32;
        int max_length = 36;
        bool use_hyphens = true;
        std::string charset = "0123456789abcdef";
    } uuid_config;

    // 模板配置
    struct {
        std::string prefix = "string-";
        std::string suffix = "";
        int numeric_digits = 7;
        int sequence_start = 0;
        bool zero_padding = true;
    } template_config;

    // 句子配置
    struct {
        int min_words = 3;
        int max_words = 15;
        bool capitalize_first = true;
        bool add_punctuation = true;
    } sentence_config;

    Distribution distribution = Distribution::UNIFORM;
    int cardinality = -1;  // -1表示不限制
    double duplicate_ratio = 0.0;
};

class DataGenerator {
public:
    DataGenerator(uint32_t seed = std::random_device{}());

    // 生成整数数据
    std::vector<int64_t> GenerateIntData(
        int64_t size,
        Distribution dist,
        int64_t min_val = 0,
        int64_t max_val = 1000000,
        int cardinality = -1);

    // 生成浮点数数据
    std::vector<double> GenerateFloatData(
        int64_t size,
        Distribution dist,
        double min_val = 0.0,
        double max_val = 1000000.0);

    // 生成字符串数据
    std::vector<std::string> GenerateStringData(
        int64_t size,
        const StringGenConfig& config);

    // 生成布尔数据
    std::vector<bool> GenerateBoolData(
        int64_t size,
        double true_ratio = 0.5);

    // 生成NULL掩码
    std::vector<bool> GenerateNullMask(
        int64_t size,
        double null_ratio);

private:
    // UUID生成
    std::vector<std::string> GenerateUUIDLikeData(
        int64_t size,
        const StringGenConfig& config);

    // 模板字符串生成
    std::vector<std::string> GenerateTemplateData(
        int64_t size,
        const StringGenConfig& config);

    // 句子生成
    std::vector<std::string> GenerateSentenceData(
        int64_t size,
        const StringGenConfig& config);

    // 混合模式生成
    std::vector<std::string> GenerateMixedData(
        int64_t size,
        const StringGenConfig& config);

    // 应用基数限制
    template<typename T>
    void ApplyCardinality(std::vector<T>& data, int cardinality);

    // 应用分布
    template<typename T>
    void ApplyDistribution(std::vector<T>& data, Distribution dist);

    // 生成Zipf分布索引
    std::vector<int> GenerateZipfIndices(int size, int cardinality, double alpha = 1.0);

private:
    std::mt19937_64 rng_;

    // 词库
    static const std::vector<std::string> english_words_;
};

// 模板方法实现
template<typename T>
void DataGenerator::ApplyCardinality(std::vector<T>& data, int cardinality) {
    if (cardinality <= 0 || cardinality >= data.size()) {
        return;
    }

    // 生成基数个唯一值
    std::vector<T> unique_values(data.begin(), data.begin() + cardinality);

    // 用唯一值随机填充剩余位置
    std::uniform_int_distribution<int> dist(0, cardinality - 1);
    for (size_t i = cardinality; i < data.size(); ++i) {
        data[i] = unique_values[dist(rng_)];
    }
}

template<typename T>
void DataGenerator::ApplyDistribution(std::vector<T>& data, Distribution dist) {
    if (dist == Distribution::ZIPF) {
        // 应用Zipf分布
        int cardinality = std::min(static_cast<int>(data.size()), 1000);
        auto indices = GenerateZipfIndices(data.size(), cardinality);

        std::vector<T> unique_values(data.begin(), data.begin() + cardinality);
        for (size_t i = 0; i < data.size(); ++i) {
            data[i] = unique_values[indices[i]];
        }
    }
    // 其他分布...
}

} // namespace scalar_bench
} // namespace milvus