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

#include <string>
#include <map>
#include <functional>
#include "scalar_filter_benchmark.h"

namespace milvus {
namespace scalar_bench {

// 预设配置管理器
class BenchmarkPresets {
public:
    // 预设配置生成函数类型
    using PresetGenerator = std::function<BenchmarkConfig()>;

    // 注册预设配置
    static void RegisterPreset(const std::string& name, PresetGenerator generator);

    // 获取预设配置
    static BenchmarkConfig GetPreset(const std::string& name);

    // 获取默认预设名称
    static std::string GetDefaultPresetName();

    // 获取所有预设名称列表
    static std::vector<std::string> GetPresetNames();

    // 检查预设是否存在
    static bool HasPreset(const std::string& name);

private:
    static std::map<std::string, PresetGenerator>& GetPresets();
};

// 预设配置生成函数声明

// 简单测试配置 - 基础功能测试
BenchmarkConfig CreateSimpleTestConfig();

// 全面测试配置 - 测试所有数据类型和索引组合
BenchmarkConfig CreateComprehensiveConfig();

// 性能测试配置 - 大数据量性能测试
BenchmarkConfig CreatePerformanceConfig();

// 快速测试配置 - 用于快速验证
BenchmarkConfig CreateQuickTestConfig();

} // namespace scalar_bench
} // namespace milvus