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
#include <memory>
#include <functional>
#include <thread>
#include <atomic>

namespace milvus {
namespace scalar_bench {

class FlameGraphProfiler {
public:
    struct Config {
        std::string flamegraph_repo_path = "~/FlameGraph";
        double profile_duration_seconds = 3.0;      // perf采集时长（增加到3秒）
        double total_duration_seconds = 4.0;        // 总运行时长（增加到4秒）
        double pre_buffer_seconds = 0.25;           // 前置缓冲时间
        double post_buffer_seconds = 0.75;          // 后置缓冲时间
        int perf_frequency = 9999;                  // perf采样频率（增加到9999）
        std::string perf_events = "cpu-cycles";     // perf事件类型
        int perf_mmap_pages = 256;                  // perf缓冲区大小（页数）
    };

    explicit FlameGraphProfiler(const std::string& flamegraph_repo = "~/FlameGraph");
    explicit FlameGraphProfiler(const Config& config);
    ~FlameGraphProfiler();

    // 对指定的工作负载进行性能分析并生成火焰图
    // workload: 要分析的工作负载函数
    // output_path: 输出SVG文件的完整路径
    // case_name: 用于perf记录的case名称（可选）
    bool ProfileAndGenerateFlameGraph(
        const std::function<void()>& workload,
        const std::string& output_path,
        const std::string& case_name = "");

    // 验证FlameGraph工具是否可用
    bool ValidateEnvironment() const;

    // 获取最后的错误信息
    std::string GetLastError() const { return last_error_; }

private:
    Config config_;
    std::string expanded_flamegraph_path_;
    mutable std::string last_error_;
    mutable std::string perf_path_;  // 缓存找到的perf路径
    mutable bool needs_sudo_ = false;  // 是否需要sudo权限
    std::atomic<bool> profiling_active_{false};
    std::atomic<pid_t> workload_pid_{0};

    // 展开路径中的~
    std::string ExpandPath(const std::string& path) const;

    // 动态查找perf路径
    std::string FindPerfPath() const;

    // 启动perf记录
    bool StartPerfRecord(const std::string& perf_data_path, pid_t target_pid);

    // 停止perf记录
    bool StopPerfRecord();

    // 使用perf script和flamegraph.pl生成火焰图
    bool GenerateFlameGraph(const std::string& perf_data_path,
                           const std::string& svg_output_path,
                           const std::string& case_name);

    // 清理临时文件
    void CleanupTempFiles(const std::string& perf_data_path);

    // 执行shell命令并返回是否成功
    bool ExecuteCommand(const std::string& command) const;

    // 执行shell命令并获取输出
    std::string ExecuteCommandWithOutput(const std::string& command) const;
};

} // namespace scalar_bench
} // namespace milvus