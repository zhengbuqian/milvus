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

#include "flame_graph_profiler.h"
#include "bench_paths.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <chrono>
#include <cstdlib>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>
#include <pwd.h>

namespace milvus {
namespace scalar_bench {

FlameGraphProfiler::FlameGraphProfiler(const std::string& flamegraph_repo)
    : FlameGraphProfiler(Config{flamegraph_repo}) {
}

FlameGraphProfiler::FlameGraphProfiler(const Config& config)
    : config_(config) {
    expanded_flamegraph_path_ = ExpandPath(config_.flamegraph_repo_path);
}

FlameGraphProfiler::~FlameGraphProfiler() {
    // 确保清理任何可能遗留的perf进程
    if (profiling_active_) {
        StopPerfRecord();
    }
}

bool
FlameGraphProfiler::ProfileAndGenerateFlameGraph(
    const std::function<void()>& workload,
    const std::string& output_path,
    const std::string& case_name) {

    if (!ValidateEnvironment()) {
        return false;
    }

    // 生成临时perf数据文件名（使用项目的artifacts目录）
    auto timestamp = std::chrono::system_clock::now().time_since_epoch().count();

    // 使用bench_paths中的临时目录
    std::string temp_dir = GetTempDir();
    std::string perf_data_path = temp_dir + "perf_" + std::to_string(timestamp) + ".data";

    // 获取perf路径
    std::string perf_path = FindPerfPath();
    if (perf_path.empty()) {
        last_error_ = "Failed to find perf executable";
        return false;
    }

    // 创建工作负载子进程
    pid_t workload_pid = fork();
    if (workload_pid == -1) {
        last_error_ = "Failed to fork workload process";
        return false;
    }

    if (workload_pid == 0) {
        // 子进程：运行工作负载
        // 运行总时长的工作负载
        auto start_time = std::chrono::steady_clock::now();
        auto end_time = start_time + std::chrono::milliseconds(
            static_cast<int>(config_.total_duration_seconds * 1000));

        while (std::chrono::steady_clock::now() < end_time) {
            workload();
        }
        _exit(0);
    }

    // 父进程：等待pre_buffer时间
    std::this_thread::sleep_for(
        std::chrono::milliseconds(static_cast<int>(config_.pre_buffer_seconds * 1000)));

    // 构建perf命令 - 使用timeout来确保perf会自动结束
    std::stringstream perf_cmd;

    // 如果需要sudo且系统有sudo命令，添加sudo
    if (needs_sudo_) {
        std::string sudo_check = ExecuteCommandWithOutput("which sudo");
        if (!sudo_check.empty()) {
            perf_cmd << "sudo -n ";  // -n表示非交互式，如果需要密码会失败
        }
    }

    perf_cmd << "timeout " << config_.profile_duration_seconds << " "
             << perf_path << " record -F " << config_.perf_frequency
             << " -p " << workload_pid
             << " -e " << config_.perf_events
             << " -g -o " << perf_data_path
             << " 2>&1";

    std::cout << "Starting perf profiling for case: " << case_name << std::endl;

    // 执行perf record（会阻塞直到timeout）
    int perf_result = std::system(perf_cmd.str().c_str());

    // 等待post buffer时间
    std::this_thread::sleep_for(
        std::chrono::milliseconds(static_cast<int>(config_.post_buffer_seconds * 1000)));

    // 终止工作负载进程
    kill(workload_pid, SIGTERM);
    waitpid(workload_pid, nullptr, 0);

    // 检查perf数据文件是否成功生成
    std::ifstream perf_data_check(perf_data_path);
    if (!perf_data_check.good()) {
        last_error_ = "Perf data file was not created. Command exit code: " + std::to_string(perf_result);
        return false;
    }
    perf_data_check.close();

    // 如果使用了sudo创建文件，修改权限以便后续操作
    if (needs_sudo_) {
        std::string chmod_cmd = "sudo chmod 644 " + perf_data_path;
        ExecuteCommand(chmod_cmd);
    }

    // 生成火焰图
    bool success = GenerateFlameGraph(perf_data_path, output_path, case_name);

    // 清理临时文件
    CleanupTempFiles(perf_data_path);

    return success;
}

bool
FlameGraphProfiler::ValidateEnvironment() const {
    // 动态查找perf路径
    std::string perf_path = FindPerfPath();
    if (perf_path.empty()) {
        last_error_ = "perf not found. Please install perf tools.";
        return false;
    }

    // 检查FlameGraph脚本是否存在
    std::string flamegraph_script = expanded_flamegraph_path_ + "/flamegraph.pl";
    std::ifstream fg_check(flamegraph_script);
    if (!fg_check.good()) {
        last_error_ = "FlameGraph scripts not found at: " + expanded_flamegraph_path_ +
                     ". Please clone https://github.com/brendangregg/FlameGraph";
        return false;
    }

    // 检查是否有权限运行perf
    std::string perf_paranoid = ExecuteCommandWithOutput("cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null");
    if (!perf_paranoid.empty()) {
        try {
            int paranoid_level = std::stoi(perf_paranoid);

            // 检查是否已经以root运行
            bool is_root = (geteuid() == 0);

            if (paranoid_level > 1 && !is_root) {
                // 只在第一次显示警告
                static bool warning_shown = false;
                if (!warning_shown) {
                    std::cerr << "\n[PERF CONFIG] perf_event_paranoid=" << paranoid_level << std::endl;
                    std::cerr << "  This may limit perf profiling capabilities." << std::endl;
                    std::cerr << "  To enable full profiling, run: sudo sysctl kernel.perf_event_paranoid=1" << std::endl;
                    std::cerr << "  Or run the benchmark with sudo." << std::endl << std::endl;
                    warning_shown = true;
                }
                // 如果paranoid级别太高，我们将尝试使用sudo
                needs_sudo_ = (paranoid_level >= 3);
            } else if (is_root) {
                // 以root运行时不需要额外的sudo
                needs_sudo_ = false;
            }
        } catch (...) {
            // 忽略解析错误
        }
    }

    return true;
}

bool
FlameGraphProfiler::StartPerfRecord(const std::string& perf_data_path, pid_t target_pid) {
    std::string perf_path = FindPerfPath();
    if (perf_path.empty()) {
        last_error_ = "Failed to find perf executable";
        return false;
    }

    std::stringstream cmd;
    cmd << perf_path << " record -F " << config_.perf_frequency
        << " -p " << target_pid
        << " -e " << config_.perf_events
        << " -g -o " << perf_data_path
        << " &";

    return ExecuteCommand(cmd.str());
}

bool
FlameGraphProfiler::StopPerfRecord() {
    // 停止所有perf record进程
    ExecuteCommand("pkill -SIGINT perf");
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    return true;
}

bool
FlameGraphProfiler::GenerateFlameGraph(const std::string& perf_data_path,
                                       const std::string& svg_output_path,
                                       const std::string& case_name) {
    // 获取perf路径
    std::string perf_path = FindPerfPath();
    if (perf_path.empty()) {
        last_error_ = "Failed to find perf executable";
        return false;
    }

    // 生成perf script输出
    std::string perf_script_output = perf_data_path + ".script";
    std::stringstream perf_script_cmd;

    // 如果需要sudo权限
    if (needs_sudo_) {
        std::string sudo_check = ExecuteCommandWithOutput("which sudo");
        if (!sudo_check.empty()) {
            perf_script_cmd << "sudo -n ";
        }
    }

    perf_script_cmd << perf_path << " script -i " << perf_data_path << " > " << perf_script_output;

    if (!ExecuteCommand(perf_script_cmd.str())) {
        last_error_ = "Failed to generate perf script output";
        return false;
    }

    // 生成折叠栈
    std::string folded_output = perf_data_path + ".folded";
    std::stringstream stackcollapse_cmd;
    stackcollapse_cmd << expanded_flamegraph_path_ << "/stackcollapse-perf.pl "
                      << perf_script_output << " > " << folded_output;

    if (!ExecuteCommand(stackcollapse_cmd.str())) {
        last_error_ = "Failed to collapse stacks";
        CleanupTempFiles(perf_script_output);
        return false;
    }

    // 生成火焰图
    std::stringstream flamegraph_cmd;
    flamegraph_cmd << expanded_flamegraph_path_ << "/flamegraph.pl ";
    if (!case_name.empty()) {
        flamegraph_cmd << "--title \"" << case_name << "\" ";
    }
    flamegraph_cmd << "--width 1500 "
                   << folded_output << " > " << svg_output_path;

    if (!ExecuteCommand(flamegraph_cmd.str())) {
        last_error_ = "Failed to generate flame graph";
        CleanupTempFiles(perf_script_output);
        CleanupTempFiles(folded_output);
        return false;
    }

    // 清理中间文件
    CleanupTempFiles(perf_script_output);
    CleanupTempFiles(folded_output);

    std::cout << "Flame graph generated: " << svg_output_path << std::endl;
    return true;
}

void
FlameGraphProfiler::CleanupTempFiles(const std::string& file_path) {
    std::remove(file_path.c_str());
}

std::string
FlameGraphProfiler::ExpandPath(const std::string& path) const {
    if (path.empty() || path[0] != '~') {
        return path;
    }

    const char* home = std::getenv("HOME");
    if (home == nullptr) {
        return path;
    }

    return std::string(home) + path.substr(1);
}

std::string
FlameGraphProfiler::FindPerfPath() const {
    // 如果已经缓存了路径，直接返回
    if (!perf_path_.empty()) {
        return perf_path_;
    }

    // 首先尝试系统默认的perf
    std::string default_perf = ExecuteCommandWithOutput("which perf");
    if (!default_perf.empty()) {
        // 检查是否可执行
        std::string test_cmd = default_perf + " --version 2>&1";
        std::string test_result = ExecuteCommandWithOutput(test_cmd);
        if (test_result.find("perf version") != std::string::npos) {
            perf_path_ = default_perf;
            return perf_path_;
        }
    }

    // 尝试在linux-tools目录下查找最新版本的perf
    std::string find_cmd = "ls -v /usr/lib/linux-tools-*/perf 2>/dev/null | tail -n 1";
    std::string linux_tools_perf = ExecuteCommandWithOutput(find_cmd);

    if (!linux_tools_perf.empty()) {
        // 验证找到的perf是否可用
        std::string test_cmd = linux_tools_perf + " --version 2>&1";
        std::string test_result = ExecuteCommandWithOutput(test_cmd);
        if (test_result.find("perf version") != std::string::npos) {
            perf_path_ = linux_tools_perf;
            std::cout << "Found perf at: " << perf_path_ << std::endl;
            return perf_path_;
        }
    }

    // 如果都没找到，返回空字符串
    return "";
}

bool
FlameGraphProfiler::ExecuteCommand(const std::string& command) const {
    int result = std::system(command.c_str());
    return result == 0;
}

std::string
FlameGraphProfiler::ExecuteCommandWithOutput(const std::string& command) const {
    char buffer[128];
    std::string result;

    FILE* pipe = popen(command.c_str(), "r");
    if (!pipe) {
        return "";
    }

    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
        result += buffer;
    }

    pclose(pipe);

    // 去除尾部换行
    if (!result.empty() && result.back() == '\n') {
        result.pop_back();
    }

    return result;
}

} // namespace scalar_bench
} // namespace milvus