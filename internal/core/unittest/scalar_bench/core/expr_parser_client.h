// Copyright (C) 2019-2024 Zilliz. All rights reserved.
// Licensed under the Apache License, Version 2.0

#pragma once

#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "common/Schema.h"

namespace milvus::scalar_bench {

class ExprParserClient {
public:
    static ExprParserClient& Instance();

    // Initialize child process; safe to call multiple times
    void Start();
    void Stop();

    // Parse expr -> serialized plan node (protobuf wire bytes)
    // Throws std::runtime_error on failure
    std::string ParseExprToPlanBytes(const std::string& expr,
                                     const std::string& schema_proto_bytes,
                                     bool is_count,
                                     int64_t limit);

private:
    ExprParserClient() = default;
    ~ExprParserClient();
    ExprParserClient(const ExprParserClient&) = delete;
    ExprParserClient& operator=(const ExprParserClient&) = delete;

    struct Pending {
        std::string result;
        std::string error;
        bool done{false};
        std::condition_variable cv;
    };

    // process management
    void SpawnChild();
    void ReaderLoop(int fd_out);
    void EnsureStarted();

    // io
    void SendLine(const std::string& line);

    // helpers
    static std::string Base64Encode(const std::string& in);
    static std::string Base64Decode(const std::string& in);
    static std::string NewId();

private:
    std::mutex mu_;
    int fd_in_{-1};   // write to child stdin
    int fd_out_{-1};  // read from child stdout
    pid_t child_pid_{-1};
    std::thread reader_;
    bool running_{false};

    std::map<std::string, std::shared_ptr<Pending>> pendings_;
};

// Build C++ schema proto bytes from segcore Schema
std::string BuildCollectionSchemaProtoBytes(milvus::SchemaPtr schema);

} // namespace milvus::scalar_bench


