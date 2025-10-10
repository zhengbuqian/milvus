// Copyright (C) 2019-2024 Zilliz. All rights reserved.
// Licensed under the Apache License, Version 2.0

#include "expr_parser_client.h"

#include <fcntl.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <random>

#include "pb/schema.pb.h"
// #include "pb/plan.pb.h"
// #include "log/Log.h"

namespace milvus::scalar_bench {

namespace {
static std::string GetHelperPath() {
    const char* env = std::getenv("MILVUS_EXPRPARSER_PATH");
    if (env && *env) return std::string(env);
    // default to ./bin/exprparser relative to project root
    return std::string("./bin/exprparser");
}

static std::string JsonEscape(const std::string& s) {
    std::string o; o.reserve(s.size() + 8);
    for (unsigned char c : s) {
        switch (c) {
            case '"': o += "\\\""; break;
            case '\\': o += "\\\\"; break;
            case '\n': o += "\\n"; break;
            case '\r': o += "\\r"; break;
            case '\t': o += "\\t"; break;
            default:
                if (c < 0x20) { char buf[7]; snprintf(buf, sizeof(buf), "\\u%04x", c); o += buf; }
                else o.push_back(c);
        }
    }
    return o;
}
}

ExprParserClient& ExprParserClient::Instance() {
    static ExprParserClient inst;
    return inst;
}

ExprParserClient::~ExprParserClient() { Stop(); }

void ExprParserClient::Start() { EnsureStarted(); }

void ExprParserClient::EnsureStarted() {
    std::lock_guard<std::mutex> g(mu_);
    if (running_) return;
    SpawnChild();
}

void ExprParserClient::Stop() {
    std::lock_guard<std::mutex> g(mu_);
    if (!running_) return;
    if (fd_in_ >= 0) close(fd_in_);
    if (fd_out_ >= 0) close(fd_out_);
    if (child_pid_ > 0) {
        kill(child_pid_, SIGTERM);
        waitpid(child_pid_, nullptr, 0);
    }
    if (reader_.joinable()) reader_.join();
    running_ = false;
    fd_in_ = fd_out_ = -1;
    child_pid_ = -1;
}

void ExprParserClient::SpawnChild() {
    int inpipe[2]; // parent -> child (stdin)
    int outpipe[2]; // child -> parent (stdout)
    if (pipe(inpipe) != 0 || pipe(outpipe) != 0) {
        throw std::runtime_error("pipe() failed for exprparser");
    }

    pid_t pid = fork();
    if (pid < 0) {
        throw std::runtime_error("fork() failed for exprparser");
    }
    if (pid == 0) {
        // child
        dup2(inpipe[0], STDIN_FILENO);
        dup2(outpipe[1], STDOUT_FILENO);
        close(inpipe[0]); close(inpipe[1]);
        close(outpipe[0]); close(outpipe[1]);
        std::string path = GetHelperPath();
        execl(path.c_str(), path.c_str(), (char*)nullptr);
        _exit(127);
    }

    // parent
    close(inpipe[0]);
    close(outpipe[1]);
    fd_in_ = inpipe[1];
    fd_out_ = outpipe[0];
    child_pid_ = pid;
    running_ = true;
    reader_ = std::thread(&ExprParserClient::ReaderLoop, this, fd_out_);
}

void ExprParserClient::ReaderLoop(int fd_out) {
    FILE* fp = fdopen(fd_out, "r");
    if (!fp) return;
    char* lineptr = nullptr; size_t n = 0;
    for (;;) {
        ssize_t r = getline(&lineptr, &n, fp);
        if (r <= 0) break;
        std::string line(lineptr, r);
        // parse minimal JSON: {"id":"..","ok":true, ...}
        // We'll just search fields to avoid adding a JSON lib.
        auto findField = [&](const char* key)->std::optional<std::string> {
            std::string k = std::string("\"") + key + "\":";
            size_t p = line.find(k);
            if (p == std::string::npos) return std::nullopt;
            p += k.size();
            // if next is '"' -> string
            if (p < line.size() && line[p] == '"') {
                size_t q = line.find('"', p+1);
                if (q == std::string::npos) return std::nullopt;
                return line.substr(p+1, q-(p+1));
            }
            // else read until , or }
            size_t q = line.find_first_of("},\n\r", p);
            if (q == std::string::npos) q = line.size();
            return line.substr(p, q-p);
        };

        auto idOpt = findField("id");
        if (!idOpt) continue;
        std::string id = *idOpt;
        std::shared_ptr<Pending> pending;
        {
            std::lock_guard<std::mutex> g(mu_);
            auto it = pendings_.find(id);
            if (it != pendings_.end()) pending = it->second;
        }
        if (!pending) continue;

        // ok
        auto okOpt = findField("ok");
        bool ok = okOpt && (okOpt->find("true") != std::string::npos);
        if (ok) {
            auto planOpt = findField("plan_b64");
            pending->result = planOpt.value_or("");
        } else {
            pending->error = findField("error").value_or("unknown error");
        }
        {
            std::lock_guard<std::mutex> g(mu_);
            pending->done = true;
        }
        pending->cv.notify_all();
    }
    if (lineptr) free(lineptr);
}

void ExprParserClient::SendLine(const std::string& line) {
    ssize_t off = 0;
    const char* data = line.c_str();
    ssize_t len = line.size();
    while (off < len) {
        ssize_t w = write(fd_in_, data + off, len - off);
        if (w <= 0) throw std::runtime_error("write to exprparser failed");
        off += w;
    }
}

std::string ExprParserClient::ParseExprToPlanBytes(const std::string& expr,
                                                   const std::string& schema_proto_bytes,
                                                   bool is_count,
                                                   int64_t limit) {
    EnsureStarted();
    std::string id = NewId();
    auto pending = std::make_shared<Pending>();
    {
        std::lock_guard<std::mutex> g(mu_);
        pendings_[id] = pending;
    }

    std::string schema_b64 = Base64Encode(schema_proto_bytes);
    std::string line;
    line.reserve(expr.size() + schema_b64.size() + 128);
    line += "{\"id\":\"" + id + "\",\"op\":\"parse_expr\",\"schema_b64\":\"";
    line += schema_b64;
    line += "\",\"expr\":\"";
    line += JsonEscape(expr);
    line += "\",\"options\":{\"is_count\":";
    line += is_count ? "true" : "false";
    line += ",\"limit\":" + std::to_string(limit) + "}}\n";

    SendLine(line);

    std::unique_lock<std::mutex> lk(mu_);
    pending->cv.wait(lk, [&]{ return pending->done; });
    std::string err = pending->error;
    std::string plan_b64 = pending->result;
    pendings_.erase(id);
    lk.unlock();

    if (!err.empty()) {
        throw std::runtime_error(err);
    }
    return Base64Decode(plan_b64);
}

std::string ExprParserClient::Base64Encode(const std::string& in) {
    static const char* tbl = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    size_t i = 0; unsigned val = 0; int valb = -6;
    for (unsigned char c : in) {
        val = (val << 8) + c; valb += 8;
        while (valb >= 0) { out.push_back(tbl[(val>>valb)&0x3F]); valb -= 6; }
    }
    if (valb > -6) out.push_back(tbl[((val<<8)>>(valb+8))&0x3F]);
    while (out.size() % 4) out.push_back('=');
    return out;
}

std::string ExprParserClient::Base64Decode(const std::string& in) {
    static const int T[256] = {
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
        -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,62,-1,-1,-1,63,
        52,53,54,55,56,57,58,59,60,61,-1,-1,-1,-2,-1,-1,
        -1,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,
        15,16,17,18,19,20,21,22,23,24,25,-1,-1,-1,-1,-1,
        -1,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
        41,42,43,44,45,46,47,48,49,50,51,-1,-1,-1,-1,-1,
        // rest initialized to -1
    };
    std::string out; out.reserve(in.size()*3/4);
    int val = 0, valb = -8;
    for (unsigned char c : in) {
        if (c== '=') break;
        int d = c < 256 ? T[c] : -1;
        if (d == -1) continue;
        val = (val<<6) + d; valb += 6;
        if (valb >= 0) { out.push_back(char((val>>valb)&0xFF)); valb -= 8; }
    }
    return out;
}

std::string ExprParserClient::NewId() {
    static std::mt19937_64 rng{std::random_device{}()};
    uint64_t x = rng();
    char buf[17];
    snprintf(buf, sizeof(buf), "%016llx", (unsigned long long)x);
    return std::string(buf);
}

std::string BuildCollectionSchemaProtoBytes(milvus::SchemaPtr schema) {
    milvus::proto::schema::CollectionSchema proto;
    // Minimal conversion: map fields and primary/dynamic flags
    for (const auto& field_id : schema->get_field_ids()) {
        const auto& fm = (*schema)[field_id];
        auto* f = proto.add_fields();
        f->set_fieldid(field_id.get());
        f->set_name(fm.get_name().get());
        f->set_data_type(static_cast<milvus::proto::schema::DataType>(fm.get_data_type()));
        f->set_is_primary_key(schema->get_primary_field_id().has_value() && schema->get_primary_field_id()->get()==field_id.get());
        f->set_is_dynamic(schema->get_dynamic_field_id().has_value() && schema->get_dynamic_field_id()->get()==field_id.get());
        // FieldSchema in core pb may not expose max_length setter; skip here for helper needs
        if (fm.get_data_type() == milvus::DataType::ARRAY) {
            f->set_element_type(static_cast<milvus::proto::schema::DataType>(fm.get_element_type()));
        }
    }
    proto.set_enable_dynamic_field(schema->get_dynamic_field_id().has_value());
    std::string out; proto.SerializeToString(&out);
    return out;
}

} // namespace milvus::scalar_bench


