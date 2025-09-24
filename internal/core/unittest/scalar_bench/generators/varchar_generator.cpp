#include "varchar_generator.h"
#include <stdexcept>
#include <algorithm>
#include <sstream>
#include <fstream>
#include "../config/benchmark_config_loader.h"

namespace milvus {
namespace scalar_bench {

VarcharGenerator::VarcharGenerator(const FieldConfig& config)
    : config_(config), template_regex_(R"(\{(\w+)\})") {
    if (config_.generator != FieldGeneratorType::VARCHAR) {
        throw std::runtime_error("Invalid generator type for VarcharGenerator");
    }
    Initialize();
}

void VarcharGenerator::Initialize() {
    const auto& varchar_config = config_.varchar_config;

    switch (varchar_config.mode) {
        case VarcharMode::RANDOM:
            LoadTokenPool();
            break;
        case VarcharMode::TEMPLATE:
            LoadTemplatePools();
            break;
        case VarcharMode::CORPUS:
            LoadCorpus();
            break;
        case VarcharMode::SINGLE_UUID:
        case VarcharMode::SINGLE_TIMESTAMP:
            // no preload required
            break;
        default:
            throw std::runtime_error("Unknown varchar generation mode");
    }
}

void VarcharGenerator::LoadTokenPool() {
    const auto& varchar_config = config_.varchar_config;

    // Load tokens from dictionary or inline
    if (!varchar_config.values.dictionary.empty()) {
        auto& registry = DictionaryRegistry::GetInstance();
        token_pool_ = registry.GetDictionary(varchar_config.values.dictionary, 0);
        if (token_pool_.empty()) {
            throw std::runtime_error("Token pool for VarcharGenerator is empty: " + varchar_config.values.dictionary);
        }
    } else if (!varchar_config.values.inline_items.empty()) {
        token_pool_ = varchar_config.values.inline_items;
        if (token_pool_.empty()) {
            throw std::runtime_error("Inline token pool for VarcharGenerator is empty");
        }
    } else {
        // Default token pool
        token_pool_ = {"the", "a", "an", "and", "or", "but", "in", "on", "at", "to",
                      "for", "of", "with", "by", "from", "up", "about", "into", "through", "during"};
    }
}

void VarcharGenerator::LoadTemplatePools() {
    const auto& varchar_config = config_.varchar_config;

    for (const auto& [name, pool] : varchar_config.pools) {
        template_pools_[name] = pool;
    }

    // Validate template
    if (varchar_config.template_str.empty()) {
        throw std::runtime_error("Template mode requires a template string");
    }

    // Parse and validate placeholders
    auto placeholders = ParseTemplatePlaceholders(varchar_config.template_str);
    for (const auto& placeholder : placeholders) {
        auto it = template_pools_.find(placeholder);
        if (it == template_pools_.end()) {
            throw std::runtime_error("Template placeholder '" + placeholder +
                                     "' has no corresponding pool");
        }
        const auto& pool = it->second;
        if (pool.empty()) {
            throw std::runtime_error("Template pool for placeholder '" +
                                     placeholder + "' is empty");
        }
        for (size_t i = 0; i < pool.size(); ++i) {
            if (pool[i].empty()) {
                throw std::runtime_error(
                    "Template pool for placeholder '" + placeholder +
                    "' contains an empty item at index " + std::to_string(i));
            }
        }
    }
}

void VarcharGenerator::LoadCorpus() {
    const auto& varchar_config = config_.varchar_config;

    if (varchar_config.corpus_file.empty()) {
        throw std::runtime_error("Corpus mode requires a corpus file");
    }

    // Resolve corpus file path (similar to dictionary path resolution)
    std::string corpus_path = BenchmarkConfigLoader::ResolveDictionaryPath(varchar_config.corpus_file);

    std::ifstream file(corpus_path);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open corpus file: " + corpus_path);
    }

    std::string line;
    while (std::getline(file, line)) {
        if (!line.empty() && line[0] != '#') {  // Skip empty lines and comments
            corpus_lines_.push_back(line);
        }
    }

    if (corpus_lines_.empty()) {
        throw std::runtime_error("Corpus file is empty: " + corpus_path);
    }
}

DataArray VarcharGenerator::Generate(size_t num_rows, RandomContext& ctx) {
    const auto& varchar_config = config_.varchar_config;
    std::vector<std::string> result;
    result.reserve(num_rows);
    std::vector<bool> null_mask;

    null_mask.reserve(num_rows);
    for (size_t i = 0; i < num_rows; i++) {
        std::string text;

        switch (varchar_config.mode) {
            case VarcharMode::RANDOM:
                text = GenerateRandomText(ctx);
                break;
            case VarcharMode::TEMPLATE:
                text = GenerateTemplateText(ctx);
                break;
            case VarcharMode::CORPUS:
                text = GenerateCorpusText(ctx);
                break;
            case VarcharMode::SINGLE_UUID:
                text = GenerateSingleUuid(ctx);
                break;
            case VarcharMode::SINGLE_TIMESTAMP:
                text = GenerateSingleTimestamp(ctx);
                break;
        }

        text = TruncateToMaxLength(text);
        bool is_valid = true;
        if (config_.nullable && config_.null_ratio > 0.0 && ctx.Bernoulli(config_.null_ratio)) {
            is_valid = false;
            text.clear();
        }
        result.push_back(std::move(text));
        if (config_.nullable && config_.null_ratio > 0.0) null_mask.push_back(is_valid);
    }

    DataArray data_array;
    data_array.set_type(milvus::proto::schema::DataType::VarChar);
    data_array.set_field_name(config_.field_name);
    data_array.set_is_dynamic(false);
    auto* string_array = data_array.mutable_scalars()->mutable_string_data();
    string_array->mutable_data()->Reserve(result.size());
    for (auto& s : result) {
        string_array->add_data(std::move(s));
    }
    if (!null_mask.empty()) {
        auto* vd = data_array.mutable_valid_data();
        vd->mutable_data()->Reserve(null_mask.size());
        for (auto b : null_mask) vd->add_data(b);
    }
    return data_array;
}

std::string VarcharGenerator::GenerateRandomText(RandomContext& ctx) {
    const auto& varchar_config = config_.varchar_config;

    // Determine number of tokens
    int token_count;
    if (varchar_config.token_count.min == varchar_config.token_count.max) {
        token_count = varchar_config.token_count.min;
    } else {
        if (varchar_config.token_count.distribution == Distribution::UNIFORM) {
            token_count = ctx.UniformInt(varchar_config.token_count.min, varchar_config.token_count.max);
        } else {
            // TODO: support other distributions
            throw std::runtime_error("VarcharGenerator now supports only UNIFORM distribution");
            // For other distributions, use a simple average
            // token_count = (varchar_config.token_count.min + varchar_config.token_count.max) / 2;
        }
    }

    // Generate random tokens
    std::stringstream ss;
    for (int i = 0; i < token_count; i++) {
        if (i > 0) ss << " ";
        size_t idx = ctx.UniformInt(0, token_pool_.size() - 1);
        ss << token_pool_[idx];
    }

    std::string text = ss.str();

    // Apply keywords and phrase sets
    text = ApplyKeywords(text, ctx);
    text = ApplyPhraseSets(text, ctx);

    return text;
}

std::string VarcharGenerator::GenerateTemplateText(RandomContext& ctx) {
    const auto& varchar_config = config_.varchar_config;
    std::string result = varchar_config.template_str;

    // Replace placeholders with random values from pools
    std::smatch match;
    while (std::regex_search(result, match, template_regex_)) {
        std::string placeholder = match[1].str();
        auto it = template_pools_.find(placeholder);
        if (it != template_pools_.end() && !it->second.empty()) {
            size_t idx = ctx.UniformInt(0, it->second.size() - 1);
            result = std::regex_replace(result, std::regex("\\{" + placeholder + "\\}"),
                                      it->second[idx], std::regex_constants::format_first_only);
        }
    }

    return result;
}

std::string VarcharGenerator::GenerateCorpusText(RandomContext& ctx) {
    if (corpus_lines_.empty()) {
        return "";
    }

    size_t idx = ctx.UniformInt(0, corpus_lines_.size() - 1);
    return corpus_lines_[idx];
}

std::string VarcharGenerator::GenerateSingleUuid(RandomContext& ctx) {
    // simple UUID generation using RNG; for V4 use random hex; for V1 fallback to random as well (no MAC/time deps here)
    auto to_hex = [](uint32_t v, int width) {
        static const char* hex = "0123456789abcdef";
        std::string s(width, '0');
        for (int i = width - 1; i >= 0; --i) {
            s[i] = hex[v & 0xF];
            v >>= 4;
        }
        return s;
    };
    uint32_t a = static_cast<uint32_t>(ctx.GetRNG()());
    uint32_t b = static_cast<uint32_t>(ctx.GetRNG()());
    uint32_t c = static_cast<uint32_t>(ctx.GetRNG()());
    uint32_t d = static_cast<uint32_t>(ctx.GetRNG()());
    // Format 8-4-4-4-12 (lowercase hex)
    std::string uuid = to_hex(a, 8) + "-" + to_hex((b >> 16) & 0xFFFF, 4) + "-" +
                       to_hex((b & 0xFFFF) | 0x4000, 4) + "-" +
                       to_hex(((c >> 16) & 0x3FFF) | 0x8000, 4) + "-" +
                       to_hex(c & 0xFFFF, 4) + to_hex(d, 8);
    int max_len = config_.varchar_config.uuid_length > 0 ? config_.varchar_config.uuid_length : 36;
    if ((int)uuid.size() > max_len) uuid.resize(max_len);
    return uuid;
}

std::string VarcharGenerator::GenerateSingleTimestamp(RandomContext& ctx) {
    // generate timestamp using embedded timestamp config
    int64_t start = config_.varchar_config.ts_embedding.range.start;
    int64_t end = config_.varchar_config.ts_embedding.range.end;
    if (end <= start) {
        end = start + 1;
    }
    int64_t ts = ctx.UniformInt(start, end);
    if (config_.varchar_config.ts_embedding.jitter > 0) {
        int64_t jitter = ctx.UniformInt(-config_.varchar_config.ts_embedding.jitter,
                                        config_.varchar_config.ts_embedding.jitter);
        ts += jitter;
    }
    if (config_.varchar_config.ts_format == TimestampStringFormat::UNIX) {
        return std::to_string(ts);
    }
    // ISO8601 basic, UTC 'Z'
    std::time_t t = static_cast<std::time_t>(ts / 1000);
    std::tm gmt{};
#ifdef _WIN32
    gmtime_s(&gmt, &t);
#else
    gmt = *std::gmtime(&t);
#endif
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02dZ",
                  gmt.tm_year + 1900, gmt.tm_mon + 1, gmt.tm_mday,
                  gmt.tm_hour, gmt.tm_min, gmt.tm_sec);
    return std::string(buf);
}

std::string VarcharGenerator::ApplyKeywords(const std::string& text, RandomContext& ctx) {
    const auto& varchar_config = config_.varchar_config;
    std::string result = text;

    for (const auto& keyword : varchar_config.keywords) {
        if (ctx.Bernoulli(keyword.frequency)) {
            // Pick a random position in the string
            size_t rand_pos = ctx.UniformInt(0, result.length());
            // Move forward until a space or end of string
            size_t insert_pos = rand_pos;
            while (insert_pos < result.length() && result[insert_pos] != ' ') {
                ++insert_pos;
            }
            // Insert at word boundary (after the word or at end)
            // Insert with proper spacing
            std::string to_insert = keyword.token;
            // Add a space before if not at the start and not already preceded by a space
            if (insert_pos != 0 && result[insert_pos - 1] != ' ') {
                to_insert = " " + to_insert;
            }
            // Add a space after if not at the end and not already followed by a space
            if (insert_pos != result.size() && (insert_pos >= result.size() || result[insert_pos] != ' ')) {
                to_insert = to_insert + " ";
            }
            result.insert(insert_pos, to_insert);
        }
    }

    return result;
}

std::string VarcharGenerator::ApplyPhraseSets(const std::string& text, RandomContext& ctx) {
    const auto& varchar_config = config_.varchar_config;
    std::string result = text;

    for (const auto& phrase_set : varchar_config.phrase_sets) {
        // Randomly decide whether to include this phrase set
        if (ctx.Bernoulli(0.5)) {  // 50% chance, could be configurable
            std::stringstream phrase;
            for (size_t i = 0; i < phrase_set.size(); i++) {
                if (i > 0) phrase << " ";
                phrase << phrase_set[i];
            }
            std::string phrase_str = phrase.str();

            // Pick a random position in the string
            size_t rand_pos = ctx.UniformInt(0, result.length());
            // Move forward until a space or end of string
            size_t insert_pos = rand_pos;
            while (insert_pos < result.length() && result[insert_pos] != ' ') {
                ++insert_pos;
            }
            // Insert at word boundary (after the word or at end)
            // Insert with proper spacing
            std::string to_insert = phrase_str;
            if (insert_pos != 0 && result[insert_pos - 1] != ' ') {
                to_insert = " " + to_insert;
            }
            if (insert_pos != result.size() && (insert_pos >= result.size() || result[insert_pos] != ' ')) {
                to_insert = to_insert + " ";
            }
            result.insert(insert_pos, to_insert);
        }
    }

    return result;
}

std::string VarcharGenerator::TruncateToMaxLength(const std::string& text) {
    const auto& varchar_config = config_.varchar_config;

    if (varchar_config.max_length > 0 && text.length() > varchar_config.max_length) {
        return text.substr(0, varchar_config.max_length);
    }

    return text;
}

std::vector<std::string> VarcharGenerator::ParseTemplatePlaceholders(const std::string& tmpl) {
    std::vector<std::string> placeholders;
    std::string temp = tmpl;
    std::smatch match;

    while (std::regex_search(temp, match, template_regex_)) {
        placeholders.push_back(match[1].str());
        temp = match.suffix();
    }

    return placeholders;
}

} // namespace scalar_bench
} // namespace milvus