#pragma once

#include <string>
#include <vector>
#include <map>
#include <memory>
#include <functional>
#include <random>
#include <mutex>

namespace milvus {
namespace scalar_bench {

// Forward declaration
class Dictionary;

// Type alias for dictionary generator function
using DictionaryGenerator = std::function<std::vector<std::string>(size_t count, uint32_t seed)>;

class DictionaryRegistry {
public:
    static DictionaryRegistry& GetInstance();

    // Register different dictionary types
    void RegisterInlineDictionary(const std::string& name, const std::vector<std::string>& items);
    void RegisterFileDictionary(const std::string& name, const std::string& path);
    void RegisterBuiltinDictionary(const std::string& name, DictionaryGenerator generator);

    // Get dictionary items (with caching and lazy loading)
    std::vector<std::string> GetDictionary(const std::string& name, uint32_t seed = 0);

    // Check if dictionary exists
    bool HasDictionary(const std::string& name) const;

    // Clear all registered dictionaries (useful for testing)
    void Clear();

    // Initialize built-in dictionaries
    void InitializeBuiltins();

private:
    DictionaryRegistry() = default;
    ~DictionaryRegistry() = default;

    // Disable copy
    DictionaryRegistry(const DictionaryRegistry&) = delete;
    DictionaryRegistry& operator=(const DictionaryRegistry&) = delete;

    // Dictionary storage
    mutable std::mutex mutex_;
    std::map<std::string, std::unique_ptr<Dictionary>> dictionaries_;

    // Cache for shuffled dictionaries (key: name + seed)
    mutable std::map<std::string, std::vector<std::string>> cache_;

    // Deterministic shuffle
    std::vector<std::string> ShuffleDictionary(const std::vector<std::string>& items, uint32_t seed) const;
};

// Base dictionary class
class Dictionary {
public:
    virtual ~Dictionary() = default;
    virtual std::vector<std::string> GetItems() const = 0;
    virtual size_t GetSize() const = 0;
};

// Inline dictionary (stores items directly)
class InlineDictionary : public Dictionary {
public:
    explicit InlineDictionary(const std::vector<std::string>& items) : items_(items) {}

    std::vector<std::string> GetItems() const override { return items_; }
    size_t GetSize() const override { return items_.size(); }

private:
    std::vector<std::string> items_;
};

// File-based dictionary (lazy loading)
class FileDictionary : public Dictionary {
public:
    explicit FileDictionary(const std::string& path) : path_(path), loaded_(false) {}

    std::vector<std::string> GetItems() const override;
    size_t GetSize() const override;

private:
    std::string path_;
    mutable std::vector<std::string> items_;
    mutable bool loaded_;
    mutable std::mutex mutex_;

    void LoadIfNeeded() const;
};

// Built-in dictionary (generated on demand)
class BuiltinDictionary : public Dictionary {
public:
    BuiltinDictionary(DictionaryGenerator generator, size_t default_count = 10000)
        : generator_(generator), default_count_(default_count) {}

    std::vector<std::string> GetItems() const override;
    size_t GetSize() const override { return default_count_; }

private:
    DictionaryGenerator generator_;
    size_t default_count_;
};

// Built-in dictionary generators
namespace generators {
    // UUID generators
    std::vector<std::string> GenerateUUIDv4Lower(size_t count, uint32_t seed);
    std::vector<std::string> GenerateUUIDv4Upper(size_t count, uint32_t seed);

    // H3 cell generators
    std::vector<std::string> GenerateH3Level8(size_t count, uint32_t seed);
    std::vector<std::string> GenerateH3Level10(size_t count, uint32_t seed);

    // Sequential generators
    std::vector<std::string> GenerateSequentialNumbers(size_t count, uint32_t seed);
    std::vector<std::string> GenerateSequentialPadded(size_t count, uint32_t seed);

    // Common word lists
    std::vector<std::string> GenerateEnglishNouns(size_t count, uint32_t seed);
    std::vector<std::string> GenerateEnglishVerbs(size_t count, uint32_t seed);
    std::vector<std::string> GenerateEnglishAdjectives(size_t count, uint32_t seed);

    // Location generators
    std::vector<std::string> GenerateCityNames(size_t count, uint32_t seed);
    std::vector<std::string> GenerateCountryNames(size_t count, uint32_t seed);

    // Email/domain generators
    std::vector<std::string> GenerateEmailDomains(size_t count, uint32_t seed);
}

} // namespace scalar_bench
} // namespace milvus