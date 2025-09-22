#include "dictionary_registry.h"
#include <fstream>
#include <sstream>
#include <algorithm>
#include <random>
#include <iomanip>
#include <filesystem>
#include <stdexcept>

namespace milvus {
namespace scalar_bench {

// Singleton implementation
DictionaryRegistry& DictionaryRegistry::GetInstance() {
    static DictionaryRegistry instance;
    static std::once_flag init_flag;
    std::call_once(init_flag, [&]() {
        instance.InitializeBuiltins();
    });
    return instance;
}

void DictionaryRegistry::RegisterInlineDictionary(const std::string& name, const std::vector<std::string>& items) {
    std::lock_guard<std::mutex> lock(mutex_);
    dictionaries_[name] = std::make_unique<InlineDictionary>(items);
    // Clear cache for this dictionary
    auto it = cache_.begin();
    while (it != cache_.end()) {
        if (it->first.find(name + ":") == 0) {
            it = cache_.erase(it);
        } else {
            ++it;
        }
    }
}

void DictionaryRegistry::RegisterFileDictionary(const std::string& name, const std::string& path) {
    std::lock_guard<std::mutex> lock(mutex_);
    dictionaries_[name] = std::make_unique<FileDictionary>(path);
    // Clear cache for this dictionary
    auto it = cache_.begin();
    while (it != cache_.end()) {
        if (it->first.find(name + ":") == 0) {
            it = cache_.erase(it);
        } else {
            ++it;
        }
    }
}

void DictionaryRegistry::RegisterBuiltinDictionary(const std::string& name, DictionaryGenerator generator) {
    std::lock_guard<std::mutex> lock(mutex_);
    dictionaries_[name] = std::make_unique<BuiltinDictionary>(generator);
}

std::vector<std::string> DictionaryRegistry::GetDictionary(const std::string& name, uint32_t seed) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Check cache first
    std::string cache_key = name + ":" + std::to_string(seed);
    auto cache_it = cache_.find(cache_key);
    if (cache_it != cache_.end()) {
        return cache_it->second;
    }

    // Find dictionary
    auto dict_it = dictionaries_.find(name);
    if (dict_it == dictionaries_.end()) {
        throw std::runtime_error("Dictionary not found: " + name);
    }

    // Get items
    std::vector<std::string> items = dict_it->second->GetItems();

    // Apply deterministic shuffle if seed is provided
    if (seed != 0) {
        items = ShuffleDictionary(items, seed);
    }

    // Cache and return
    cache_[cache_key] = items;
    return items;
}

bool DictionaryRegistry::HasDictionary(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return dictionaries_.find(name) != dictionaries_.end();
}

void DictionaryRegistry::Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    dictionaries_.clear();
    cache_.clear();
}

void DictionaryRegistry::InitializeBuiltins() {
    // UUID generators
    RegisterBuiltinDictionary("uuid_v4_lower", generators::GenerateUUIDv4Lower);
    RegisterBuiltinDictionary("uuid_v4_upper", generators::GenerateUUIDv4Upper);

    // H3 cell generators
    RegisterBuiltinDictionary("h3_level8", generators::GenerateH3Level8);
    RegisterBuiltinDictionary("h3_level10", generators::GenerateH3Level10);

    // Sequential generators
    RegisterBuiltinDictionary("sequential_numbers", generators::GenerateSequentialNumbers);
    RegisterBuiltinDictionary("sequential_padded", generators::GenerateSequentialPadded);

    // Common word lists
    RegisterBuiltinDictionary("english_nouns", generators::GenerateEnglishNouns);
    RegisterBuiltinDictionary("english_verbs", generators::GenerateEnglishVerbs);
    RegisterBuiltinDictionary("english_adjectives", generators::GenerateEnglishAdjectives);

    // Location generators
    RegisterBuiltinDictionary("city_names", generators::GenerateCityNames);
    RegisterBuiltinDictionary("country_names", generators::GenerateCountryNames);

    // Email/domain generators
    RegisterBuiltinDictionary("email_domains", generators::GenerateEmailDomains);
}

std::vector<std::string> DictionaryRegistry::ShuffleDictionary(const std::vector<std::string>& items, uint32_t seed) const {
    std::vector<std::string> result = items;
    std::mt19937 rng(seed);
    std::shuffle(result.begin(), result.end(), rng);
    return result;
}

// FileDictionary implementation
std::vector<std::string> FileDictionary::GetItems() const {
    LoadIfNeeded();
    return items_;
}

size_t FileDictionary::GetSize() const {
    LoadIfNeeded();
    return items_.size();
}

void FileDictionary::LoadIfNeeded() const {
    if (loaded_) return;

    std::lock_guard<std::mutex> lock(mutex_);
    if (loaded_) return; // Double-check

    std::ifstream file(path_);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open dictionary file: " + path_);
    }

    std::string line;
    while (std::getline(file, line)) {
        // Skip empty lines and comments
        if (line.empty() || line[0] == '#') continue;

        // Trim whitespace
        size_t first = line.find_first_not_of(" \t\r\n");
        size_t last = line.find_last_not_of(" \t\r\n");
        if (first != std::string::npos) {
            items_.push_back(line.substr(first, last - first + 1));
        }
    }

    loaded_ = true;
}

// BuiltinDictionary implementation
std::vector<std::string> BuiltinDictionary::GetItems() const {
    // Generate with seed 0 for consistency
    return generator_(default_count_, 0);
}

namespace generators {

// Helper function to generate UUID v4
static std::string GenerateUUID(std::mt19937& rng, bool lowercase) {
    std::uniform_int_distribution<int> dist(0, 15);

    const char* hex = lowercase ? "0123456789abcdef" : "0123456789ABCDEF";
    std::stringstream ss;

    for (int i = 0; i < 36; i++) {
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            ss << '-';
        } else if (i == 14) {
            ss << '4'; // Version 4
        } else if (i == 19) {
            ss << hex[(dist(rng) & 0x3) | 0x8]; // Variant
        } else {
            ss << hex[dist(rng)];
        }
    }

    return ss.str();
}

std::vector<std::string> GenerateUUIDv4Lower(size_t count, uint32_t seed) {
    std::vector<std::string> result;
    result.reserve(count);
    std::mt19937 rng(seed);

    for (size_t i = 0; i < count; i++) {
        result.push_back(GenerateUUID(rng, true));
    }

    return result;
}

std::vector<std::string> GenerateUUIDv4Upper(size_t count, uint32_t seed) {
    std::vector<std::string> result;
    result.reserve(count);
    std::mt19937 rng(seed);

    for (size_t i = 0; i < count; i++) {
        result.push_back(GenerateUUID(rng, false));
    }

    return result;
}

// H3 cell generators (simplified - generates hex strings that look like H3 cells)
std::vector<std::string> GenerateH3Level8(size_t count, uint32_t seed) {
    std::vector<std::string> result;
    result.reserve(count);
    std::mt19937 rng(seed);
    std::uniform_int_distribution<uint64_t> dist(0x08001fffffffffff, 0x080ffffffffffffff);

    for (size_t i = 0; i < count; i++) {
        std::stringstream ss;
        ss << std::hex << std::setfill('0') << std::setw(15) << dist(rng);
        result.push_back(ss.str());
    }

    return result;
}

std::vector<std::string> GenerateH3Level10(size_t count, uint32_t seed) {
    std::vector<std::string> result;
    result.reserve(count);
    std::mt19937 rng(seed);
    std::uniform_int_distribution<uint64_t> dist(0x0a001fffffffffff, 0x0a0ffffffffffffff);

    for (size_t i = 0; i < count; i++) {
        std::stringstream ss;
        ss << std::hex << std::setfill('0') << std::setw(15) << dist(rng);
        result.push_back(ss.str());
    }

    return result;
}

// Sequential generators
std::vector<std::string> GenerateSequentialNumbers(size_t count, uint32_t seed) {
    std::vector<std::string> result;
    result.reserve(count);
    uint64_t start = seed ? seed : 1;

    for (size_t i = 0; i < count; i++) {
        result.push_back(std::to_string(start + i));
    }

    return result;
}

std::vector<std::string> GenerateSequentialPadded(size_t count, uint32_t seed) {
    std::vector<std::string> result;
    result.reserve(count);
    uint64_t start = seed ? seed : 1;

    for (size_t i = 0; i < count; i++) {
        std::stringstream ss;
        ss << std::setfill('0') << std::setw(10) << (start + i);
        result.push_back(ss.str());
    }

    return result;
}

// Common word lists
std::vector<std::string> GenerateEnglishNouns(size_t count, uint32_t seed) {
    static const std::vector<std::string> nouns = {
        "time", "year", "people", "way", "day", "man", "thing", "woman", "life", "child",
        "world", "school", "state", "family", "student", "group", "country", "problem", "hand", "part",
        "place", "case", "week", "company", "system", "program", "question", "work", "government", "number",
        "night", "point", "home", "water", "room", "mother", "area", "money", "story", "fact",
        "month", "lot", "right", "study", "book", "eye", "job", "word", "business", "issue"
    };

    std::vector<std::string> result;
    result.reserve(count);
    std::mt19937 rng(seed);

    for (size_t i = 0; i < count; i++) {
        if (i < nouns.size()) {
            result.push_back(nouns[i]);
        } else {
            // Generate synthetic nouns
            result.push_back("noun" + std::to_string(i));
        }
    }

    // Shuffle
    std::shuffle(result.begin(), result.end(), rng);
    return result;
}

std::vector<std::string> GenerateEnglishVerbs(size_t count, uint32_t seed) {
    static const std::vector<std::string> verbs = {
        "be", "have", "do", "say", "get", "make", "go", "know", "take", "see",
        "come", "think", "look", "want", "give", "use", "find", "tell", "ask", "work",
        "seem", "feel", "try", "leave", "call", "run", "walk", "talk", "sit", "stand",
        "write", "read", "play", "move", "live", "believe", "hold", "bring", "happen", "write",
        "provide", "sit", "stand", "lose", "pay", "meet", "include", "continue", "set", "learn"
    };

    std::vector<std::string> result;
    result.reserve(count);
    std::mt19937 rng(seed);

    for (size_t i = 0; i < count; i++) {
        if (i < verbs.size()) {
            result.push_back(verbs[i]);
        } else {
            // Generate synthetic verbs
            result.push_back("verb" + std::to_string(i));
        }
    }

    std::shuffle(result.begin(), result.end(), rng);
    return result;
}

std::vector<std::string> GenerateEnglishAdjectives(size_t count, uint32_t seed) {
    static const std::vector<std::string> adjectives = {
        "good", "new", "first", "last", "long", "great", "little", "own", "other", "old",
        "right", "big", "high", "different", "small", "large", "next", "early", "young", "important",
        "few", "public", "bad", "same", "able", "political", "late", "general", "full", "special",
        "easy", "clear", "recent", "strong", "possible", "free", "common", "poor", "natural", "significant",
        "similar", "hot", "dead", "central", "happy", "serious", "ready", "simple", "left", "physical"
    };

    std::vector<std::string> result;
    result.reserve(count);
    std::mt19937 rng(seed);

    for (size_t i = 0; i < count; i++) {
        if (i < adjectives.size()) {
            result.push_back(adjectives[i]);
        } else {
            // Generate synthetic adjectives
            result.push_back("adj" + std::to_string(i));
        }
    }

    std::shuffle(result.begin(), result.end(), rng);
    return result;
}

// Location generators
std::vector<std::string> GenerateCityNames(size_t count, uint32_t seed) {
    static const std::vector<std::string> cities = {
        "Tokyo", "Delhi", "Shanghai", "Sao Paulo", "Mexico City", "Cairo", "Mumbai", "Beijing", "Dhaka", "Osaka",
        "New York", "Karachi", "Buenos Aires", "Chongqing", "Istanbul", "Kolkata", "Manila", "Lagos", "Rio de Janeiro", "Tianjin",
        "Kinshasa", "Guangzhou", "Los Angeles", "Moscow", "Shenzhen", "Lahore", "Bangalore", "Paris", "Bogota", "Jakarta",
        "Chennai", "Lima", "Bangkok", "Seoul", "Nagoya", "Hyderabad", "London", "Tehran", "Chicago", "Chengdu",
        "Nanjing", "Wuhan", "Ho Chi Minh City", "Luanda", "Ahmedabad", "Kuala Lumpur", "Xi'an", "Hong Kong", "Dongguan", "Hangzhou"
    };

    std::vector<std::string> result;
    result.reserve(count);
    std::mt19937 rng(seed);

    for (size_t i = 0; i < count; i++) {
        if (i < cities.size()) {
            result.push_back(cities[i]);
        } else {
            // Generate synthetic city names
            result.push_back("City" + std::to_string(i));
        }
    }

    std::shuffle(result.begin(), result.end(), rng);
    return result;
}

std::vector<std::string> GenerateCountryNames(size_t count, uint32_t seed) {
    static const std::vector<std::string> countries = {
        "China", "India", "United States", "Indonesia", "Pakistan", "Brazil", "Nigeria", "Bangladesh", "Russia", "Mexico",
        "Japan", "Ethiopia", "Philippines", "Egypt", "Vietnam", "Germany", "Turkey", "Iran", "Thailand", "United Kingdom",
        "France", "Italy", "Tanzania", "South Africa", "Myanmar", "Kenya", "South Korea", "Colombia", "Spain", "Uganda",
        "Argentina", "Algeria", "Sudan", "Ukraine", "Iraq", "Afghanistan", "Poland", "Canada", "Morocco", "Saudi Arabia",
        "Uzbekistan", "Peru", "Angola", "Malaysia", "Mozambique", "Ghana", "Yemen", "Nepal", "Venezuela", "Madagascar"
    };

    std::vector<std::string> result;
    result.reserve(count);
    std::mt19937 rng(seed);

    for (size_t i = 0; i < count; i++) {
        if (i < countries.size()) {
            result.push_back(countries[i]);
        } else {
            // Generate synthetic country names
            result.push_back("Country" + std::to_string(i));
        }
    }

    std::shuffle(result.begin(), result.end(), rng);
    return result;
}

// Email/domain generators
std::vector<std::string> GenerateEmailDomains(size_t count, uint32_t seed) {
    static const std::vector<std::string> domains = {
        "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com",
        "aol.com", "protonmail.com", "mail.com", "yandex.com", "qq.com",
        "163.com", "126.com", "sina.com", "live.com", "msn.com",
        "me.com", "mac.com", "fastmail.com", "tutanota.com", "zoho.com"
    };

    static const std::vector<std::string> tlds = {
        ".com", ".org", ".net", ".edu", ".gov", ".io", ".co", ".ai", ".dev", ".app"
    };

    std::vector<std::string> result;
    result.reserve(count);
    std::mt19937 rng(seed);

    // Add known domains first
    for (const auto& domain : domains) {
        if (result.size() >= count) break;
        result.push_back(domain);
    }

    // Generate synthetic domains if needed
    std::uniform_int_distribution<int> tld_dist(0, tlds.size() - 1);
    while (result.size() < count) {
        result.push_back("domain" + std::to_string(result.size()) + tlds[tld_dist(rng)]);
    }

    std::shuffle(result.begin(), result.end(), rng);
    return result;
}

} // namespace generators

} // namespace scalar_bench
} // namespace milvus