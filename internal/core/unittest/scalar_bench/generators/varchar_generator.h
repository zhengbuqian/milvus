#pragma once

#include "field_generator.h"
#include "../dictionaries/dictionary_registry.h"
#include <regex>

namespace milvus {
namespace scalar_bench {

class VarcharGenerator : public IFieldGenerator {
public:
    explicit VarcharGenerator(const FieldConfig& config);

    FieldColumn Generate(size_t num_rows, RandomContext& ctx) override;
    const FieldConfig& GetConfig() const override { return config_; }

private:
    FieldConfig config_;
    std::vector<std::string> token_pool_;  // For random mode
    std::map<std::string, std::vector<std::string>> template_pools_;  // For template mode
    std::vector<std::string> corpus_lines_;  // For corpus mode
    std::regex template_regex_;  // For parsing template placeholders

    void Initialize();

    // Mode-specific generators
    std::string GenerateRandomText(RandomContext& ctx);
    std::string GenerateTemplateText(RandomContext& ctx);
    std::string GenerateCorpusText(RandomContext& ctx);

    // Helper methods
    void LoadTokenPool();
    void LoadTemplatePools();
    void LoadCorpus();
    std::string ApplyKeywords(const std::string& text, RandomContext& ctx);
    std::string ApplyPhraseSets(const std::string& text, RandomContext& ctx);
    std::string TruncateToMaxLength(const std::string& text);
    std::vector<std::string> ParseTemplatePlaceholders(const std::string& tmpl);
};

} // namespace scalar_bench
} // namespace milvus