# Data Generation Schema Design

This document defines the schema for scalar benchmark data generation with a modular file organization. Data configurations are stored separately from benchmark cases, allowing reuse and better organization. The system supports multi-field data generation with rich patterns (enumerations, ranges, arrays, etc.) and per-field index configurations..

## File Organization

```
benchmark_cases/
├── benchmark_cases/          # Benchmark configuration files
│   ├── quick.yaml
│   ├── simple.yaml
│   └── comprehensive.yaml
└── data_configs/             # Reusable data configuration files
    ├── uniform_int64_high_card.yaml
    ├── zipf_int64_low_card.yaml
    ├── ecommerce_clicks.yaml
    └── ...
```

### Benchmark Case File Structure
Benchmark case files (in `benchmark_cases/benchmark_cases/`) reference data configurations by file path:

```yaml
version: 1
preset_name: quick
data_configs:
  - path: data_configs/uniform_int64_high_card.yaml
  - path: data_configs/zipf_int64_low_card.yaml
index_configs:
  - name: bitmap_index_config
    field_configs:
      user_id:
        type: BITMAP
        params: {chunk_size: "8192"}
      price:
        type: INVERTED
        params: {}
      # Fields not listed use NONE (no index)
expr_templates: [...]
test_params: [...]
```

### Data Configuration File Structure
Each data configuration file (in `benchmark_cases/data_configs/`) contains a single segment definition:

```yaml
name: ecommerce_clicks
segment_size: 1000000
segment_seed: 42  # Optional, for reproducibility
global_dictionaries:
  cities_small:
    items: ["Beijing", "Shanghai", "Shenzhen"]
fields:
  - field_name: user_id
    generator: categorical
    # ... field configuration
  - field_name: price
    generator: numeric
    # ... field configuration
```

## Supported Generators

### `categorical`
- **Purpose**: ID or enum columns (`city`, `label`, `user_id`).
- **Members**:
  - `type`: Milvus scalar type (`VARCHAR`, `INT64`, `INT32`). Defaults to `VARCHAR` when omitted. For `VARCHAR`, provide `max_length` so values adhere to the collection schema (default clamp is 64 if unspecified).
  - `values.dictionary`: Name of a dictionary declared under `global_dictionaries` or registered as a built-in (e.g., `uuid_v4_lower`, `h3_level8`). This defines the candidate token pool.
  - `values.inline`: Inline list of candidate tokens for enumerations. The generator uses only these predefined values to fill the segment.
  - `duplication_ratios`: List of ratios for each value, representing the percentage distribution. The sum should equal 1.0, and the list length should not exceed the number of available values (from dictionary or inline). Example: `duplication_ratios: [0.5, 0.3, 0.2]` means first value gets 50%, second gets 30%, third gets 20% of rows.
  - `max_length`: Absolute character cap for `VARCHAR` outputs; any longer dictionary values are truncated.
- **Generation order**: dictionary/inline sourcing → duplication ratio application → length truncation. The generator uses predefined values with specified distributions, ensuring the final column is schema-compliant.

### `numeric`
- **Purpose**: general scalar metrics (price, counts, scores).
- **Members**:
  - `type`: `INT64`, `FLOAT`, `DOUBLE`.
  - `range`: `min` / `max` or `min` / `max` per bucket.
  - `distribution`: `UNIFORM`, `NORMAL`, `ZIPF`, `CUSTOM_HIST`.
  - `buckets`: weighted subranges for composite distributions.
  - `outliers`: injection of extreme values with configurable ratio and explicit list.
  - `precision`: for floating types, number of decimal places.
  - `null_ratio`.

### `timestamp`
- **Purpose**: time-based filters (`time > ...`, date windows) stored as Unix epoch numbers.
- **Members**:
  - `range`: `start`, `end` expressed in epoch numbers.
  - `hotspots`: windows (with weight) to simulate burst traffic; window boundaries are epoch numbers.
  - `jitter`: optional random jitter (still measured in epoch units) to avoid perfectly uniform spacing.
  - *Note*: implementation can reuse the numeric generator pipeline—`timestamp` is a thin wrapper with epoch-specific defaults and helpers.

### `varchar`
- **Purpose**: Generate varchar strings with multiple words.
- **Members**:
  - `max_length`: Required upper bound for emitted strings.
  - `mode`: Generation mode:
    - `random`: Random combinations of tokens
      - `values.inline`
      - `values.dictionary`
      - `token_count`: Number of tokens (for `random` mode):
        - `min`, `max`, `distribution`
      - `keywords`: Required tokens with frequency guarantees.
        ```yaml
        keywords:
          - token: "AI"
            frequency: 0.3
        ```
      - `phrase_sets`: Groups of words that appear together.
    - `template`: Template-based generation with placeholders
      - `template`: Template string with placeholders (for `template` mode). Example: `"{brand} {model} - {feature}"`
      - `pools`: Named token lists for template placeholders:
        ```yaml
        pools:
          brand: ["TechCorp", "InnovateCo"]
          model: ["X1", "Pro", "Plus"]
        ```
    - `corpus`: Load from corpus file, will trim if line exceeds `max_length`.
      - `corpus_file`: Path to text file (for `corpus` mode, one entry per line).
- **Examples**:
  ```yaml
  # Random mode - multiple random tokens
  - field_name: "search_content"
    generator: varchar
    max_length: 512
    mode: random
    values:
      inline: ["data", "science", "machine", "learning", "neural", "network"]
    token_count:
      min: 10
      max: 50
    keywords:
      - token: "AI"
        frequency: 0.3

  # Template mode - structured text
  - field_name: "product_title"
    generator: varchar
    max_length: 128
    mode: template
    template: "{adjective} {product_type} - {brand} {model}"
    pools:
      adjective: ["Premium", "Professional"]
      product_type: ["Laptop", "Desktop"]
      brand: ["TechPro", "UltraMax"]
      model: ["X1", "Pro"]

  # Corpus mode - from file
  - field_name: "description"
    generator: varchar
    max_length: 512
    mode: corpus
    corpus_file: datasets/descriptions.txt
  ```

### `array`
- **Purpose**: arrays (`ARRAY_CONTAINS`, `ARRAY_CONTAINS_ANY`).
- **Members**:
  - `element`: nested generator specification; reuse any scalar generator type defined in this document (categorical, numeric, timestamp, varchar, boolean). Nested arrays are intentionally disallowed for now.
  - `length`: `min`, `max`, `distribution`, `avg` for Poisson-like lengths.
  - `max_capacity`: optional hard cap for array length (cannot exceed collection schema limits); defaults to `length.max` when omitted.
  - `contains`: rules ensuring inclusion/exclusion of certain tokens with probability (`include`, `exclude`).
  - `unique`: whether elements within a row must be unique.

### `boolean`
- **Purpose**: `isShow`, `isVectorized`, etc.
- **Members**:
  - `true_ratio` (default 0.5).

## Dictionary Sources

The `values.dictionary` reference resolves to one of three sources:

1. **Inline dictionary** – declare directly under `global_dictionaries`:
   ```yaml
   global_dictionaries:
     cities_small:
       items: ["Beijing", "Shanghai", "Shenzhen"]
   ```

2. **External file** – provide a newline-delimited text file and point to it:
   ```yaml
   global_dictionaries:
     ecommerce_tags:
       items_file: datasets/tags.txt   # relative to preset directory
   ```
   Each line in `items_file` becomes one token. Comments and blank lines are ignored.

When a field specifies `values.dictionary: cities_small`, the loader first checks the preset’s `global_dictionaries` block, then the built-in registry. If neither exists, configuration loading fails.

## Complete Example: Ecommerce Benchmark

### Benchmark Case File: `benchmark_cases/benchmark_cases/ecommerce_benchmark.yaml`

```yaml
version: 1
preset_name: ecommerce_benchmark
data_configs:
  - path: data_configs/ecommerce_clicks.yaml
  - path: data_configs/ecommerce_products.yaml
index_configs:
  - name: standard_indexes
    field_configs:
      user_id:
        type: BITMAP
        params: {chunk_size: "8192"}
      location_h3:
        type: INVERTED
        params: {}
      price:
        type: STL_SORT
        params: {}
      tags:
        type: INVERTED
        params: {}
      product_title:
        type: INVERTED
        params: {analyzer: "standard"}
      # Other fields use NONE (no index)
  - name: optimized_indexes
    field_configs:
      user_id:
        type: INVERTED
        params: {}
      price:
        type: BITMAP
        params: {chunk_size: "4096"}
expr_templates:
  - name: price_range
    type: RANGE
    expr_template: |
      # Template uses {field:price} for field name substitution
query_values: []
test_params:
  warmup_iterations: 5
  test_iterations: 100
```

### Data Config File: `benchmark_cases/data_configs/ecommerce_clicks.yaml`

```yaml
name: ecommerce_clicks
segment_size: 1000000
segment_seed: 42
global_dictionaries:
  cities_small:
    items: ["Beijing", "Shanghai", "Shenzhen", "Guangzhou", "Chengdu", "Hangzhou"]
  ecommerce_tags:
    items_file: datasets/tags.txt
fields:
  - field_name: "user_id"
    generator: categorical
    type: VARCHAR
    max_length: 36
    values:
      dictionary: uuid_v4_lower
    duplication_ratios: [0.4, 0.3, 0.2, 0.1]  # Top 4 UUIDs get different distributions
  - field_name: "location_h3"
    generator: categorical
    values:
      dictionary: h3_level8
    duplication_ratios: [0.3, 0.2, 0.15, 0.15, 0.1, 0.1]  # Top 6 H3 cells with varying frequencies
  - field_name: "price"
    generator: numeric
    type: FLOAT
    range: { min: 1.0, max: 5000.0 }
    distribution: CUSTOM_HIST
    buckets:
      - { weight: 0.6, min: 1.0, max: 200.0 }
      - { weight: 0.3, min: 200.0, max: 1000.0 }
      - { weight: 0.1, min: 1000.0, max: 5000.0 }
    outliers:
      ratio: 0.002
      values: [9999.0]
  - field_name: "tags"
    generator: array
    max_capacity: 8
    element:
      generator: categorical
      type: VARCHAR
      max_length: 32
      values:
        dictionary: ecommerce_tags
      duplication_ratios: [0.3, 0.2, 0.2, 0.15, 0.1, 0.05]  # Distribution for top 6 tags
    length: { min: 1, max: 5, distribution: ZIPF }
    contains:
      - include: ["sale", "discount"]
        probability: 0.15
    unique: true
  - field_name: "created_at"
    generator: timestamp
    unit: milliseconds
    range:
      start: 1704067200000   # 2024-01-01 UTC
      end: 1727731199000     # 2024-10-30 UTC
    hotspots:
      - { window: { start: 1711046400000, end: 1713657600000 }, weight: 0.35 }
      - { window: { start: 1720396800000, end: 1721087999000 }, weight: 0.25 }
  - field_name: "product_title"
    generator: varchar
    max_length: 128
    mode: template
    template: "{adjective} {product_type} - {brand} {model}"
    pools:
      adjective: ["Premium", "Professional", "Portable", "Advanced"]
      product_type: ["Laptop", "Desktop", "Monitor", "Keyboard"]
      brand: ["TechPro", "UltraMax", "PowerTech"]
      model: ["X1", "Pro", "Plus", "Elite"]
  - field_name: "primary_tag"
    generator: categorical
    type: VARCHAR
    max_length: 32
    values:
      inline: ["electronics", "computers", "accessories", "gaming", "office", "home"]
    duplication_ratios: [0.3, 0.25, 0.2, 0.15, 0.08, 0.02]
  - field_name: "search_text"
    generator: varchar
    max_length: 256
    mode: random
    values:
      inline: ["laptop", "desktop", "monitor", "keyboard", "mouse",
                "portable", "wireless", "bluetooth", "USB", "HDMI",
                "4K", "HD", "ultra", "pro", "gaming", "office"]
    token_count:
      min: 5
      max: 25
      distribution: UNIFORM
    keywords:
      - token: "sale"
        frequency: 0.25
      - token: "discount"
        frequency: 0.15
    phrase_sets:
      - ["free", "shipping"]
      - ["best", "seller"]
  - field_name: "description"
    generator: varchar
    max_length: 512
    mode: corpus
    corpus_file: datasets/product_descriptions.txt
  - field_name: "is_active"
    generator: boolean
    true_ratio: 0.55
```
