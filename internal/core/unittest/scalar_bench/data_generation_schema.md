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
Benchmark case files (in `benchmark_cases/benchmark_cases/`) use a preset-based structure with reusable configurations:

```yaml
version: 1
test_params:
  warmup_iterations: 30
  test_iterations: 100
  collect_memory_stats: false
  enable_flame_graph: false
  flamegraph_repo_path: ~/FlameGraph

preset_data_configs:
  - name: low_card
    path: data_configs/uniform_int64_low_card.yaml
  - name: high_card
    path: data_configs/uniform_int64_high_card.yaml

preset_index_configs:
  - name: no_index
    field_configs: {}
  - name: bitmap_index
    field_configs:
      user_id:
        type: BITMAP
  - name: inverted_index
    field_configs:
      user_id:
        type: INVERTED
        params: {}

# Multiple cases allow different expression types to reuse same data/index configs
# Multiple suites allow same expression types with different data/expressions for different selectivity
# Outputs from different suites of the same case go into the same test result
# Outputs from different cases go into separate test results
cases:
  - name: int_range
    suites:
      - name: low_card
        data_configs: [low_card]  # Reference preset by name
        index_configs: [no_index, bitmap_index, inverted_index]  # Reference presets
        expr_templates:
          - name: user_id_range_1%
            expr_template: 99 < user_id <= 100
  - name: int_equal
    suites:
      - name: default
        data_configs: [low_card, high_card]
        index_configs: [no_index, bitmap_index]
        expr_templates:
          - name: user_id_equal_100
            expr_template: user_id == 100
```

**Key Design Principles:**
- `preset_data_configs` and `preset_index_configs` define reusable configuration templates at the top level
- `cases` group tests by expression type (e.g., range, equal, in, like)
- `suites` within each case specify different data/index combinations and expressions
- Suites reference presets by name in arrays, allowing flexible combinations
- All suites within a case share the same output file/report
- Different cases produce separate output files/reports

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

If the `fields` array does not explicitly include a `pk` field, the generator still creates a synthetic INT64 primary key column. `SegmentDataGenerator::GenerateMultiFieldData` pre-populates the column with `0..segment_size-1`, so downstream steps always have a primary key available. When you define a `pk` field in the configuration, that user-provided column overrides the default.

## Supported Generators

### `categorical`
- **Purpose**: ID or enum columns (`city`, `label`, `user_id`).
- **Members**:
  - `type`: Milvus scalar type (`VARCHAR`, `INT64`, `INT32`). Defaults to `VARCHAR` when omitted. For `VARCHAR`, provide `max_length` so values adhere to the collection schema (default clamp is 64 if unspecified).
  - `values.dictionary`: Name of a dictionary declared under `global_dictionaries` or registered as a built-in (e.g., `uuid_v4_lower`, `h3_level8`). This defines the candidate token pool.
  - `values.inline`: Inline list of candidate tokens for enumerations. The generator uses only these predefined values to fill the segment.
  - `values.pick`: Integer. When used with `values.dictionary`, restricts the candidate pool to the first N tokens from the resolved dictionary order.
  - `values.random_pick`: Integer. When used with `values.dictionary`, restricts the candidate pool to N randomly selected tokens from the resolved dictionary (without replacement; respects `segment_seed` when available).
    - Constraints: `values.pick` and `values.random_pick` are mutually exclusive. When `values.inline` is used, neither may be set.
  - `duplication_ratios`: List of ratios for each value, representing the percentage distribution. The sum should equal 1.0, and the list length should not exceed the number of available values (from dictionary or inline). Example: `duplication_ratios: [0.5, 0.3, 0.2]` means first value gets 50%, second gets 30%, third gets 20% of rows.
  - `max_length`: Absolute character cap for `VARCHAR` outputs; any longer dictionary values are truncated.
- **Generation order**: dictionary/inline sourcing → candidate sub-selection (`values.pick`/`values.random_pick`) → duplication ratio application → length truncation. The generator uses predefined values with specified distributions, ensuring the final column is schema-compliant.

### `numeric`
- **Purpose**: general scalar metrics (price, counts, scores).
- **Members**:
  - `type`: `INT64`, `FLOAT`, `DOUBLE`.
  - `range`: `min` / `max` or `min` / `max` per bucket.
  - `distribution`: `UNIFORM`, `NORMAL`, `ZIPF`, `CUSTOM_HIST`, `SEQUENTIAL`.
  - `step` (SEQUENTIAL only): increment between consecutive values. Defaults to `1`.
    - Starts from `range.min` and produces `size` values: `min, min+step, ...`
    - If `range.min + (size-1)*step` exceeds `range.max` (for positive step) or is less than `range.max` (for negative step), configuration is invalid and generation fails.
  - `buckets`: weighted subranges for composite distributions.
  - `outliers`: injection of extreme values with configurable ratio and explicit list.
    - `ratio`: ratio of outliers to the total number of values.
    - `values`: list of outlier values.
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
      - `values.pick` (with `values.dictionary` only): Integer. Use only the first N tokens from the dictionary as the token pool.
      - `values.random_pick` (with `values.dictionary` only): Integer. Use N randomly selected tokens from the dictionary as the token pool (without replacement; respects seed when available).
        - Constraints: Do not set both `values.pick` and `values.random_pick`. When `values.inline` is used, neither may be set.
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

### Candidate sub-selection for dictionaries

For any generator that supports `values.dictionary` (e.g., `categorical`, `varchar` in `random` mode, nested `element` generators), you may optionally restrict the candidate pool using one of:

- `values.pick`: Integer. Use only the first N tokens from the resolved dictionary order (inline list order, file order for `items_file`, or built-in order).
- `values.random_pick`: Integer. Use N randomly selected tokens from the resolved dictionary, without replacement. Randomness respects `segment_seed` when available for reproducibility.

Rules:
- `values.pick` and `values.random_pick` are mutually exclusive.
- When `values.inline` is provided, neither `values.pick` nor `values.random_pick` may be set.

## Data Config Override Mechanism

The override mechanism allows you to reuse existing data configuration files while modifying specific parameters. This is useful when you want to create variations of a base configuration without duplicating the entire file.

### Basic Usage

In `preset_data_configs`, you can specify an optional `override` block to modify parameters from the referenced data config file:

```yaml
preset_data_configs:
  - name: base_config
    path: data_configs/base.yaml
  - name: modified_config
    path: data_configs/base.yaml
    override:
      name: modified_version  # Override the name
      segment_size: 500000    # Override segment size
      fields:
        - true_ratio: 0.9     # Override first field's parameter
```

### Override Behavior by Node Type

The override mechanism uses recursive merging with different behaviors for different YAML node types:

#### 1. Map Nodes (Objects)
For map/object nodes, the override performs **key-level merging**:
- Existing keys are recursively updated
- New keys are added
- Unlisted keys remain unchanged

Example:
```yaml
# Base config
fields:
  - field_name: status
    generator: boolean
    true_ratio: 0.5

# Override
override:
  fields:
    - true_ratio: 0.9  # Only modify true_ratio

# Result
fields:
  - field_name: status     # Preserved
    generator: boolean     # Preserved
    true_ratio: 0.9        # Modified
```

#### 2. Sequence Nodes (Arrays)
For sequence/array nodes, the override performs **index-based element merging**:
- Each override element is applied to the corresponding base element by index
- The override sequence must not be longer than the base sequence
- Unlisted elements remain unchanged

Example:
```yaml
# Base config with 3 fields
fields:
  - field_name: field1
    generator: boolean
    true_ratio: 0.5
  - field_name: field2
    generator: numeric
    range: {min: 0, max: 100}
  - field_name: field3
    generator: boolean
    true_ratio: 0.6

# Override only the second field
override:
  fields:
    - {}  # Empty object - skip first field (no changes)
    - range: {min: 0, max: 500}  # Modify second field's range
    # Third field not mentioned - remains unchanged

# Result
fields:
  - field_name: field1
    generator: boolean
    true_ratio: 0.5          # Unchanged (empty object placeholder)
  - field_name: field2
    generator: numeric
    range: {min: 0, max: 500}  # Modified
  - field_name: field3
    generator: boolean
    true_ratio: 0.6          # Unchanged (not in override)
```

**Important**: Using an empty object `{}` as a placeholder is safe. The empty map has no keys to iterate, so no modifications are made to the base element.

#### 3. Scalar Nodes (Primitives)
For scalar nodes (strings, numbers, booleans), the override performs **direct replacement**:

```yaml
# Base
segment_size: 1000000

# Override
override:
  segment_size: 500000

# Result: 500000
```

### Practical Examples

#### Example 1: Varying Boolean Distribution

```yaml
preset_data_configs:
  - name: balanced
    path: data_configs/uniform_bool_5050.yaml
  - name: skewed_9010
    path: data_configs/uniform_bool_5050.yaml
    override:
      name: skewed_9010
      fields:
        - true_ratio: 0.9
  - name: most_true
    path: data_configs/uniform_bool_5050.yaml
    override:
      name: most_true
      fields:
        - true_ratio: 0.999
```

#### Example 2: Adjusting Array Element Dictionary Size

```yaml
preset_data_configs:
  - name: card_100
    path: data_configs/array_base.yaml
  - name: card_500
    path: data_configs/array_base.yaml
    override:
      name: card_500
      fields:
        - element:
            values:
              pick: 500  # Use 500 dictionary tokens instead of default
```

#### Example 3: Modifying Specific Field in Multi-Field Config

When the base config has multiple fields and you only want to override a specific one:

```yaml
# Base config has 4 fields
preset_data_configs:
  - name: modified_third_field
    path: data_configs/multi_field_base.yaml
    override:
      fields:
        - {}  # Skip field 1 (no changes)
        - {}  # Skip field 2 (no changes)
        - range: {min: 0, max: 1000}  # Modify field 3
        # Field 4 unchanged (not mentioned)
```

### Type Safety

The override mechanism enforces type compatibility:
- Cannot override a Map with a Sequence
- Cannot override a Sequence with a Scalar
- Cannot override a Scalar with a Map
- Type mismatches will cause configuration loading to fail with a clear error message

### Best Practices

1. **Use descriptive names**: When overriding, always provide a new `name` to clearly identify the variant
2. **Minimal overrides**: Only specify parameters that differ from the base config
3. **Empty object placeholders**: Use `{}` to skip array elements you don't want to modify
4. **Validate early**: Run a quick test after defining overrides to ensure they work as expected

## Complete Example: Ecommerce Benchmark

### Benchmark Case File: `benchmark_cases/benchmark_cases/ecommerce_benchmark.yaml`

```yaml
version: 1
test_params:
  warmup_iterations: 5
  test_iterations: 100
  collect_memory_stats: false
  enable_flame_graph: false
  flamegraph_repo_path: ~/FlameGraph

preset_data_configs:
  - name: clicks
    path: data_configs/ecommerce_clicks.yaml
  - name: products
    path: data_configs/ecommerce_products.yaml

preset_index_configs:
  - name: no_index
    field_configs: {}
  - name: standard_indexes
    field_configs:
      user_id:
        type: BITMAP
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
  - name: optimized_indexes
    field_configs:
      user_id:
        type: INVERTED
        params: {}
      price:
        type: BITMAP

cases:
  - name: price_range
    suites:
      - name: default
        data_configs: [clicks, products]
        index_configs: [no_index, standard_indexes, optimized_indexes]
        expr_templates:
          - name: price_low
            expr_template: price < 100.0
          - name: price_mid
            expr_template: price >= 100.0 && price < 1000.0
          - name: price_high
            expr_template: price >= 1000.0
  - name: tag_search
    suites:
      - name: default
        data_configs: [clicks]
        index_configs: [standard_indexes]
        expr_templates:
          - name: single_tag
            expr_template: array_contains(tags, "sale")
          - name: multi_tag
            expr_template: array_contains_any(tags, ["sale", "discount"])
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
