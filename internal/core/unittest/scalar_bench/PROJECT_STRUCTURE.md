# Scalar Benchmark Project Structure

## Directory Organization

The scalar benchmark project has been reorganized into a modular structure for better maintainability and scalability.

### Main Directory
- `main.cpp` - Entry point for the benchmark application
- `scalar_filter_benchmark.cpp/h` - Main benchmark execution logic
- `test_config_loader.cpp` - Test utility for config loading
- `CMakeLists.txt` - Build configuration

### `/config`
Configuration management and loading
- `benchmark_config.h` - All configuration structures
- `benchmark_config_loader.cpp/h` - YAML config loading implementation

### `/core`
Core benchmark functionality
- `segment_wrapper.cpp/h` - Milvus segment management
- `segment_data.cpp/h` - Data storage structures
- `index_wrapper.cpp/h` - Index building and management
- `query_executor.cpp/h` - Query execution logic
- `data_generator.cpp/h` - Legacy data generation (being replaced)

### `/dictionaries`
Dictionary management for data generation
- `dictionary_registry.cpp/h` - Centralized dictionary management with built-in generators

### `/generators`
Field data generators for multi-field support
- `field_generator.h` - Base interface and factory
- `field_generator_factory.cpp` - Factory implementation
- `categorical_generator.cpp/h` - Categorical/enum field generation
- `numeric_generator.cpp/h` - Numeric field generation (int, float, double)
- `timestamp_generator.cpp/h` - Timestamp field generation
- `varchat_generator.cpp/h` - Varchar field generation (random, template, corpus)
- `array_generator.cpp/h` - Array field generation
- `boolean_generator.cpp/h` - Boolean field generation

### `/utils`
Utility functions
- `bench_paths.cpp/h` - Path management utilities
- `flame_graph_profiler.cpp/h` - Performance profiling utilities

### `/presets`
Configuration presets and data
- `/benchmark_cases` - Benchmark configuration files
- `/data_configs` - Reusable data configuration files
- `/datasets` - Dictionary files and corpus data

### `/_artifacts`
Runtime generated files (git-ignored)
- `/results` - Benchmark results
- `/segments` - Generated segment data
- `/storage` - Milvus storage files
- `/temp` - Temporary files

### `/ui`
React-based UI for benchmark visualization
- `/src` - Source code
- `/dist` - Built distribution
- `/node_modules` - Dependencies

## Build System

The CMakeLists.txt has been updated to automatically collect source files from all subdirectories. The build system maintains the same targets and dependencies as before.

## Include Path Convention

All includes use relative paths from the scalar_bench directory:
- Within same directory: `#include "file.h"`
- From subdirectory: `#include "subdir/file.h"`
- From parent: `#include "../subdir/file.h"`

## Development Guidelines

1. Place new generators in `/generators`
2. Configuration structures go in `/config`
3. Core benchmark logic in `/core`
4. Utilities in `/utils`
5. Keep main directory minimal - only top-level files