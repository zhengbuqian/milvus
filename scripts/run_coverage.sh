#!/bin/bash

# Script to build and run C++ tests with coverage

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR="$( cd "${SCRIPT_DIR}/.." && pwd )"

echo "============================================"
echo "Building C++ code with coverage enabled..."
echo "============================================"

# Clean previous coverage data
find ${ROOT_DIR} -name "*.gcda" -delete 2>/dev/null || true
find ${ROOT_DIR} -name "*.gcno" -delete 2>/dev/null || true

# Build with coverage
cd ${ROOT_DIR}
# make build-cpp-with-coverage mode=Debug use_asan=ON

echo "============================================"
echo "Running tests..."
echo "============================================"

# Run the tests
cd ${ROOT_DIR}/internal/core/output/unittest
LD_PRELOAD=$(gcc -print-file-name=libasan.so) ./all_tests ${@}

echo "============================================"
echo "Generating coverage report..."
echo "============================================"

cd ${ROOT_DIR}

# Find all source files that have coverage data
echo "Coverage data files (.gcda):"
find . -name "*.gcda" | head -20

# Generate coverage info using lcov
if command -v lcov &> /dev/null; then
    echo "Generating HTML coverage report with lcov..."
    
    # Capture coverage data
    lcov --capture \
         --directory cmake_build \
         --output-file coverage.info
    
    # Filter out system and test files
    lcov --remove coverage.info \
         '/usr/*' \
         '*/unittest/*' \
         '*/test/*' \
         '*/thirdparty/*' \
         '*/build/*' \
         '*/cmake_build/*' \
         --output-file coverage_filtered.info
    
    # Generate HTML report
    genhtml coverage_filtered.info \
            --output-directory coverage_report
    
    echo "HTML coverage report generated in: ${ROOT_DIR}/coverage_report/index.html"
else
    echo "lcov not found. Using gcov directly..."
    
    # Find source files and run gcov on them
    cd ${ROOT_DIR}/cmake_build
    for gcda_file in $(find . -name "*.gcda"); do
        dir=$(dirname "$gcda_file")
        cd "$dir"
        gcov *.gcda 2>/dev/null || true
        cd - > /dev/null
    done
    
    echo "gcov reports generated in cmake_build directories"
    echo "Look for *.gcov files to see line-by-line coverage"
fi

echo "============================================"
echo "Coverage analysis complete!"
echo "============================================"