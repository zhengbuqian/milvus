#!/bin/bash

# Test script to verify Milvus build environment is properly set up
# This script tests that all prerequisites are available and the build can start

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Test function
test_command() {
    local cmd="$1"
    local expected_version="$2"
    local name="$3"
    
    if command -v "$cmd" &> /dev/null; then
        version_output=$($cmd --version 2>/dev/null | head -1 || echo "unknown")
        log_success "$name: Available - $version_output"
        return 0
    else
        log_error "$name: Not found"
        return 1
    fi
}

# Test Go version specifically
test_go() {
    if command -v go &> /dev/null; then
        go_version=$(go version | awk '{print $3}' | sed 's/go//')
        required_version="1.21"
        if [[ "$(printf '%s\n' "$required_version" "$go_version" | sort -V | head -n1)" == "$required_version" ]]; then
            log_success "Go: $go_version (meets requirement >= $required_version)"
            return 0
        else
            log_error "Go: $go_version (does not meet requirement >= $required_version)"
            return 1
        fi
    else
        log_error "Go: Not found"
        return 1
    fi
}

# Test CMake version specifically
test_cmake() {
    if command -v cmake &> /dev/null; then
        cmake_version=$(cmake --version | head -1 | awk '{print $3}')
        required_version="3.26"
        if [[ "$(printf '%s\n' "$required_version" "$cmake_version" | sort -V | head -n1)" == "$required_version" ]]; then
            log_success "CMake: $cmake_version (meets requirement >= $required_version)"
            return 0
        else
            log_error "CMake: $cmake_version (does not meet requirement >= $required_version)"
            return 1
        fi
    else
        log_error "CMake: Not found"
        return 1
    fi
}

# Test Conan profile
test_conan_profile() {
    if command -v conan &> /dev/null; then
        if conan profile show default &> /dev/null; then
            compiler_version=$(conan profile show default | grep "compiler.version=" | cut -d'=' -f2)
            if [[ "$compiler_version" == "12" ]]; then
                log_success "Conan: Profile configured correctly with GCC-12"
                return 0
            else
                log_error "Conan: Profile not configured correctly (compiler.version=$compiler_version, expected 12)"
                return 1
            fi
        else
            log_error "Conan: Default profile not found"
            return 1
        fi
    else
        log_error "Conan: Not found"
        return 1
    fi
}

# Test make build start
test_make_build() {
    log_info "Testing make build process (will timeout after 60 seconds)..."
    
    # Source environment
    if [[ -f ~/.bashrc ]]; then
        source ~/.bashrc 2>/dev/null || true
    fi
    if [[ -f ~/.cargo/env ]]; then
        source ~/.cargo/env 2>/dev/null || true
    elif [[ -f /usr/local/cargo/env ]]; then
        source /usr/local/cargo/env 2>/dev/null || true
    fi
    
    # Test make build with timeout
    if timeout 60 make build-3rdparty &> /dev/null; then
        log_success "Make: Build process started successfully"
        return 0
    else
        exit_code=$?
        if [[ $exit_code -eq 124 ]]; then
            log_success "Make: Build process started successfully (timed out as expected)"
            return 0
        else
            log_error "Make: Build process failed to start"
            return 1
        fi
    fi
}

main() {
    log_info "Testing Milvus build environment..."
    
    ERRORS=0
    
    # Test individual tools
    test_go || ERRORS=$((ERRORS + 1))
    test_cmake || ERRORS=$((ERRORS + 1))
    test_command "gcc" "" "GCC" || ERRORS=$((ERRORS + 1))
    test_command "g++" "" "G++" || ERRORS=$((ERRORS + 1))
    test_command "rustc" "" "Rust" || ERRORS=$((ERRORS + 1))
    test_command "cargo" "" "Cargo" || ERRORS=$((ERRORS + 1))
    test_command "conan" "" "Conan" || ERRORS=$((ERRORS + 1))
    test_command "make" "" "Make" || ERRORS=$((ERRORS + 1))
    test_command "git" "" "Git" || ERRORS=$((ERRORS + 1))
    test_command "python3" "" "Python3" || ERRORS=$((ERRORS + 1))
    
    # Test Conan profile
    test_conan_profile || ERRORS=$((ERRORS + 1))
    
    # Test make build
    test_make_build || ERRORS=$((ERRORS + 1))
    
    echo ""
    if [[ $ERRORS -eq 0 ]]; then
        log_success "All tests passed! Milvus build environment is ready."
        log_info "You can now run 'make' or 'make build-cpp-with-unittest' to build Milvus."
        exit 0
    else
        log_error "$ERRORS test(s) failed. Please run the setup script first."
        exit 1
    fi
}

# Run main function
main "$@"