#!/bin/bash
#
# IWYU (Include What You Use) analysis and fix script for Milvus C++ code
#
# Usage:
#   ./run_iwyu.sh <file_or_directory> [options]
#
# Options:
#   --fix-headers    Also fix header files (default: only fix .cpp/.cc files)
#   --dry-run        Only analyze, don't apply fixes
#   --skip-tests     Skip test files (*Test.cpp, *_test.cpp)
#   -j, --jobs N     Run N parallel analysis jobs (default: 1)
#   --verbose        Show verbose output
#   --help           Show this help message
#
# Examples:
#   ./run_iwyu.sh src/query/Plan.cpp              # Analyze and fix single file
#   ./run_iwyu.sh src/query/                      # Analyze and fix all C++ files in directory
#   ./run_iwyu.sh src/query/ --dry-run            # Only analyze, show suggestions
#   ./run_iwyu.sh src/query/ --fix-headers        # Also fix header files (risky!)
#

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MILVUS_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
IWYU_DIR="/home/zilliz/iwyu/build/bin"
COMPILE_DB="$MILVUS_ROOT/cmake_build"
MAPPING_FILE="$SCRIPT_DIR/milvus.imp"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default options
FIX_HEADERS=false
DRY_RUN=false
VERBOSE=false
SKIP_TESTS=false
JOBS=1

# Parse arguments
TARGET=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --fix-headers)
            FIX_HEADERS=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --skip-tests)
            SKIP_TESTS=true
            shift
            ;;
        --jobs|-j)
            JOBS="$2"
            shift 2
            ;;
        --help|-h)
            head -25 "$0" | tail -23
            exit 0
            ;;
        -*)
            echo -e "${RED}Error: Unknown option $1${NC}"
            exit 1
            ;;
        *)
            if [[ -z "$TARGET" ]]; then
                TARGET="$1"
            else
                echo -e "${RED}Error: Multiple targets specified${NC}"
                exit 1
            fi
            shift
            ;;
    esac
done

# Validate target
if [[ -z "$TARGET" ]]; then
    echo -e "${RED}Error: No file or directory specified${NC}"
    echo "Usage: $0 <file_or_directory> [options]"
    exit 1
fi

# Convert to absolute path if relative
if [[ ! "$TARGET" = /* ]]; then
    TARGET="$PWD/$TARGET"
fi

if [[ ! -e "$TARGET" ]]; then
    echo -e "${RED}Error: $TARGET does not exist${NC}"
    exit 1
fi

# Check dependencies
if [[ ! -x "$IWYU_DIR/include-what-you-use" ]]; then
    echo -e "${RED}Error: IWYU not found at $IWYU_DIR${NC}"
    echo "Please ensure IWYU is compiled and available."
    exit 1
fi

if [[ ! -f "$COMPILE_DB/compile_commands.json" ]]; then
    echo -e "${RED}Error: compile_commands.json not found at $COMPILE_DB${NC}"
    echo "Please run './compile.sh compile' first to generate it."
    exit 1
fi

if [[ ! -f "$MAPPING_FILE" ]]; then
    echo -e "${YELLOW}Warning: Mapping file not found at $MAPPING_FILE${NC}"
    echo "Continuing without mapping file..."
    MAPPING_ARG=""
else
    MAPPING_ARG="-Xiwyu --mapping_file=$MAPPING_FILE"
fi

# Collect C++ files
collect_files() {
    local target="$1"

    if [[ -f "$target" ]]; then
        # Single file - check if it's a C++ file
        case "$target" in
            *.cpp|*.cc|*.cxx|*.c++|*.h|*.hpp|*.hxx|*.h++)
                echo "$target"
                ;;
            *)
                echo -e "${YELLOW}Warning: $target is not a recognized C++ file${NC}" >&2
                ;;
        esac
    elif [[ -d "$target" ]]; then
        # Directory - find all C++ files
        find "$target" -type f \( \
            -name "*.cpp" -o \
            -name "*.cc" -o \
            -name "*.cxx" -o \
            -name "*.c++" -o \
            -name "*.h" -o \
            -name "*.hpp" -o \
            -name "*.hxx" -o \
            -name "*.h++" \
        \) | sort
    fi
}

# Filter files that exist in compile_commands.json
filter_compiled_files() {
    local file
    while read -r file; do
        # For header files, we need to find a corresponding source file
        # For source files, check if they're in compile_commands.json
        case "$file" in
            *.cpp|*.cc|*.cxx|*.c++)
                if grep -q "\"file\": \"$file\"" "$COMPILE_DB/compile_commands.json" 2>/dev/null; then
                    echo "$file"
                elif $VERBOSE; then
                    echo -e "${YELLOW}Skipping $file (not in compile_commands.json)${NC}" >&2
                fi
                ;;
            *.h|*.hpp|*.hxx|*.h++)
                # Headers are analyzed through their including source files
                # We'll include them but they'll be processed via source files
                echo "$file"
                ;;
        esac
    done
}

# Separate source and header files
separate_files() {
    local src_files=()
    local hdr_files=()

    while read -r file; do
        case "$file" in
            *.cpp|*.cc|*.cxx|*.c++)
                src_files+=("$file")
                ;;
            *.h|*.hpp|*.hxx|*.h++)
                hdr_files+=("$file")
                ;;
        esac
    done

    # Return source files (headers are analyzed through source files)
    printf '%s\n' "${src_files[@]}"
}

# Filter out test files
filter_test_files() {
    local file
    while read -r file; do
        if $SKIP_TESTS; then
            # Skip files ending with Test.cpp, _test.cpp, etc.
            case "$(basename "$file")" in
                *Test.cpp|*_test.cpp|*Test.cc|*_test.cc)
                    if $VERBOSE; then
                        echo -e "${YELLOW}Skipping test file: $file${NC}" >&2
                    fi
                    continue
                    ;;
            esac
        fi
        echo "$file"
    done
}

# Main execution
echo -e "${GREEN}=== IWYU Analysis ===${NC}"
echo "Target: $TARGET"
echo "Fix headers: $FIX_HEADERS"
echo "Dry run: $DRY_RUN"
echo "Skip tests: $SKIP_TESTS"
echo "Parallel jobs: $JOBS"
echo ""

# Collect and filter files
echo -e "${GREEN}Collecting C++ files...${NC}"
ALL_FILES=$(collect_files "$TARGET")
SRC_FILES=$(echo "$ALL_FILES" | separate_files | filter_compiled_files | filter_test_files)

if [[ -z "$SRC_FILES" ]]; then
    echo -e "${YELLOW}No C++ source files found to analyze${NC}"
    exit 0
fi

FILE_COUNT=$(echo "$SRC_FILES" | wc -l)
echo "Found $FILE_COUNT source file(s) to analyze"

# Create temporary file for IWYU output
IWYU_OUTPUT=$(mktemp /tmp/iwyu_output.XXXXXX)
trap "rm -f $IWYU_OUTPUT" EXIT

# Run IWYU analysis
echo -e "${GREEN}Running IWYU analysis...${NC}"
if $VERBOSE; then
    echo "Command: iwyu_tool.py -p $COMPILE_DB <files> -- $MAPPING_ARG"
fi

export PATH="$IWYU_DIR:$PATH"

# Run iwyu_tool.py with collected source files
# shellcheck disable=SC2086
python3 "$IWYU_DIR/iwyu_tool.py" \
    -p "$COMPILE_DB" \
    -j "$JOBS" \
    $SRC_FILES \
    -- $MAPPING_ARG \
    2>&1 | tee "$IWYU_OUTPUT"

# Check if there are any suggestions
if ! grep -q "should add these lines:\|should remove these lines:" "$IWYU_OUTPUT"; then
    echo -e "${GREEN}No changes needed - all includes are correct!${NC}"
    exit 0
fi

# Show summary
echo ""
echo -e "${GREEN}=== Summary ===${NC}"
ADD_COUNT=$(grep -c "should add these lines:" "$IWYU_OUTPUT" 2>/dev/null || echo 0)
REMOVE_COUNT=$(grep -c "should remove these lines:" "$IWYU_OUTPUT" 2>/dev/null || echo 0)
echo "Files with suggested additions: $ADD_COUNT"
echo "Files with suggested removals: $REMOVE_COUNT"

# Apply fixes if not dry run
if $DRY_RUN; then
    echo ""
    echo -e "${YELLOW}Dry run mode - no changes applied${NC}"
    echo "To apply changes, run without --dry-run"
else
    echo ""
    echo -e "${GREEN}Applying fixes...${NC}"

    FIX_ARGS="--nosafe_headers --reorder"

    if ! $FIX_HEADERS; then
        # Only fix source files, not headers (safer)
        FIX_ARGS="$FIX_ARGS --only_re='\.(cpp|cc|cxx|c\+\+)$'"
        echo "(Only fixing source files, use --fix-headers to also fix headers)"
    fi

    # Apply fixes
    if $FIX_HEADERS; then
        cat "$IWYU_OUTPUT" | python3 "$IWYU_DIR/fix_includes.py" --nosafe_headers --reorder
    else
        cat "$IWYU_OUTPUT" | python3 "$IWYU_DIR/fix_includes.py" --nosafe_headers --reorder --only_re='\.(cpp|cc|cxx|c\+\+)$'
    fi

    echo ""
    echo -e "${GREEN}Done!${NC}"
    echo ""
    echo "Next steps:"
    echo "  1. Review changes: git diff"
    echo "  2. Compile to verify: ./compile.sh compile"
    echo "  3. If compilation fails, revert: git checkout -- <files>"
fi
