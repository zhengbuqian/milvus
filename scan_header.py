#!/usr/bin/env python3
"""
Scan header files for IWYU processing.
This script reads iwyu_progress.md and finds the next batch of header files to process.
"""

import os
import re
import sys
from pathlib import Path
from typing import Set, List, Dict, Tuple

PROGRESS_FILE = "iwyu_progress.md"
INTERNAL_CORE = "internal/core"
PROJECT_ROOT = Path(__file__).parent


def parse_progress_file() -> Tuple[Set[str], Set[str], Set[str]]:
    """
    Parse iwyu_progress.md and return:
    - unprocessed_headers: headers marked [ ]
    - processed_headers: headers marked [x]
    - problem_files: files marked [!] (both cpp and headers)
    """
    unprocessed_headers = set()
    processed_headers = set()
    problem_files = set()

    progress_path = PROJECT_ROOT / PROGRESS_FILE
    if not progress_path.exists():
        print(f"Error: {PROGRESS_FILE} not found")
        sys.exit(1)

    with open(progress_path, "r") as f:
        content = f.read()

    # Match lines like "- [ ] /path/to/file.h" or "- [x] /path/to/file.h" or "- [!] /path/to/file.cpp"
    pattern = r"- \[([x! ])\] (/[^\s]+)"
    for match in re.finditer(pattern, content):
        status = match.group(1)
        filepath = match.group(2)

        if status == "!":
            problem_files.add(filepath)
        elif status == "x":
            if filepath.endswith((".h", ".hpp")):
                processed_headers.add(filepath)
        elif status == " ":
            if filepath.endswith((".h", ".hpp")):
                unprocessed_headers.add(filepath)

    return unprocessed_headers, processed_headers, problem_files


def scan_cpp_includes(cpp_file: str, internal_core_path: str) -> Set[str]:
    """
    Scan a cpp file and return all internal/core header includes.
    """
    headers = set()
    include_pattern = re.compile(r'#include\s+[<"]([^>"]+)[>"]')

    try:
        with open(cpp_file, "r", errors="ignore") as f:
            content = f.read()
    except Exception as e:
        return headers

    for match in include_pattern.finditer(content):
        include_path = match.group(1)

        # Skip system headers and third-party headers
        if include_path.startswith(("boost/", "arrow/", "google/", "fmt/", "folly/",
                                    "nlohmann/", "simdjson/", "tbb/", "aws/", "azure/",
                                    "opentelemetry/", "prometheus/", "re2/", "antlr4",
                                    "tantivy", "knowhere/", "faiss/", "minio/", "opendal/")):
            continue

        # Skip generated protobuf files
        if include_path.endswith(".pb.h"):
            continue

        # Try to resolve the header path
        # Headers may be relative or in standard include paths
        possible_paths = [
            os.path.join(internal_core_path, "src", include_path),
            os.path.join(internal_core_path, "unittest", include_path),
            os.path.join(os.path.dirname(cpp_file), include_path),
        ]

        for p in possible_paths:
            if os.path.exists(p):
                headers.add(os.path.abspath(p))
                break

    return headers


def find_all_cpp_files(internal_core_path: str, problem_files: Set[str]) -> List[str]:
    """
    Find all cpp files in internal/core, excluding problem files.
    """
    cpp_files = []

    for root, dirs, files in os.walk(internal_core_path):
        # Skip build directories
        dirs[:] = [d for d in dirs if d not in ("build", "cmake_build", "output", "thirdparty")]

        for f in files:
            if f.endswith(".cpp"):
                filepath = os.path.abspath(os.path.join(root, f))
                if filepath not in problem_files:
                    cpp_files.append(filepath)

    return cpp_files


def find_next_batch(target_headers: int = 12) -> Tuple[List[str], List[str]]:
    """
    Find the next batch of cpp files and their unprocessed headers.
    Returns (cpp_files, header_files) where header_files is the batch to process.
    """
    unprocessed_headers, processed_headers, problem_files = parse_progress_file()

    internal_core_path = str(PROJECT_ROOT / INTERNAL_CORE)
    cpp_files = find_all_cpp_files(internal_core_path, problem_files)

    selected_cpps = []
    collected_headers = set()

    for cpp in cpp_files:
        cpp_headers = scan_cpp_includes(cpp, internal_core_path)
        # Filter to only unprocessed headers in internal/core
        new_headers = {h for h in cpp_headers
                       if h in unprocessed_headers and h not in collected_headers}

        if new_headers:
            selected_cpps.append(cpp)
            collected_headers.update(new_headers)

            if len(collected_headers) >= target_headers:
                break

    return selected_cpps, sorted(collected_headers)


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--stats":
        unprocessed, processed, problems = parse_progress_file()
        print(f"Unprocessed headers: {len(unprocessed)}")
        print(f"Processed headers: {len(processed)}")
        print(f"Problem files: {len(problems)}")
        return

    target = int(sys.argv[1]) if len(sys.argv) > 1 else 12

    cpp_files, headers = find_next_batch(target)

    print("=== Next batch of headers to process ===")
    print(f"Total headers: {len(headers)}")
    for h in headers:
        print(f"  {h}")

    print(f"\n=== CPP files to use as entry points ({len(cpp_files)}) ===")
    for c in cpp_files:
        print(f"  {c}")


if __name__ == "__main__":
    main()
