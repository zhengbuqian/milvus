#!/usr/bin/env python3
"""E2E test for regex filter (=~ and !~) on Milvus standalone."""

import time
import sys
import numpy as np
from pymilvus import (
    connections, Collection, CollectionSchema, FieldSchema, DataType,
    utility, MilvusException
)

MILVUS_HOST = "127.0.0.1"
MILVUS_PORT = "19530"
DIM = 8

# Test data: carefully chosen strings to test various regex patterns
TEST_STRINGS = [
    "hello world",           # 0
    "Hello World",           # 1  (uppercase H/W)
    "say hello",             # 2
    "HELLO",                 # 3
    "error: timeout",        # 4
    "connection error",      # 5
    "no errors here",        # 6
    "color red",             # 7
    "colour blue",           # 8
    "user_123@gmail.com",    # 9
    "user_abc@gmail.com",    # 10
    "abc",                   # 11
    "xyzabcdef",             # 12
    "123-456-7890",          # 13 (phone number)
    "192.168.1.1",           # 14 (IP address)
    "",                      # 15 (empty string)
    "line1\nline2",          # 16 (contains newline)
    "a\tb",                  # 17 (contains tab)
    "\xe4\xb8\xad\xe6\x96\x87test",  # 18 (中文test)
    "cat food",              # 19
    "dog food",              # 20
    "hamster food",          # 21
    "start_here",            # 22
    "not_start",             # 23
    "end_here_end",          # 24
    "price$10",              # 25
    "file.txt",              # 26
    "filetxt",               # 27
    "aaabbb",                # 28
    "aabbb",                 # 29
]

def connect():
    print("Connecting to Milvus...")
    connections.connect(host=MILVUS_HOST, port=MILVUS_PORT)
    print("Connected.")

def create_collection(name, with_json=False):
    if utility.has_collection(name):
        utility.drop_collection(name)

    fields = [
        FieldSchema("id", DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema("text", DataType.VARCHAR, max_length=256),
        FieldSchema("vec", DataType.FLOAT_VECTOR, dim=DIM),
    ]
    if with_json:
        fields.append(FieldSchema("meta", DataType.JSON))

    schema = CollectionSchema(fields)
    col = Collection(name, schema)
    return col

def insert_data(col, with_json=False):
    n = len(TEST_STRINGS)
    ids = list(range(n))
    vecs = np.random.random((n, DIM)).astype(np.float32).tolist()

    data = [ids, TEST_STRINGS, vecs]
    if with_json:
        json_data = [{"tag": s, "num": i} for i, s in enumerate(TEST_STRINGS)]
        data.append(json_data)

    col.insert(data)
    col.flush()
    print(f"Inserted {n} rows.")

def query_regex(col, expr, expected_ids, test_name):
    """Query with regex expr and verify results match expected IDs."""
    try:
        results = col.query(expr=expr, output_fields=["id", "text"])
        got_ids = sorted([r["id"] for r in results])
        expected = sorted(expected_ids)

        if got_ids == expected:
            print(f"  PASS: {test_name}")
            return True
        else:
            print(f"  FAIL: {test_name}")
            print(f"    expr: {expr}")
            print(f"    expected: {expected}")
            print(f"    got:      {got_ids}")
            # Show what strings were returned vs expected
            got_strs = [TEST_STRINGS[i] for i in got_ids if i < len(TEST_STRINGS)]
            exp_strs = [TEST_STRINGS[i] for i in expected if i < len(TEST_STRINGS)]
            print(f"    expected_strs: {exp_strs}")
            print(f"    got_strs:      {got_strs}")
            return False
    except MilvusException as e:
        print(f"  FAIL: {test_name} — exception: {e}")
        return False

def query_regex_error(col, expr, test_name):
    """Verify that the expression raises an error."""
    try:
        col.query(expr=expr, output_fields=["id"])
        print(f"  FAIL: {test_name} — expected error but succeeded")
        return False
    except Exception:
        print(f"  PASS: {test_name} (expected error)")
        return True

def run_tests(col, label):
    print(f"\n{'='*60}")
    print(f"Running tests: {label}")
    print(f"{'='*60}")

    passed = 0
    failed = 0

    def check(expr, expected_ids, name):
        nonlocal passed, failed
        if query_regex(col, expr, expected_ids, name):
            passed += 1
        else:
            failed += 1

    def check_error(expr, name):
        nonlocal passed, failed
        if query_regex_error(col, expr, name):
            passed += 1
        else:
            failed += 1

    # --- Basic substring match ---
    check('text =~ "hello"', [0, 2], "basic substring 'hello'")
    check('text =~ "abc"', [11, 12], "basic substring 'abc'")
    check('text =~ "error"', [4, 5, 6], "basic substring 'error'")

    # --- Anchors ---
    check('text =~ "^hello"', [0], "anchor ^hello")
    check('text =~ "^Hello"', [1], "anchor ^Hello (case sensitive)")
    check('text =~ "world$"', [0], "anchor world$")  # "hello world" ends with "world"
    check('text =~ "^abc$"', [11], "anchor ^abc$ (exact match)")

    # --- Empty pattern matches everything ---
    all_ids = list(range(len(TEST_STRINGS)))
    check('text =~ ""', all_ids, "empty pattern matches all")

    # --- Case insensitive ---
    check('text =~ "(?i)hello"', [0, 1, 2, 3], "(?i) case insensitive")

    # --- Alternation ---
    check('text =~ "cat|dog"', [19, 20], "alternation cat|dog")

    # --- Optional char ---
    check('text =~ "colou?r"', [7, 8], "optional u: colou?r")

    # --- Character class ---
    check('text =~ "user_[0-9]+@gmail\\.com"', [9], "char class user_[0-9]+")

    # --- Digit shorthand ---
    check('text =~ "\\d{3}-\\d{3}-\\d{4}"', [13], "\\d phone number")

    # --- Escaped dot ---
    check('text =~ "file\\.txt"', [26], "escaped dot file\\.txt")

    # --- Negation !~ ---
    # !~ "error" should return everything except indices 4,5,6
    no_error_ids = [i for i in all_ids if i not in [4, 5, 6]]
    check('text !~ "error"', no_error_ids, "negation !~ error")

    # --- Regex that optimizes to LIKE ---
    # Pure literal "abc" → InnerMatch
    check('text =~ "abc"', [11, 12], "regex→LIKE: pure literal")
    # ^start → PrefixMatch
    check('text =~ "^start"', [22], "regex→LIKE: ^prefix")
    # end$ → PostfixMatch
    check('text =~ "end$"', [24], "regex→LIKE: suffix$")

    # --- dot_nl: . matches \n ---
    check('text =~ "line1.line2"', [16], "dot_nl: . matches \\n")

    # --- (?-s) disables dot_nl ---
    check('text =~ "(?-s)line1.line2"', [], "(?-s) disables dot_nl")

    # --- Unicode ---
    check('text =~ "\\p{Han}+"', [18], "Unicode \\p{Han}")

    # --- Quantifiers ---
    check('text =~ "a{3}"', [28], "quantifier a{3}")
    check('text =~ "a{2,3}"', [28, 29], "quantifier a{2,3}")

    # --- Escaped $ ---
    check('text =~ "price\\$"', [25], "escaped dollar")

    # --- Error cases ---
    check_error('text =~ "[unclosed"', "invalid regex rejected")

    print(f"\nResults: {passed} passed, {failed} failed")
    return failed == 0

def create_index(col, index_type, index_params=None):
    """Create scalar index on 'text' field."""
    col.release()

    # Drop existing index on text field if any
    try:
        col.drop_index(field_name="text")
    except Exception:
        pass

    params = {"index_type": index_type}
    if index_params:
        params.update(index_params)

    print(f"\nCreating {index_type} index on 'text'...")
    col.create_index("text", params)
    col.load()
    time.sleep(2)
    print(f"{index_type} index created and loaded.")

def main():
    connect()

    all_passed = True

    # ============================================================
    # Test 1: No index (brute force)
    # ============================================================
    col = create_collection("regex_test_noindex")
    insert_data(col)
    col.load()
    time.sleep(2)

    if not run_tests(col, "NO INDEX (brute force)"):
        all_passed = False

    # ============================================================
    # Test 2: Inverted index
    # ============================================================
    col2 = create_collection("regex_test_inverted")
    insert_data(col2)
    create_index(col2, "INVERTED")

    if not run_tests(col2, "INVERTED INDEX"):
        all_passed = False

    # ============================================================
    # Test 3: Bitmap index
    # ============================================================
    col3 = create_collection("regex_test_bitmap")
    insert_data(col3)
    create_index(col3, "BITMAP")

    if not run_tests(col3, "BITMAP INDEX"):
        all_passed = False

    # ============================================================
    # Test 4: STL_SORT index
    # ============================================================
    col4 = create_collection("regex_test_sort")
    insert_data(col4)
    create_index(col4, "STL_SORT")

    if not run_tests(col4, "STL_SORT INDEX"):
        all_passed = False

    # ============================================================
    # Test 5: Trie (MARISA) index
    # ============================================================
    col5 = create_collection("regex_test_trie")
    insert_data(col5)
    create_index(col5, "Trie")

    if not run_tests(col5, "TRIE (MARISA) INDEX"):
        all_passed = False

    # ============================================================
    # Test 6: JSON field
    # ============================================================
    col6 = create_collection("regex_test_json", with_json=True)
    insert_data(col6, with_json=True)
    col6.load()
    time.sleep(2)

    print(f"\n{'='*60}")
    print("Running tests: JSON FIELD")
    print(f"{'='*60}")

    json_passed = 0
    json_failed = 0

    def jcheck(expr, expected_ids, name):
        nonlocal json_passed, json_failed
        if query_regex(col6, expr, expected_ids, name):
            json_passed += 1
        else:
            json_failed += 1

    jcheck('meta["tag"] =~ "hello"', [0, 2], "JSON: meta[tag] =~ hello")
    jcheck('meta["tag"] =~ "^abc$"', [11], "JSON: meta[tag] =~ ^abc$")
    jcheck('meta["tag"] =~ "(?i)hello"', [0, 1, 2, 3], "JSON: (?i) case insensitive")
    jcheck('meta["tag"] =~ "error"', [4, 5, 6], "JSON: error substring")
    jcheck('meta["tag"] !~ "error"',
           [i for i in range(len(TEST_STRINGS)) if i not in [4, 5, 6]],
           "JSON: !~ negation")

    print(f"\nJSON Results: {json_passed} passed, {json_failed} failed")
    if json_failed > 0:
        all_passed = False

    # ============================================================
    # Summary
    # ============================================================
    print(f"\n{'='*60}")
    if all_passed:
        print("ALL E2E TESTS PASSED")
    else:
        print("SOME TESTS FAILED")
    print(f"{'='*60}")

    # Cleanup
    for name in ["regex_test_noindex", "regex_test_inverted", "regex_test_bitmap",
                  "regex_test_sort", "regex_test_trie", "regex_test_json"]:
        try:
            utility.drop_collection(name)
        except Exception:
            pass

    connections.disconnect("default")
    sys.exit(0 if all_passed else 1)

if __name__ == "__main__":
    main()
