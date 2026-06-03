#!/usr/bin/env python3
"""
Simulate row-level security with Milvus scalar filters and measure the cost.

This benchmark creates a collection with ACL-like fields:
  owner, shared_users, shared_departments, and can_access_uXX bool fields.

For each test user, can_access_uXX is generated from the same predicate used by
the simulated RLS expression:
  owner == user OR array_contains(shared_users, user)
  OR array_contains(shared_departments, user_department)

The bool field benchmark is a lower-bound comparison for filtering overhead.
It is not a claim that bool filtering is free.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import re
import socket
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from pymilvus import MilvusClient

DataType = None
MilvusClient = None


ID_FIELD = "id"
VECTOR_FIELD = "vector"
OWNER_FIELD = "owner"
SHARED_USERS_FIELD = "shared_users"
SHARED_DEPARTMENTS_FIELD = "shared_departments"
COUNT_FIELD = "count(*)"
EXTERNAL_OWNER = "external_owner"

BASE_ROWS = 1_010_000
USER_COUNT = 26
USERS = [f"u{i:02d}" for i in range(USER_COUNT)]
DEPARTMENTS = {user: f"d{i:02d}" for i, user in enumerate(USERS)}
DEFAULT_HEAVY_CAPACITIES = [100, 1000, 5000, 10000, 50000, 100000]
DEFAULT_SHARED_USER_DOC_COUNTS = [100, 1000, 5000, 10000, 50000, 100000]
SCALAR_INDEX_TYPES = ("BITMAP", "INVERTED", "AUTOINDEX", "HYBRID", "STL_SORT")
PROJECTION_POC_HEADER_RE = re.compile(r"RLS projection POC rows=(?P<rows>\d+)\s+")
PROJECTION_POC_RESULT_RE = re.compile(r"RLS_PROJECTION_POC_RESULT\s+(?P<fields>.+)")

STYLE_NONE = 0
STYLE_OWNER = 1
STYLE_SHARED_USER = 2
STYLE_DEPARTMENT = 3
STYLE_OVERLAP = 4


@dataclass(frozen=True)
class UserPlan:
    user: str
    category: str
    base_rows: int
    style: int | None


@dataclass(frozen=True)
class BenchCase:
    phase: str
    user: str
    category: str
    expr_type: str
    expression: str
    expected_count: int | None


@dataclass(frozen=True)
class HeavyArrayPlan:
    capacity: int
    rows: int
    user: str
    category: str


USER_PLANS = [
    UserPlan("u00", "owner_only", 50_000, STYLE_OWNER),
    UserPlan("u01", "shared_user_only", 50_000, STYLE_SHARED_USER),
    UserPlan("u02", "department_only", 50_000, STYLE_DEPARTMENT),
    UserPlan("u03", "overlap_owner_user_department", 50_000, STYLE_OVERLAP),
    UserPlan("u04", "broad_250k", 250_000, STYLE_DEPARTMENT),
    UserPlan("u05", "medium_100k", 100_000, STYLE_SHARED_USER),
    UserPlan("u06", "sparse_10k", 10_000, STYLE_OWNER),
    UserPlan("u07", "ultra_sparse_1k", 1_000, STYLE_DEPARTMENT),
    *[UserPlan(f"u{i:02d}", "mixed_25k", 25_000, None) for i in range(8, 19)],
    UserPlan("u19", "no_access", 0, STYLE_NONE),
    *[
        UserPlan(f"u{i + 20:02d}", f"shared_user_docs_{count}", count, STYLE_SHARED_USER)
        for i, count in enumerate(DEFAULT_SHARED_USER_DOC_COUNTS)
    ],
]


def require_pymilvus() -> None:
    global DataType, MilvusClient
    if DataType is not None and MilvusClient is not None:
        return
    try:
        from pymilvus import DataType as _DataType
        from pymilvus import MilvusClient as _MilvusClient
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "pymilvus is required to run this benchmark. Install the 2.6 client, "
            "for example: python3 -m pip install -r tests/python_client/requirements.txt"
        ) from exc
    DataType = _DataType
    MilvusClient = _MilvusClient


def log(message: str) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{now}] {message}", flush=True)


def bool_field(user: str) -> str:
    return f"can_access_{user}"


def quote(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def rls_expression(user: str) -> str:
    dept = DEPARTMENTS[user]
    return (
        f"{OWNER_FIELD} == {quote(user)} or "
        f"array_contains({SHARED_USERS_FIELD}, {quote(user)}) or "
        f"array_contains({SHARED_DEPARTMENTS_FIELD}, {quote(dept)})"
    )


def bool_expression(user: str) -> str:
    return f"{bool_field(user)} == true"


def parse_csv_ints(raw: str) -> list[int]:
    values = [int(item.strip()) for item in raw.split(",") if item.strip()]
    if not values:
        raise ValueError("at least one capacity is required")
    if any(value <= 0 for value in values):
        raise ValueError("capacities must be positive")
    return values


def parse_users(raw: str) -> list[str]:
    if raw.strip().lower() == "all":
        return list(USERS)
    users = [item.strip() for item in raw.split(",") if item.strip()]
    unknown = sorted(set(users) - set(USERS))
    if unknown:
        raise ValueError(f"unknown users: {', '.join(unknown)}")
    return users


def git_value(args: list[str]) -> str:
    try:
        return subprocess.check_output(
            ["git", *args],
            cwd=Path(__file__).resolve().parents[3],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return ""


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def scale_user_counts(total_rows: int) -> dict[str, int]:
    if total_rows <= 0:
        raise ValueError("--rows must be positive")

    raw_counts: list[tuple[str, int, float]] = []
    floor_total = 0
    for plan in USER_PLANS:
        raw = plan.base_rows * total_rows / BASE_ROWS
        floor_value = int(raw)
        raw_counts.append((plan.user, floor_value, raw - floor_value))
        floor_total += floor_value

    base_access_rows = sum(plan.base_rows for plan in USER_PLANS)
    target_access_rows = min(total_rows, int(round(base_access_rows * total_rows / BASE_ROWS)))
    remaining = max(0, target_access_rows - floor_total)
    counts = {user: floor_value for user, floor_value, _ in raw_counts}

    for user, _, _ in sorted(raw_counts, key=lambda item: item[2], reverse=True):
        if remaining == 0:
            break
        if next(plan for plan in USER_PLANS if plan.user == user).base_rows == 0:
            continue
        counts[user] += 1
        remaining -= 1

    return counts


def plan_by_user() -> dict[str, UserPlan]:
    return {plan.user: plan for plan in USER_PLANS}


def mixed_style(position: int) -> int:
    return [STYLE_OWNER, STYLE_SHARED_USER, STYLE_DEPARTMENT, STYLE_OVERLAP][position % 4]


def encode_acl(user: str, style: int) -> int:
    return (USERS.index(user) + 1) * 10 + style


def decode_acl(code: int) -> tuple[str, str, list[str], list[str]]:
    if code == STYLE_NONE:
        return EXTERNAL_OWNER, "", [], []
    user_index = code // 10 - 1
    style = code % 10
    user = USERS[user_index]
    dept = DEPARTMENTS[user]

    if style == STYLE_OWNER:
        return user, user, [], []
    if style == STYLE_SHARED_USER:
        return EXTERNAL_OWNER, user, [user], []
    if style == STYLE_DEPARTMENT:
        return EXTERNAL_OWNER, user, [], [dept]
    if style == STYLE_OVERLAP:
        return user, user, [user], [dept]
    raise ValueError(f"invalid ACL style code: {code}")


def build_acl_schedule(total_rows: int, seed: int) -> tuple[list[int], dict[str, int]]:
    counts = scale_user_counts(total_rows)
    plans = plan_by_user()
    schedule: list[int] = []

    for user in USERS:
        count = counts[user]
        if count == 0:
            continue
        plan = plans[user]
        if plan.style is None:
            for i in range(count):
                schedule.append(encode_acl(user, mixed_style(i)))
        else:
            schedule.extend([encode_acl(user, plan.style)] * count)

    no_access_rows = total_rows - len(schedule)
    if no_access_rows < 0:
        raise ValueError("scaled access rows exceed total rows")
    schedule.extend([STYLE_NONE] * no_access_rows)

    random.Random(seed).shuffle(schedule)
    return schedule, counts


def normalize_vectors(vectors: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return vectors / norms


def make_vectors(rng: np.random.Generator, rows: int, dim: int) -> list[list[float]]:
    vectors = rng.random((rows, dim), dtype=np.float32)
    return normalize_vectors(vectors).tolist()


def bool_values(owner: str, shared_users: Iterable[str], shared_departments: Iterable[str]) -> dict[str, bool]:
    shared_user_set = set(shared_users)
    department_set = set(shared_departments)
    return {
        bool_field(user): owner == user or user in shared_user_set or DEPARTMENTS[user] in department_set
        for user in USERS
    }


def build_row(row_id: int, vector: list[float], acl_code: int) -> dict[str, Any]:
    owner, _, shared_users, shared_departments = decode_acl(acl_code)
    row = {
        ID_FIELD: row_id,
        VECTOR_FIELD: vector,
        OWNER_FIELD: owner,
        SHARED_USERS_FIELD: shared_users,
        SHARED_DEPARTMENTS_FIELD: shared_departments,
    }
    row.update(bool_values(owner, shared_users, shared_departments))
    return row


def heavy_array_plans(args: argparse.Namespace) -> list[HeavyArrayPlan]:
    if args.heavy_rows_per_capacity == 0:
        return []
    plans: list[HeavyArrayPlan] = []
    for i, capacity in enumerate(args.heavy_capacities):
        user = USERS[(args.heavy_start_user_index + i) % USER_COUNT]
        plans.append(
            HeavyArrayPlan(
                capacity=capacity,
                rows=args.heavy_rows_per_capacity,
                user=user,
                category=f"shared_users_len_{capacity}",
            )
        )
    return plans


def heavy_row_count(args: argparse.Namespace) -> int:
    return sum(plan.rows for plan in heavy_array_plans(args))


def total_rows(args: argparse.Namespace) -> int:
    return args.rows + heavy_row_count(args)


def build_expected_counts(args: argparse.Namespace) -> dict[str, int]:
    counts = scale_user_counts(args.rows)
    for plan in heavy_array_plans(args):
        counts[plan.user] += plan.rows
    return counts


def heavy_shared_users(capacity: int, row_id: int, user: str) -> list[str]:
    values = [f"hu{capacity}_{row_id}_{i}" for i in range(capacity - 1)]
    values.append(user)
    return values


def build_heavy_row(row_id: int, vector: list[float], plan: HeavyArrayPlan) -> dict[str, Any]:
    shared_users = heavy_shared_users(plan.capacity, row_id, plan.user)
    row = {
        ID_FIELD: row_id,
        VECTOR_FIELD: vector,
        OWNER_FIELD: EXTERNAL_OWNER,
        SHARED_USERS_FIELD: shared_users,
        SHARED_DEPARTMENTS_FIELD: [],
    }
    row.update(bool_values(EXTERNAL_OWNER, shared_users, []))
    return row


def create_schema(client: MilvusClient, args: argparse.Namespace) -> Any:
    schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
    schema.add_field(ID_FIELD, DataType.INT64, is_primary=True, auto_id=False)
    schema.add_field(VECTOR_FIELD, DataType.FLOAT_VECTOR, dim=args.dim)
    schema.add_field(OWNER_FIELD, DataType.VARCHAR, max_length=64)
    schema.add_field(
        SHARED_USERS_FIELD,
        DataType.ARRAY,
        element_type=DataType.VARCHAR,
        max_capacity=args.max_array_capacity,
        max_length=args.array_varchar_max_length,
    )
    schema.add_field(
        SHARED_DEPARTMENTS_FIELD,
        DataType.ARRAY,
        element_type=DataType.VARCHAR,
        max_capacity=8,
        max_length=64,
    )
    for user in USERS:
        schema.add_field(bool_field(user), DataType.BOOL)
    return schema


def create_index_params(client: MilvusClient, args: argparse.Namespace) -> Any:
    index_params = client.prepare_index_params()
    if not args.skip_vector_index:
        vector_params = {}
        if args.vector_index_type == "HNSW":
            vector_params = {"M": args.hnsw_m, "efConstruction": args.hnsw_ef_construction}
        index_params.add_index(
            field_name=VECTOR_FIELD,
            index_type=args.vector_index_type,
            metric_type="COSINE",
            params=vector_params,
        )
    scalar_params = {}
    if args.bitmap_cardinality_limit is not None:
        scalar_params["bitmap_cardinality_limit"] = args.bitmap_cardinality_limit
    for field_name in [OWNER_FIELD, SHARED_USERS_FIELD, SHARED_DEPARTMENTS_FIELD]:
        index_params.add_index(
            field_name=field_name,
            index_type=args.rls_index_type,
            params=scalar_params,
        )
    for user in USERS:
        index_params.add_index(
            field_name=bool_field(user),
            index_type=args.bool_index_type,
            params=scalar_params,
        )
    return index_params


def collection_exists(client: MilvusClient, collection_name: str) -> bool:
    return bool(client.has_collection(collection_name))


def insert_rows(client: MilvusClient, args: argparse.Namespace, rows: list[dict[str, Any]]) -> None:
    client.insert(
        collection_name=args.collection,
        data=rows,
        timeout=args.insert_timeout,
    )


def prepare_collection(client: MilvusClient, args: argparse.Namespace) -> dict[str, int]:
    if collection_exists(client, args.collection):
        if not args.recreate:
            raise RuntimeError(
                f"collection {args.collection!r} already exists; use --run-only or --recreate"
            )
        log(f"dropping existing collection {args.collection}")
        client.drop_collection(args.collection)

    log(f"creating collection {args.collection}")
    schema = create_schema(client, args)
    client.create_collection(
        collection_name=args.collection,
        schema=schema,
        consistency_level="Strong",
        timeout=args.query_timeout,
    )

    schedule, _ = build_acl_schedule(args.rows, args.seed)
    expected_counts = build_expected_counts(args)
    rng = np.random.default_rng(args.seed)
    inserted = 0
    start = time.perf_counter()

    for offset in range(0, args.rows, args.batch_size):
        batch_codes = schedule[offset : offset + args.batch_size]
        vectors = make_vectors(rng, len(batch_codes), args.dim)
        rows = [
            build_row(offset + i, vectors[i], acl_code)
            for i, acl_code in enumerate(batch_codes)
        ]
        insert_rows(client, args, rows)
        inserted += len(rows)
        if inserted == args.rows or inserted % args.progress_interval == 0:
            elapsed = time.perf_counter() - start
            rate = inserted / elapsed if elapsed > 0 else 0
            log(f"inserted base rows {inserted}/{args.rows} ({rate:.0f} rows/s)")

    heavy_plans = heavy_array_plans(args)
    if heavy_plans and args.flush_before_heavy:
        log("flushing base rows before inserting heavy shared_users rows")
        client.flush(args.collection, timeout=args.flush_timeout)

    if heavy_plans:
        log(
            "inserting heavy shared_users rows: "
            + ", ".join(f"{plan.rows}x{plan.capacity}" for plan in heavy_plans)
        )
    next_id = args.rows
    heavy_batch: list[dict[str, Any]] = []
    for plan in heavy_plans:
        for _ in range(plan.rows):
            vector = make_vectors(rng, 1, args.dim)[0]
            heavy_batch.append(build_heavy_row(next_id, vector, plan))
            next_id += 1
            if len(heavy_batch) >= args.heavy_insert_batch_size:
                insert_rows(client, args, heavy_batch)
                inserted += len(heavy_batch)
                log(f"inserted total rows {inserted}/{total_rows(args)}")
                heavy_batch = []
    if heavy_batch:
        insert_rows(client, args, heavy_batch)
        inserted += len(heavy_batch)
        log(f"inserted total rows {inserted}/{total_rows(args)}")

    log("flushing collection")
    client.flush(args.collection, timeout=args.flush_timeout)

    log(
        "building ACL and bool indexes "
        + ("with vector index " if not args.skip_vector_index else "without vector index ")
        + f"(rls={args.rls_index_type}, bool={args.bool_index_type}, "
        f"bitmap_cardinality_limit={args.bitmap_cardinality_limit})"
    )
    client.create_index(
        collection_name=args.collection,
        index_params=create_index_params(client, args),
        timeout=args.index_timeout,
    )

    log("loading collection")
    client.load_collection(args.collection, timeout=args.load_timeout)
    return expected_counts


def load_collection(client: MilvusClient, args: argparse.Namespace) -> None:
    log(f"loading collection {args.collection}")
    client.load_collection(args.collection, timeout=args.load_timeout)


def extract_count(result: Any) -> int:
    if isinstance(result, tuple):
        result = result[0]
    if not result:
        raise RuntimeError(f"empty count result: {result!r}")
    first = result[0]
    if COUNT_FIELD not in first:
        raise RuntimeError(f"count field missing from result: {result!r}")
    return int(first[COUNT_FIELD])


def query_count(client: MilvusClient, args: argparse.Namespace, expression: str) -> int:
    result = client.query(
        collection_name=args.collection,
        filter=expression,
        output_fields=[COUNT_FIELD],
        timeout=args.query_timeout,
    )
    return extract_count(result)


def make_query_vectors(args: argparse.Namespace) -> list[list[float]]:
    rng = np.random.default_rng(args.query_seed)
    return make_vectors(rng, args.nq, args.dim)


def search_once(
    client: MilvusClient,
    args: argparse.Namespace,
    query_vectors: list[list[float]],
    expression: str,
) -> Any:
    return client.search(
        collection_name=args.collection,
        data=query_vectors,
        anns_field=VECTOR_FIELD,
        filter=expression,
        limit=args.topk,
        output_fields=[],
        search_params={"metric_type": "COSINE", "params": {"ef": args.hnsw_ef}},
        timeout=args.query_timeout,
    )


def percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = (len(sorted_values) - 1) * pct
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    weight = position - lower
    return sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight


def summarize_latencies(latencies_ms: list[float]) -> dict[str, float]:
    ordered = sorted(latencies_ms)
    return {
        "latency_min_ms": min(ordered),
        "latency_mean_ms": statistics.fmean(ordered),
        "latency_p50_ms": percentile(ordered, 0.50),
        "latency_p95_ms": percentile(ordered, 0.95),
        "latency_p99_ms": percentile(ordered, 0.99),
        "latency_max_ms": max(ordered),
        "latency_stddev_ms": statistics.pstdev(ordered) if len(ordered) > 1 else 0.0,
    }


def measure(call: Callable[[], Any], warmups: int, runs: int) -> dict[str, float]:
    for _ in range(warmups):
        call()

    latencies_ms: list[float] = []
    for _ in range(runs):
        start = time.perf_counter()
        call()
        latencies_ms.append((time.perf_counter() - start) * 1000.0)
    return summarize_latencies(latencies_ms)


def scalar_stats(latency_ms: float) -> dict[str, float]:
    return summarize_latencies([latency_ms])


def category_for_user(user: str, args: argparse.Namespace) -> str:
    heavy_categories = [plan.category for plan in heavy_array_plans(args) if plan.user == user]
    base_category = plan_by_user()[user].category
    if not heavy_categories:
        return base_category
    return base_category + "+" + "+".join(heavy_categories)


def build_count_cases(
    selected_users: list[str],
    expected_counts: dict[str, int],
    args: argparse.Namespace,
) -> list[BenchCase]:
    cases: list[BenchCase] = []
    for user in selected_users:
        cases.append(
            BenchCase(
                phase="count_query",
                user=user,
                category=category_for_user(user, args),
                expr_type="current",
                expression=rls_expression(user),
                expected_count=expected_counts.get(user),
            )
        )
        cases.append(
            BenchCase(
                phase="count_query",
                user=user,
                category=category_for_user(user, args),
                expr_type="bool",
                expression=bool_expression(user),
                expected_count=expected_counts.get(user),
            )
        )
    return cases


def build_search_cases(
    selected_users: list[str],
    expected_counts: dict[str, int],
    args: argparse.Namespace,
) -> list[BenchCase]:
    cases = [
        BenchCase(
            phase="search",
            user="all",
            category="no_filter",
            expr_type="none",
            expression="",
            expected_count=None,
        )
    ]
    for user in selected_users:
        cases.append(
            BenchCase(
                phase="search",
                user=user,
                category=category_for_user(user, args),
                expr_type="current",
                expression=rls_expression(user),
                expected_count=expected_counts.get(user),
            )
        )
        cases.append(
            BenchCase(
                phase="search",
                user=user,
                category=category_for_user(user, args),
                expr_type="bool",
                expression=bool_expression(user),
                expected_count=expected_counts.get(user),
            )
        )
    return cases


def validate_counts(
    client: MilvusClient,
    args: argparse.Namespace,
    selected_users: list[str],
    expected_counts: dict[str, int],
) -> dict[str, int]:
    log("validating RLS expressions against bool expressions")
    actual_counts: dict[str, int] = {}
    for user in selected_users:
        rls_count = query_count(client, args, rls_expression(user))
        bool_count = query_count(client, args, bool_expression(user))
        if rls_count != bool_count:
            raise RuntimeError(
                f"count mismatch for {user}: rls={rls_count}, bool={bool_count}"
            )

        expected = expected_counts.get(user)
        if expected is not None and rls_count != expected:
            message = f"expected count mismatch for {user}: expected={expected}, actual={rls_count}"
            if args.strict_expected_counts:
                raise RuntimeError(message)
            log(f"warning: {message}")

        actual_counts[user] = rls_count
        log(f"validated {user}: {rls_count} accessible rows")
    actual_total_rows = query_count(client, args, "")
    expected_total_rows = total_rows(args)
    if actual_total_rows != expected_total_rows:
        message = f"total row count mismatch: expected={expected_total_rows}, actual={actual_total_rows}"
        if args.strict_expected_counts:
            raise RuntimeError(message)
        log(f"warning: {message}")
    actual_counts["all"] = actual_total_rows
    return actual_counts


def record_for_case(
    case: BenchCase,
    args: argparse.Namespace,
    measured_count: int | None,
    stats: dict[str, float],
) -> dict[str, Any]:
    selectivity = None
    if measured_count is not None:
        selectivity = measured_count / total_rows(args)
    return {
        "phase": case.phase,
        "user": case.user,
        "category": case.category,
        "expr_type": case.expr_type,
        "expected_count": case.expected_count,
        "measured_count": measured_count,
        "selectivity": selectivity,
        "nq": args.nq if case.phase == "search" else None,
        "topk": args.topk if case.phase == "search" else None,
        "runs": args.search_runs if case.phase == "search" else args.count_runs,
        "warmups": args.search_warmups if case.phase == "search" else args.count_warmups,
        "expression": case.expression,
        **stats,
    }


def parse_projection_poc_fields(line: str) -> dict[str, str]:
    match = PROJECTION_POC_RESULT_RE.search(line)
    if match is None:
        raise ValueError(f"not a projection POC result line: {line}")
    fields: dict[str, str] = {}
    for item in match.group("fields").split():
        key, separator, value = item.partition("=")
        if not separator:
            raise ValueError(f"invalid projection POC field {item!r} in line: {line}")
        fields[key] = value
    return fields


def projection_poc_binary(args: argparse.Namespace) -> Path:
    path = Path(args.projection_poc_binary)
    if not path.is_absolute():
        path = repo_root() / path
    return path


def projection_poc_env(args: argparse.Namespace) -> dict[str, str]:
    env = dict(os.environ)
    if args.projection_poc_ld_library_path:
        path = args.projection_poc_ld_library_path
    else:
        root = repo_root()
        path = f"{root / 'cmake_build/lib'}:{root / 'internal/core/output/lib'}"
    previous = env.get("LD_LIBRARY_PATH")
    env["LD_LIBRARY_PATH"] = f"{path}:{previous}" if previous else path
    return env


def run_projection_poc(args: argparse.Namespace, selected_users: list[str]) -> list[dict[str, Any]]:
    binary = projection_poc_binary(args)
    if not binary.exists():
        raise RuntimeError(
            f"projection POC binary {binary} does not exist; build it with "
            "`make -C cmake_build -j 16 all_tests` or pass --no-include-projection-poc"
        )

    command = [
        str(binary),
        "--gtest_filter=Expr.RLSCountBenchmarkArrayProjectionPOC",
    ]
    log("running C++ projection POC for current/setbit/direct/bool")
    started = time.perf_counter()
    completed = subprocess.run(
        command,
        cwd=repo_root(),
        env=projection_poc_env(args),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    if completed.returncode != 0:
        raise RuntimeError(
            "projection POC failed with exit code "
            f"{completed.returncode}\n{completed.stdout}"
        )

    selected = set(selected_users)
    records: list[dict[str, Any]] = []
    mode_fields = {
        "current": "model_a_current_p50_ms",
        "setbit": "model_a_setbit_p50_ms",
        "direct": "model_b_direct_p50_ms",
        "bool": "bool_p50_ms",
    }
    count_fields = {
        "current": "current_count",
        "setbit": "setbit_count",
        "direct": "direct_count",
        "bool": "bool_count",
    }

    projection_rows = None
    for line in completed.stdout.splitlines():
        header_match = PROJECTION_POC_HEADER_RE.search(line)
        if header_match is not None:
            projection_rows = int(header_match.group("rows"))
            break
    if projection_rows is None:
        raise RuntimeError(f"projection POC row count was not found. Raw output:\n{completed.stdout}")

    for line in completed.stdout.splitlines():
        if "RLS_PROJECTION_POC_RESULT" not in line:
            continue
        fields = parse_projection_poc_fields(line)
        user = fields["user"]
        if user not in selected:
            continue
        expected_count = int(fields["expected"])
        category = fields["category"]
        for mode, latency_field in mode_fields.items():
            measured_count = int(fields[count_fields[mode]])
            records.append(
                {
                    "phase": "projection_poc",
                    "user": user,
                    "category": category,
                    "expr_type": mode,
                    "expected_count": expected_count,
                    "measured_count": measured_count,
                    "selectivity": measured_count / projection_rows,
                    "nq": None,
                    "topk": None,
                    "runs": 3,
                    "warmups": 1,
                    "expression": "C++ Expr.RLSCountBenchmarkArrayProjectionPOC",
                    "projection_rows": projection_rows,
                    "heavy_shared_users_capacity": int(fields["heavy_shared_users_capacity"]),
                    "shared_user_doc_count": int(fields["shared_user_doc_count"]),
                    **scalar_stats(float(fields[latency_field])),
                }
            )

    expected_records = len(selected_users) * len(mode_fields)
    if len(records) != expected_records:
        raise RuntimeError(
            f"projection POC produced {len(records)} records, expected {expected_records}. "
            f"Raw output:\n{completed.stdout}"
        )

    log(f"projection POC completed in {elapsed_ms:.0f}ms with {len(records)} records")
    return records


def run_benchmark(
    client: MilvusClient,
    args: argparse.Namespace,
    selected_users: list[str],
    expected_counts: dict[str, int],
    actual_counts: dict[str, int],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    log("measuring count(*) query cases")
    for case in build_count_cases(selected_users, expected_counts, args):
        measured_count = actual_counts.get(case.user)
        stats = measure(
            lambda expression=case.expression: query_count(client, args, expression),
            args.count_warmups,
            args.count_runs,
        )
        record = record_for_case(case, args, measured_count, stats)
        records.append(record)
        log(
            f"count {case.user}/{case.expr_type}: "
            f"p50={record['latency_p50_ms']:.2f}ms "
            f"p95={record['latency_p95_ms']:.2f}ms"
        )

    if not args.skip_search:
        log("measuring vector search cases")
        query_vectors = make_query_vectors(args)
        for case in build_search_cases(selected_users, expected_counts, args):
            measured_count = actual_counts.get(case.user)
            stats = measure(
                lambda expression=case.expression: search_once(client, args, query_vectors, expression),
                args.search_warmups,
                args.search_runs,
            )
            record = record_for_case(case, args, measured_count, stats)
            records.append(record)
            log(
                f"search {case.user}/{case.expr_type}: "
                f"p50={record['latency_p50_ms']:.2f}ms "
                f"p95={record['latency_p95_ms']:.2f}ms"
            )

    if args.include_projection_poc:
        records.extend(run_projection_poc(args, selected_users))

    return records


def write_results(args: argparse.Namespace, metadata: dict[str, Any], records: list[dict[str, Any]]) -> None:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base_name = f"{args.collection}_{stamp}"
    json_path = output_dir / f"{base_name}.json"
    csv_path = output_dir / f"{base_name}.csv"

    with json_path.open("w", encoding="utf-8") as fp:
        json.dump({"metadata": metadata, "records": records}, fp, indent=2, sort_keys=True)

    fieldnames = sorted({key for record in records for key in record.keys()})
    with csv_path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    log(f"wrote JSON results to {json_path}")
    log(f"wrote CSV results to {csv_path}")


def collect_metadata(client: MilvusClient, args: argparse.Namespace, selected_users: list[str]) -> dict[str, Any]:
    try:
        server_version = client.get_server_version()
    except Exception:
        server_version = ""
    heavy_plans = heavy_array_plans(args)
    return {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "hostname": socket.gethostname(),
        "git_branch": git_value(["rev-parse", "--abbrev-ref", "HEAD"]),
        "git_sha": git_value(["rev-parse", "HEAD"]),
        "milvus_uri": args.uri,
        "milvus_server_version": server_version,
        "collection": args.collection,
        "rows": total_rows(args),
        "base_rows": args.rows,
        "dim": args.dim,
        "users": selected_users,
        "seed": args.seed,
        "query_seed": args.query_seed,
        "vector_index": {
            "skipped": args.skip_vector_index,
            "index_type": args.vector_index_type,
            "metric_type": "COSINE",
            "M": args.hnsw_m,
            "efConstruction": args.hnsw_ef_construction,
            "ef": args.hnsw_ef,
        },
        "scalar_index": {
            "rls_index_type": args.rls_index_type,
            "bool_index_type": args.bool_index_type,
            "bitmap_cardinality_limit": args.bitmap_cardinality_limit,
        },
        "array_schema": {
            "shared_users_max_capacity": args.max_array_capacity,
            "shared_users_element_max_length": args.array_varchar_max_length,
        },
        "heavy_shared_users": [
            {
                "capacity": plan.capacity,
                "rows": plan.rows,
                "user": plan.user,
                "category": plan.category,
            }
            for plan in heavy_plans
        ],
        "include_projection_poc": args.include_projection_poc,
        "projection_poc_binary": args.projection_poc_binary,
        "flush_before_heavy": args.flush_before_heavy,
        "heavy_rows_gt_5000": sum(plan.rows for plan in heavy_plans if plan.capacity > 5000),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark simulated RLS filters on Milvus 2.6 using existing scalar filtering."
    )
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default="19530")
    parser.add_argument("--uri", default=None, help="Overrides --host/--port when set.")
    parser.add_argument("--collection", default="rls_filter_bench_1m")
    parser.add_argument("--rows", type=int, default=1_010_000, help="Base rows before heavy shared_users rows.")
    parser.add_argument("--dim", type=int, default=128)
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--recreate", action="store_true")
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--run-only", action="store_true")
    parser.add_argument("--skip-validation", action="store_true")
    parser.add_argument("--strict-expected-counts", action="store_true")
    parser.add_argument("--users", default="all", help="Comma-separated users, or 'all'.")
    parser.add_argument("--output-dir", default="tests/benchmark/rls_simulation/results")

    parser.add_argument("--seed", type=int, default=19530)
    parser.add_argument("--query-seed", type=int, default=20260527)
    parser.add_argument("--progress-interval", type=int, default=100_000)

    parser.add_argument("--heavy-capacities", default=",".join(map(str, DEFAULT_HEAVY_CAPACITIES)))
    parser.add_argument("--heavy-rows-per-capacity", type=int, default=1)
    parser.add_argument("--heavy-start-user-index", type=int, default=8)
    parser.add_argument("--heavy-insert-batch-size", type=int, default=1)
    parser.add_argument("--flush-before-heavy", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--max-array-capacity", type=int, default=max(DEFAULT_HEAVY_CAPACITIES))
    parser.add_argument("--array-varchar-max-length", type=int, default=64)

    parser.add_argument("--count-warmups", type=int, default=10)
    parser.add_argument("--count-runs", type=int, default=100)
    parser.add_argument("--search-warmups", type=int, default=10)
    parser.add_argument("--search-runs", type=int, default=50)
    parser.add_argument("--skip-search", action="store_true")
    parser.add_argument("--nq", type=int, default=10)
    parser.add_argument("--topk", type=int, default=10)
    parser.add_argument("--include-projection-poc", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--projection-poc-binary", default="cmake_build/bin/all_tests")
    parser.add_argument(
        "--projection-poc-ld-library-path",
        default=None,
        help=(
            "Optional LD_LIBRARY_PATH prefix for the C++ projection POC. "
            "Defaults to cmake_build/lib and internal/core/output/lib."
        ),
    )

    parser.add_argument("--hnsw-m", type=int, default=16)
    parser.add_argument("--hnsw-ef-construction", type=int, default=200)
    parser.add_argument("--hnsw-ef", type=int, default=64)
    parser.add_argument("--vector-index-type", choices=("HNSW", "FLAT"), default="HNSW")
    parser.add_argument("--skip-vector-index", action="store_true")
    parser.add_argument(
        "--rls-index-type",
        choices=SCALAR_INDEX_TYPES,
        default="BITMAP",
        help=(
            "Scalar index type for owner/shared_users/shared_departments. "
            "Use AUTOINDEX to exercise Milvus scalar auto index; in this branch, "
            "varchar and varchar-array auto index resolves to HYBRID."
        ),
    )
    parser.add_argument(
        "--bool-index-type",
        choices=SCALAR_INDEX_TYPES,
        default="BITMAP",
        help="Scalar index type for generated can_access_uXX bool fields.",
    )
    parser.add_argument(
        "--bitmap-cardinality-limit",
        type=int,
        default=None,
        help=(
            "Optional bitmap_cardinality_limit passed to scalar indexes. "
            "For AUTOINDEX on varchar/array fields, this is preserved when proxy "
            "resolves the request to HYBRID."
        ),
    )

    parser.add_argument("--insert-timeout", type=float, default=300)
    parser.add_argument("--flush-timeout", type=float, default=1800)
    parser.add_argument("--index-timeout", type=float, default=7200)
    parser.add_argument("--load-timeout", type=float, default=1800)
    parser.add_argument("--query-timeout", type=float, default=300)

    args = parser.parse_args()
    args.heavy_capacities = parse_csv_ints(args.heavy_capacities)
    args.uri = args.uri or f"http://{args.host}:{args.port}"

    positive_ints = {
        "--rows": args.rows,
        "--dim": args.dim,
        "--batch-size": args.batch_size,
        "--progress-interval": args.progress_interval,
        "--heavy-insert-batch-size": args.heavy_insert_batch_size,
        "--max-array-capacity": args.max_array_capacity,
        "--array-varchar-max-length": args.array_varchar_max_length,
        "--count-runs": args.count_runs,
        "--search-runs": args.search_runs,
        "--nq": args.nq,
        "--topk": args.topk,
        "--hnsw-m": args.hnsw_m,
        "--hnsw-ef-construction": args.hnsw_ef_construction,
        "--hnsw-ef": args.hnsw_ef,
    }
    if args.bitmap_cardinality_limit is not None:
        positive_ints["--bitmap-cardinality-limit"] = args.bitmap_cardinality_limit
    for name, value in positive_ints.items():
        if value <= 0:
            raise ValueError(f"{name} must be positive")
    non_negative_ints = {
        "--count-warmups": args.count_warmups,
        "--search-warmups": args.search_warmups,
        "--heavy-rows-per-capacity": args.heavy_rows_per_capacity,
        "--heavy-start-user-index": args.heavy_start_user_index,
    }
    for name, value in non_negative_ints.items():
        if value < 0:
            raise ValueError(f"{name} must be non-negative")
    if args.heavy_start_user_index >= USER_COUNT:
        raise ValueError("--heavy-start-user-index must be less than the test user count")
    if max(args.heavy_capacities) > args.max_array_capacity:
        raise ValueError("--max-array-capacity must be >= the largest heavy capacity")
    if args.skip_vector_index and not args.prepare_only:
        raise ValueError(
            "Milvus requires a vector index before loading a collection with a vector field; "
            "use --vector-index-type FLAT with --skip-search for scalar-only count benchmarks"
        )
    heavy_gt_5000 = args.heavy_rows_per_capacity * sum(1 for capacity in args.heavy_capacities if capacity > 5000)
    if heavy_gt_5000 > 200:
        raise ValueError(
            "total rows with shared_users capacity > 5000 must not exceed 200; "
            f"got {heavy_gt_5000}"
        )
    if args.prepare_only and args.run_only:
        raise ValueError("--prepare-only and --run-only cannot be used together")
    return args


def main() -> int:
    args = parse_args()
    selected_users = parse_users(args.users)
    expected_counts = build_expected_counts(args)

    require_pymilvus()
    log(f"connecting to {args.uri}")
    client = MilvusClient(uri=args.uri)
    try:
        if args.run_only:
            if not collection_exists(client, args.collection):
                raise RuntimeError(f"collection {args.collection!r} does not exist")
            load_collection(client, args)
        else:
            expected_counts = prepare_collection(client, args)

        if args.prepare_only:
            log("prepare-only completed")
            return 0

        if args.skip_validation:
            actual_counts = {user: expected_counts[user] for user in selected_users}
            actual_counts["all"] = total_rows(args)
            log("skipping validation; using configured expected counts")
        else:
            actual_counts = validate_counts(client, args, selected_users, expected_counts)

        metadata = collect_metadata(client, args, selected_users)
        records = run_benchmark(client, args, selected_users, expected_counts, actual_counts)
        write_results(args, metadata, records)
        return 0
    finally:
        close = getattr(client, "close", None)
        if callable(close):
            close()


if __name__ == "__main__":
    sys.exit(main())
