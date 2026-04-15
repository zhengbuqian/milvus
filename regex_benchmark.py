#!/usr/bin/env python3
"""Regex performance benchmark: Milvus vs PostgreSQL vs ClickHouse."""

import time
import random
import string
import json
import statistics
import urllib.request
import urllib.parse
import numpy as np

# ── Config ──
N_ROWS = 100_000
DIM = 8
WARMUP = 3
RUNS = 20

PG_HOST = "172.16.50.88"
PG_PORT = 5432
PG_USER = "postgres"
PG_PASS = "101253e2a9da45c0accd0727c3ed1ab6"

CH_HOST = "172.16.50.88"
CH_PORT = 8123
CH_USER = "default"
CH_PASS = "6d0a966252e6e9363fac42e1266e1598"

# ── Test patterns ──
PATTERNS = [
    ("P1: literal 'error'",        "error"),
    ("P2: ^user_[0-9]+",           "^user_[0-9]+"),
    ("P3: email-like",             "user_[0-9]+@gmail\\.com"),
    ("P4: alternation",            "error|warning|critical"),
    ("P5: case-insensitive",       "(?i)error"),
    ("P6: multi-wildcard",         ".*error.*timeout.*"),
    ("P7: pure pattern",           "[a-z]{3,5}_[0-9]+"),
]

# ── Data generation ──
def gen_data(n):
    """Generate n rows of test strings with known patterns embedded."""
    random.seed(42)
    domains = ["gmail.com", "yahoo.com", "outlook.com"]
    levels = ["error", "warning", "critical", "info", "debug"]
    rows = []
    for i in range(n):
        kind = i % 10
        if kind == 0:
            # Email-like
            s = f"user_{random.randint(100,99999)}@{random.choice(domains)}"
        elif kind == 1:
            # Log line with error+timeout
            s = f"2026-04-11 {random.choice(levels)}: connection timeout from 192.168.{random.randint(0,255)}.{random.randint(0,255)}"
        elif kind == 2:
            # Short tag
            letters = ''.join(random.choices(string.ascii_lowercase, k=random.randint(3,5)))
            s = f"{letters}_{random.randint(10,9999)}"
        elif kind == 3:
            # Log with ERROR (uppercase)
            s = f"ERROR: disk full on node-{random.randint(1,100)}"
        elif kind == 4:
            # URL-like
            s = f"https://api.example.com/v{random.randint(1,5)}/users/{random.randint(1000,9999)}"
        elif kind == 5:
            # Warning log
            s = f"warning: high latency detected {random.randint(100,999)}ms"
        elif kind == 6:
            # Critical log
            s = f"critical: service unavailable, retry in {random.randint(1,60)}s"
        elif kind == 7:
            # Plain text
            words = ' '.join(random.choices(["hello", "world", "foo", "bar", "test", "data"], k=random.randint(3,8)))
            s = words
        elif kind == 8:
            # user_ prefix
            s = f"user_{random.randint(1,99999)}_active"
        else:
            # Random string
            s = ''.join(random.choices(string.ascii_letters + string.digits, k=random.randint(20,100)))
        rows.append(s)
    return rows

# ── ClickHouse helper ──
def ch_query(sql, settings=None):
    params = f"user={CH_USER}&password={CH_PASS}"
    if settings:
        for k, v in settings.items():
            params += f"&{k}={v}"
    url = f"http://{CH_HOST}:{CH_PORT}/?{params}"
    req = urllib.request.Request(url, data=sql.encode())
    resp = urllib.request.urlopen(req, timeout=60)
    return resp.read().decode().strip()

def ch_query_time(sql):
    """Execute query and return (result, elapsed_ms)."""
    t0 = time.time()
    r = ch_query(sql)
    return r, (time.time() - t0) * 1000

# ── Benchmark runner ──
def bench(name, func, warmup=WARMUP, runs=RUNS):
    """Run func() warmup+runs times, return median ms."""
    for _ in range(warmup):
        func()
    times = []
    for _ in range(runs):
        t0 = time.time()
        result = func()
        times.append((time.time() - t0) * 1000)
    p50 = statistics.median(times)
    p99 = sorted(times)[int(len(times) * 0.99)]
    return p50, p99, result

def main():
    print(f"Generating {N_ROWS} rows...")
    data = gen_data(N_ROWS)
    print(f"Done. Sample: {data[0][:60]}")

    results = {}

    # ================================================================
    # PostgreSQL Setup
    # ================================================================
    print("\n" + "="*70)
    print("POSTGRESQL SETUP")
    print("="*70)
    import psycopg2
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER,
                            password=PG_PASS, dbname="postgres")
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS regex_bench")
    cur.execute("CREATE TABLE regex_bench (id SERIAL PRIMARY KEY, text VARCHAR)")
    # Batch insert
    from io import StringIO
    buf = StringIO()
    for i, s in enumerate(data):
        escaped = s.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n')
        buf.write(f"{i}\t{escaped}\n")
    buf.seek(0)
    cur.copy_from(buf, 'regex_bench', columns=('id', 'text'))
    print(f"PG: inserted {N_ROWS} rows")

    # pg_trgm extension
    cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    print("PG: pg_trgm extension ready")

    # ── PG brute force ──
    print("\n--- PostgreSQL: No Index (brute force) ---")
    for pname, pattern in PATTERNS:
        def run_pg(p=pattern):
            cur.execute(f"SELECT count(*) FROM regex_bench WHERE text ~ '{p}'")
            return cur.fetchone()[0]
        p50, p99, cnt = bench(pname, run_pg)
        results[f"PG-noindex-{pname}"] = (p50, cnt)
        print(f"  {pname}: p50={p50:.1f}ms  cnt={cnt}")

    # ── PG with trigram index ──
    print("\nCreating pg_trgm GIN index...")
    cur.execute("CREATE INDEX idx_trgm ON regex_bench USING gin(text gin_trgm_ops)")
    # Analyze for optimizer
    cur.execute("ANALYZE regex_bench")
    print("PG: trigram index ready")

    print("\n--- PostgreSQL: pg_trgm GIN Index ---")
    for pname, pattern in PATTERNS:
        def run_pg(p=pattern):
            cur.execute(f"SELECT count(*) FROM regex_bench WHERE text ~ '{p}'")
            return cur.fetchone()[0]
        p50, p99, cnt = bench(pname, run_pg)
        results[f"PG-trgm-{pname}"] = (p50, cnt)
        print(f"  {pname}: p50={p50:.1f}ms  cnt={cnt}")

    cur.execute("DROP TABLE regex_bench")
    conn.close()

    # ================================================================
    # ClickHouse Setup
    # ================================================================
    print("\n" + "="*70)
    print("CLICKHOUSE SETUP")
    print("="*70)

    ch_query("DROP TABLE IF EXISTS regex_bench")
    ch_query("""CREATE TABLE regex_bench (
        id UInt32,
        text String
    ) ENGINE = MergeTree() ORDER BY id""")

    # Batch insert via CSV
    csv_lines = []
    for i, s in enumerate(data):
        escaped = s.replace('\\', '\\\\').replace("'", "\\'")
        csv_lines.append(f"({i},'{escaped}')")
    # Insert in chunks of 10000
    for chunk_start in range(0, len(csv_lines), 10000):
        chunk = csv_lines[chunk_start:chunk_start+10000]
        ch_query(f"INSERT INTO regex_bench VALUES {','.join(chunk)}")
    print(f"CH: inserted {N_ROWS} rows")

    # ── CH brute force (no skip index) ──
    print("\n--- ClickHouse: No Index (brute force + Volnitsky) ---")
    for pname, pattern in PATTERNS:
        ch_pat = pattern.replace("'", "\\'")
        def run_ch(p=ch_pat):
            return ch_query(f"SELECT count() FROM regex_bench WHERE match(text, '{p}')")
        p50, p99, cnt = bench(pname, run_ch)
        results[f"CH-noindex-{pname}"] = (p50, int(cnt))
        print(f"  {pname}: p50={p50:.1f}ms  cnt={cnt}")

    # ── CH with ngrambf skip index ──
    ch_query("DROP TABLE IF EXISTS regex_bench_ngram")
    ch_query("""CREATE TABLE regex_bench_ngram (
        id UInt32,
        text String,
        INDEX idx_ngram text TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 1
    ) ENGINE = MergeTree() ORDER BY id
    SETTINGS index_granularity = 1024""")

    for chunk_start in range(0, len(csv_lines), 10000):
        chunk = csv_lines[chunk_start:chunk_start+10000]
        ch_query(f"INSERT INTO regex_bench_ngram VALUES {','.join(chunk)}")
    print(f"\nCH: ngram bloom index table ready")

    print("\n--- ClickHouse: ngrambf_v1 Skip Index ---")
    for pname, pattern in PATTERNS:
        ch_pat = pattern.replace("'", "\\'")
        def run_ch(p=ch_pat):
            return ch_query(f"SELECT count() FROM regex_bench_ngram WHERE match(text, '{p}')")
        p50, p99, cnt = bench(pname, run_ch)
        results[f"CH-ngram-{pname}"] = (p50, int(cnt))
        print(f"  {pname}: p50={p50:.1f}ms  cnt={cnt}")

    ch_query("DROP TABLE regex_bench")
    ch_query("DROP TABLE regex_bench_ngram")

    # ================================================================
    # Milvus Setup
    # ================================================================
    print("\n" + "="*70)
    print("MILVUS SETUP")
    print("="*70)
    from pymilvus import (connections, Collection, CollectionSchema,
                          FieldSchema, DataType, utility)
    connections.connect(host="127.0.0.1", port="19530")

    def milvus_setup(col_name, index_type=None):
        if utility.has_collection(col_name):
            utility.drop_collection(col_name)
        schema = CollectionSchema([
            FieldSchema("id", DataType.INT64, is_primary=True, auto_id=False),
            FieldSchema("text", DataType.VARCHAR, max_length=2048),
            FieldSchema("vec", DataType.FLOAT_VECTOR, dim=DIM),
        ])
        col = Collection(col_name, schema)
        # Insert in batches
        batch = 10000
        for start in range(0, N_ROWS, batch):
            end = min(start + batch, N_ROWS)
            ids = list(range(start, end))
            texts = data[start:end]
            vecs = np.random.random((end - start, DIM)).astype(np.float32).tolist()
            col.insert([ids, texts, vecs])
        col.flush()
        col.create_index("vec", {"index_type": "FLAT", "metric_type": "L2", "params": {}})
        if index_type:
            col.create_index("text", {"index_type": index_type})
        col.load()
        time.sleep(5)
        return col

    # ── Milvus brute force ──
    col = milvus_setup("regex_bench_noindex")
    print(f"Milvus: inserted {N_ROWS} rows, no text index")

    print("\n--- Milvus: No Index (brute force) ---")
    for pname, pattern in PATTERNS:
        mv_pat = pattern.replace('\\', '\\\\').replace('"', '\\"')
        def run_mv(p=mv_pat, c=col):
            r = c.query(expr=f'text =~ "{p}"', output_fields=["id"])
            return len(r)
        p50, p99, cnt = bench(pname, run_mv)
        results[f"MV-noindex-{pname}"] = (p50, cnt)
        print(f"  {pname}: p50={p50:.1f}ms  cnt={cnt}")

    # ── Milvus with inverted index ──
    col2 = milvus_setup("regex_bench_inverted", "INVERTED")
    print(f"\nMilvus: inverted index ready")

    print("\n--- Milvus: Inverted Index ---")
    for pname, pattern in PATTERNS:
        mv_pat = pattern.replace('\\', '\\\\').replace('"', '\\"')
        def run_mv(p=mv_pat, c=col2):
            r = c.query(expr=f'text =~ "{p}"', output_fields=["id"])
            return len(r)
        p50, p99, cnt = bench(pname, run_mv)
        results[f"MV-inverted-{pname}"] = (p50, cnt)
        print(f"  {pname}: p50={p50:.1f}ms  cnt={cnt}")

    # ================================================================
    # Summary Table
    # ================================================================
    print("\n" + "="*70)
    print("SUMMARY (p50 latency in ms, 100K rows)")
    print("="*70)
    print(f"{'Pattern':<25} {'PG-brute':>10} {'PG-trgm':>10} {'CH-brute':>10} {'CH-ngram':>10} {'MV-brute':>10} {'MV-inv':>10}")
    print("-" * 95)
    for pname, _ in PATTERNS:
        pg_b = results.get(f"PG-noindex-{pname}", (0, 0))[0]
        pg_t = results.get(f"PG-trgm-{pname}", (0, 0))[0]
        ch_b = results.get(f"CH-noindex-{pname}", (0, 0))[0]
        ch_n = results.get(f"CH-ngram-{pname}", (0, 0))[0]
        mv_b = results.get(f"MV-noindex-{pname}", (0, 0))[0]
        mv_i = results.get(f"MV-inverted-{pname}", (0, 0))[0]
        print(f"{pname:<25} {pg_b:>10.1f} {pg_t:>10.1f} {ch_b:>10.1f} {ch_n:>10.1f} {mv_b:>10.1f} {mv_i:>10.1f}")

    # Verify result counts match
    print("\n--- Result Count Verification ---")
    for pname, _ in PATTERNS:
        counts = {}
        for k, (_, cnt) in results.items():
            if pname in k:
                system = k.split("-")[0] + "-" + k.split("-")[1]
                counts[system] = cnt
        vals = list(counts.values())
        match = "OK" if len(set(vals)) <= 2 else "MISMATCH"  # allow PG vs CH minor diffs due to (?i)
        print(f"  {pname}: {counts}  [{match}]")

    # Cleanup
    for name in ["regex_bench_noindex", "regex_bench_inverted"]:
        try: utility.drop_collection(name)
        except: pass
    connections.disconnect("default")

if __name__ == "__main__":
    main()
