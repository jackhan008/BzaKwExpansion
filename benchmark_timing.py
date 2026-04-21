"""
timing benchmark for a single theme — minimax / China market
Instruments every major sub-step and prints a summary table.
"""

import time
import sys
import os
import concurrent.futures
import json
import pandas as pd

# ── env setup ────────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(__file__))
import config
import job_store
from ai_expander import AIExpander
from db_manager import DBManager
from matcher import QueryMatcher

THEME  = "minimax"
MARKET = "China"

# ── helpers ──────────────────────────────────────────────────────────────────
class Timer:
    def __init__(self, label):
        self.label = label
        self.t0 = None
        self.elapsed = None

    def __enter__(self):
        self.t0 = time.perf_counter()
        return self

    def __exit__(self, *_):
        self.elapsed = time.perf_counter() - self.t0
        print(f"  [{self.label}] {self.elapsed:.3f}s")


timings = {}   # label -> seconds

def record(label, elapsed):
    timings[label] = elapsed


# ── 1. init ───────────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"  Benchmark: theme='{THEME}'  market={MARKET}")
print(f"{'='*60}\n")

use_azure = config.USE_AZURE_DATASOURCE
print(f"USE_AZURE_DATASOURCE = {use_azure}\n")

t0_total = time.perf_counter()

with Timer("Init DBManager") as t:
    db = DBManager(market=MARKET, use_azure=use_azure)
    if not use_azure:
        db.initialize_db()
record("Init DBManager", t.elapsed)

with Timer("Init AIExpander") as t:
    expander = AIExpander()
record("Init AIExpander", t.elapsed)

with Timer("Init QueryMatcher") as t:
    matcher = QueryMatcher(db)
record("Init QueryMatcher", t.elapsed)


# ── 2. step 1: AI expand ──────────────────────────────────────────────────────
print("\n--- Step 1: AI Expand ---")
with Timer("AI Expand (total)") as t:
    expanded_keywords = expander.expand_search_theme(THEME, market=MARKET)
record("Step1 AI Expand", t.elapsed)
print(f"  expanded keywords ({len(expanded_keywords)}): {expanded_keywords}")


# ── 3. step 2: matching — instrument per-keyword ─────────────────────────────
print("\n--- Step 2: Matching ---")

hard_times   = []
vector_times = []
import Levenshtein

all_results = {}

for keyword in expanded_keywords:
    clean_keyword = keyword.replace(" ", "").replace("\u3000", "")

    # Hard match
    t_h0 = time.perf_counter()
    df_hard = db.query_sqlite_contains(keyword)
    hard_times.append(time.perf_counter() - t_h0)

    for _, row in df_hard.iterrows():
        q = row['normalized_query']
        klen = len(clean_keyword)
        qlen = len(q.replace(" ","").replace("\u3000",""))
        relevance = klen / max(qlen, klen) if max(qlen, klen) > 0 else 0
        if q not in all_results:
            all_results[q] = {
                'normalized_query': q, 'SRPV': row['SRPV'],
                'AdClick': row['AdClick'], 'revenue': row['revenue'],
                'score_hard': 0, 'score_vector': 0,
                'relevance_accum': 0, 'matched_keyword': keyword,
            }
        all_results[q]['score_hard'] = 2
        all_results[q]['relevance_accum'] = max(all_results[q]['relevance_accum'], relevance)

    # Vector match
    t_v0 = time.perf_counter()
    df_vector = db.query_vector_similarity(keyword, n_results=100)
    vector_times.append(time.perf_counter() - t_v0)

    for _, row in df_vector.iterrows():
        q = row['normalized_query']
        similarity = 1 - row['distance']
        if similarity < 0.8:
            continue
        clean_q = q.replace(" ","").replace("\u3000","")
        if clean_keyword in clean_q or Levenshtein.distance(clean_q, clean_keyword) < len(clean_keyword)/5:
            if q not in all_results:
                all_results[q] = {
                    'normalized_query': q, 'SRPV': row['SRPV'],
                    'AdClick': row['AdClick'], 'revenue': row['revenue'],
                    'score_hard': 0, 'score_vector': 0,
                    'relevance_accum': 0, 'matched_keyword': keyword,
                }
            all_results[q]['score_vector'] = 1
            all_results[q]['relevance_accum'] = max(all_results[q]['relevance_accum'], similarity)

hard_total   = sum(hard_times)
vector_total = sum(vector_times)
match_total  = hard_total + vector_total

print(f"  Hard  match: {len(hard_times)} keywords  total={hard_total:.3f}s  avg={hard_total/len(hard_times):.3f}s/kw")
print(f"  Vector match: {len(vector_times)} keywords  total={vector_total:.3f}s  avg={vector_total/len(vector_times):.3f}s/kw")
print(f"  Match total: {match_total:.3f}s")
print(f"  Candidates before relevance filter: {len(all_results)}")

record("Step2 Hard Match total", hard_total)
record("Step2 Vector Match total", vector_total)
record("Step2 Match total", match_total)

# Build dataframe (mirror matcher.py logic)
results_list = []
for q, data in all_results.items():
    data['Score'] = data['score_hard'] + data['score_vector']
    results_list.append(data)

results_df = pd.DataFrame(results_list)
if not results_df.empty:
    results_df = results_df[results_df['relevance_accum'] >= 0.4]
    results_df.rename(columns={'relevance_accum': 'Relevance'}, inplace=True)

print(f"  Matches after relevance filter: {len(results_df)}")


# ── 4. step 3: validation ─────────────────────────────────────────────────────
print("\n--- Step 3: Validation ---")

queries_to_validate = results_df['normalized_query'].unique().tolist() if not results_df.empty else []
print(f"  Unique queries to validate: {len(queries_to_validate)}")

batch_size = 25
batches = [(queries_to_validate[i:i+batch_size], i//batch_size)
           for i in range(0, len(queries_to_validate), batch_size)]
print(f"  Batches: {len(batches)} (batch_size={batch_size})")

batch_times = []

def timed_validate_batch(batch, idx):
    t_b0 = time.perf_counter()
    result = expander._validate_batch(THEME, batch, idx, market=MARKET)
    elapsed = time.perf_counter() - t_b0
    return result, elapsed

with Timer("Validation (total wall-clock, parallel)") as t_val:
    all_validation = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        futures = {executor.submit(timed_validate_batch, batch, idx): idx
                   for batch, idx in batches}
        for future in concurrent.futures.as_completed(futures):
            result, elapsed = future.result()
            all_validation.update(result)
            batch_times.append(elapsed)

record("Step3 Validation total (wall)", t_val.elapsed)

if batch_times:
    print(f"  Per-batch times: min={min(batch_times):.3f}s  max={max(batch_times):.3f}s  avg={sum(batch_times)/len(batch_times):.3f}s")
    print(f"  Sum of all batch times (serial equiv): {sum(batch_times):.3f}s")
    print(f"  Parallelism speedup: {sum(batch_times)/t_val.elapsed:.1f}x")

valid_count   = sum(1 for v in all_validation.values() if v.get("is_valid"))
invalid_count = len(all_validation) - valid_count
print(f"  Valid={valid_count}  Invalid={invalid_count}")


# ── 5. summary ────────────────────────────────────────────────────────────────
total_elapsed = time.perf_counter() - t0_total

print(f"\n{'='*60}")
print(f"  TIMING SUMMARY  (theme='{THEME}', market={MARKET})")
print(f"{'='*60}")
print(f"  {'Stage':<40} {'Time':>8}  {'%':>6}")
print(f"  {'-'*56}")

order = [
    "Init DBManager",
    "Init AIExpander",
    "Init QueryMatcher",
    "Step1 AI Expand",
    "Step2 Hard Match total",
    "Step2 Vector Match total",
    "Step2 Match total",
    "Step3 Validation total (wall)",
]
for label in order:
    if label not in timings:
        continue
    v = timings[label]
    pct = v / total_elapsed * 100
    indent = "    " if label.startswith("Step2") and "total" not in label.split()[-1:] else "  "
    print(f"  {label:<40} {v:>7.3f}s  {pct:>5.1f}%")

print(f"  {'─'*56}")
print(f"  {'TOTAL (wall-clock)':<40} {total_elapsed:>7.3f}s  100.0%")
print(f"{'='*60}\n")
