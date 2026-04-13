"""
test_cn_runner.py — 批量测试 Data/TestData/test_cn.txt 中的 brand_theme（China 市场）

- 去除 "/" 及后面的部分（如 "品牌直通位"、"通品" 等）
- 对每个 theme 单独计时
- 结果输出到 Data/TestData/test_cn_results.csv
- 耗时汇总输出到 Data/TestData/test_cn_timing.csv
"""

import sys
import os
import re
import time
import uuid
import pandas as pd

# Fix Windows console encoding for Chinese characters
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr.encoding != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
from db_manager import DBManager
from ai_expander import AIExpander
from matcher import QueryMatcher
from main import process_theme
from logger import get_logger
import job_store
import config

logger = get_logger(__name__)

MARKET = "China"
INPUT_FILE  = os.path.join(os.path.dirname(__file__), "Data", "TestData", "test_cn.txt")
OUTPUT_CSV  = os.path.join(os.path.dirname(__file__), "Data", "TestData", "test_cn_azure_results.csv")
TIMING_CSV  = os.path.join(os.path.dirname(__file__), "Data", "TestData", "test_cn_azure_timing.csv")


def clean_brand(raw: str) -> str:
    """取 '/' 前的部分并 strip，空行返回空字符串。"""
    return raw.split("/")[0].strip()


def load_themes(path: str) -> list[tuple[str, str]]:
    """
    返回 [(raw_line, cleaned_brand), ...] 列表，跳过空行。
    保留 raw_line 以便在 timing 中记录原始输入。
    """
    with open(path, encoding="utf-8") as f:
        lines = f.readlines()

    result = []
    for line in lines:
        raw = line.strip()
        if not raw:
            continue
        cleaned = clean_brand(raw)
        if cleaned:
            result.append((raw, cleaned))
    return result


def main():
    job_store.init_db()

    print(f"Initializing DBManager for {MARKET}...")
    db_manager = DBManager(market=MARKET)
    db_manager.initialize_db()

    print("Initializing AIExpander...")
    expander = AIExpander()

    print("Initializing QueryMatcher...")
    matcher = QueryMatcher(db_manager)

    themes = load_themes(INPUT_FILE)[:10]
    print(f"Loaded {len(themes)} themes (first 10) from {INPUT_FILE}")

    all_results_dfs = []
    timing_rows = []

    job_id = uuid.uuid4().hex[:8]

    for i, (raw_line, brand) in enumerate(themes):
        theme_id = f"{job_id}-t{i}"
        print(f"[{i+1}/{len(themes)}] Processing: {brand!r}  (raw: {raw_line!r})")

        t_start = time.time()
        try:
            df, expanded_keywords = process_theme(
                brand, expander, matcher,
                market=MARKET, job_id=job_id, theme_id=theme_id
            )
            elapsed = time.time() - t_start
            status = "done"
            match_count = len(df) if not df.empty else 0

            if not df.empty:
                df = df.copy()
                df["SearchTheme"] = brand
                df["RawInput"]    = raw_line
                all_results_dfs.append(df)

        except Exception as e:
            elapsed = time.time() - t_start
            status = f"error: {e}"
            match_count = 0
            print(f"  ERROR: {e}")

        timing_rows.append({
            "index":       i + 1,
            "raw_input":   raw_line,
            "brand":       brand,
            "status":      status,
            "match_count": match_count,
            "elapsed_sec": round(elapsed, 2),
        })
        print(f"  -> {status} | matches={match_count} | {elapsed:.1f}s")

    # ── Write results CSV ──
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

    if all_results_dfs:
        final_df = pd.concat(all_results_dfs, ignore_index=True)
        cols = ["SearchTheme", "RawInput"] + [
            c for c in final_df.columns if c not in ("SearchTheme", "RawInput")
        ]
        final_df = final_df[cols]
    else:
        final_df = pd.DataFrame(columns=[
            "SearchTheme", "RawInput", "normalized_query",
            "Relevance", "SRPV", "AdClick", "revenue", "Score"
        ])

    final_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"\nResults saved to: {OUTPUT_CSV}  ({len(final_df)} rows)")

    # ── Write timing CSV ──
    timing_df = pd.DataFrame(timing_rows)
    total_elapsed = timing_df["elapsed_sec"].sum()
    timing_df.to_csv(TIMING_CSV, index=False, encoding="utf-8-sig")
    print(f"Timing  saved to: {TIMING_CSV}")
    print(f"Total elapsed: {total_elapsed:.1f}s  ({total_elapsed/60:.1f} min)")


if __name__ == "__main__":
    main()
