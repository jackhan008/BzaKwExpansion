"""
Batch keyword expansion for Japan market.

Reads:  Data/TestData/bza_jp_whitelist_account_with_brand.xlsx
Output: Data/TestData/bza_jp_expand_results.xlsx

Each row (account) is treated as one expansion job using CoreBrandKeyword as
the search theme.  The domain(s) from the Domains column are passed as light
brand context so the AI can better scope the keywords.

Usage:
    python batch_jp_expand.py [--workers N] [--limit N] [--resume]

Options:
    --workers N   Parallel theme workers (default 3).  Each worker spawns up
                  to 30 validation threads, so keep this ≤ 5 unless your
                  Azure quota is very generous.
    --limit N     Only process the first N accounts (useful for test runs).
    --resume      Skip accounts whose AccountId already appears in the output
                  file (allows resuming an interrupted run).
"""

import argparse
import ast
import os
import re
import uuid
import sys
import concurrent.futures

import pandas as pd

# ── project imports ────────────────────────────────────────────────────────────
from db_manager import DBManager
from ai_expander import AIExpander
from matcher import QueryMatcher
from main import process_themes_parallel
from logger import get_logger
import config

logger = get_logger(__name__)

# ── paths ──────────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(BASE_DIR, "Data", "TestData",
                          "bza_jp_whitelist_account_with_brand.xlsx")
OUTPUT_FILE = os.path.join(BASE_DIR, "Data", "TestData",
                           "bza_jp_expand_results.xlsx")

MARKET = "Japan"

# Unicode ranges: hiragana, katakana, CJK unified ideographs + compatibility ideographs
_JP_PATTERN = re.compile(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF\uF900-\uFAFF]')


def is_japanese(text: str) -> bool:
    return bool(_JP_PATTERN.search(text))


# ── helpers ────────────────────────────────────────────────────────────────────

def parse_list_field(value) -> list:
    """Parse a string like "['a.com', 'b.com']" into a Python list."""
    if not value:
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = ast.literal_eval(str(value))
        return parsed if isinstance(parsed, list) else [str(parsed)]
    except Exception:
        return [str(value)]


def domains_to_context(domains: list) -> str:
    """Return a short brand-context string from domain names."""
    if not domains:
        return ""
    return "Brand domain(s): " + ", ".join(domains)


def _run_phonetic_for_brand(brand: str, expander: AIExpander,
                             matcher: QueryMatcher, job_id: str,
                             existing_queries: set) -> tuple[str, str, list]:
    """Get katakana phonetic, run matcher, validate, return (brand, katakana, rows).

    existing_queries: normalized_query strings already found by normal expansion
    for this brand — phonetic results that duplicate these are skipped.
    """
    katakana = expander.get_katakana_phonetic(brand)
    if not katakana:
        logger.warning(f"[phonetic] Brand '{brand}': no katakana returned, skipping")
        return brand, "", []

    theme_id = uuid.uuid4().hex[:8]
    df_result = matcher.process_expanded_keywords(
        [katakana], job_id=job_id, theme_id=theme_id, device_types=["pc"],
    )

    if df_result is None or df_result.empty:
        logger.info(f"[phonetic] Brand '{brand}' ({katakana}): no matches")
        return brand, katakana, []

    # Keep only queries not already found by normal expansion
    df_new = df_result[~df_result["normalized_query"].isin(existing_queries)]
    if df_new.empty:
        logger.info(f"[phonetic] Brand '{brand}' ({katakana}): all matches already covered")
        return brand, katakana, []

    queries = df_new["normalized_query"].dropna().tolist()
    phonetic_ctx = (
        f"The search keyword '{katakana}' is the Japanese katakana phonetic "
        f"reading of the brand '{brand}'. Queries that phonetically match "
        f"this brand name are valid even if they don't contain the Latin spelling."
    )
    validation = expander.validate_queries(
        brand, queries, market=MARKET,
        job_id=job_id, theme_id=theme_id,
        brand_keywords=[katakana], brand_context=phonetic_ctx,
    )

    result_rows = []
    for _, row in df_new.iterrows():
        q = row.get("normalized_query")
        vr = validation.get(q, {})
        if not vr.get("is_valid", True):
            continue
        result_rows.append({
            "normalized_query": q,
            "Relevance":        row.get("Relevance"),
            "SRPV":             row.get("SRPV"),
            "AdClick":          row.get("AdClick"),
            "revenue":          row.get("revenue"),
            "Score":            row.get("Score"),
            "matched_keyword":  row.get("matched_keyword"),
            "AI_Valid":         True,
            "AI_Reason":        vr.get("reason", ""),
        })

    logger.info(f"[phonetic] Brand '{brand}' ({katakana}): "
                f"{len(df_new)} new matches → {len(result_rows)} valid")
    return brand, katakana, result_rows


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Batch JP keyword expansion")
    parser.add_argument("--workers", type=int, default=3,
                        help="Parallel theme workers (default 3)")
    parser.add_argument("--limit", type=int, default=0,
                        help="Only process first N rows (0 = all)")
    parser.add_argument("--resume", action="store_true",
                        help="Skip AccountIds already present in the output file")
    args = parser.parse_args()

    # ── load input ─────────────────────────────────────────────────────────────
    logger.info(f"Reading input: {INPUT_FILE}")
    df_input = pd.read_excel(INPUT_FILE, engine="openpyxl")
    logger.info(f"Loaded {len(df_input)} accounts")

    if args.limit > 0:
        df_input = df_input.head(args.limit)
        logger.info(f"Limiting to first {args.limit} accounts")

    # ── resume: drop already-processed accounts ────────────────────────────────
    done_ids: set = set()
    df_base = pd.DataFrame()  # existing data to preserve across chunk writes
    if args.resume and os.path.exists(OUTPUT_FILE):
        df_base = pd.read_excel(OUTPUT_FILE, engine="openpyxl")
        if "AccountId" in df_base.columns:
            done_ids = set(df_base["AccountId"].dropna().astype(int).tolist())
            before = len(df_input)
            df_input = df_input[~df_input["AccountId"].isin(done_ids)]
            logger.info(f"Resume: skipping {before - len(df_input)} already-done accounts, "
                        f"{len(df_input)} remaining")

    if df_input.empty:
        logger.info("Nothing to process. Exiting.")
        return

    # ── build per-account metadata (AccountId, AccountName, domains, context) ──
    # Key: CoreBrandKeyword — but the SAME keyword can belong to multiple accounts
    # so we track a list of account meta per keyword.
    rows = df_input.to_dict("records")

    # We'll process each account individually to keep domain context accurate.
    # Build (theme_key, brand_context, account_meta) list.
    # theme_key uniquely identifies a theme within this run.
    tasks = []  # list of dict
    for rec in rows:
        account_id   = rec.get("AccountId")
        account_name = rec.get("AccountName", "")
        brand_kw     = str(rec.get("CoreBrandKeyword", "")).strip()
        domains      = parse_list_field(rec.get("Domains", ""))
        brand_ctx    = domains_to_context(domains)

        if not brand_kw:
            logger.warning(f"AccountId={account_id} has empty CoreBrandKeyword — skipping")
            continue

        tasks.append({
            "account_id":   account_id,
            "account_name": account_name,
            "brand_kw":     brand_kw,
            "brand_ctx":    brand_ctx,
            "domains":      ", ".join(domains),
        })

    logger.info(f"Tasks to process: {len(tasks)}")

    # ── initialise shared components ───────────────────────────────────────────
    logger.info("Initialising DBManager …")
    db_manager = DBManager(market=MARKET, use_azure=config.USE_AZURE_DATASOURCE)
    db_manager.initialize_db()

    logger.info("Initialising AIExpander …")
    expander = AIExpander()

    logger.info("Initialising QueryMatcher …")
    matcher = QueryMatcher(db_manager)

    # ── process in chunks so we can write partial results and resume ───────────
    CHUNK = 50          # write output every CHUNK accounts
    all_result_rows = []

    for chunk_start in range(0, len(tasks), CHUNK):
        chunk = tasks[chunk_start: chunk_start + CHUNK]
        logger.info(f"Processing chunk {chunk_start // CHUNK + 1} "
                    f"({chunk_start + 1}–{chunk_start + len(chunk)} / {len(tasks)})")

        themes        = [t["brand_kw"]  for t in chunk]
        brand_contexts = {t["brand_kw"]: t["brand_ctx"] for t in chunk}

        # Map theme → account metadata (handle duplicate keywords within chunk)
        theme_to_meta = {}
        for t in chunk:
            theme_to_meta.setdefault(t["brand_kw"], []).append(t)

        job_id = uuid.uuid4().hex[:8]
        results = process_themes_parallel(
            themes,
            expander,
            matcher,
            market=MARKET,
            max_workers=args.workers,
            job_id=job_id,
            brand_contexts=brand_contexts,
        )

        # ── normal expansion results → rows ───────────────────────────────────
        # Also track queries found per brand for phonetic dedup
        normal_queries_by_brand: dict[str, set] = {}

        for theme, df_result, expanded_kws in results:
            metas = theme_to_meta.get(theme, [])
            if df_result.empty:
                # Still record the account with zero results so it's marked done
                for meta in metas:
                    all_result_rows.append({
                        "AccountId":        meta["account_id"],
                        "AccountName":      meta["account_name"],
                        "CoreBrandKeyword": meta["brand_kw"],
                        "Domains":          meta["domains"],
                        "KatakanaPhonetic": None,
                        "normalized_query": None,
                        "Relevance":        None,
                        "SRPV":             None,
                        "AdClick":          None,
                        "revenue":          None,
                        "Score":            None,
                        "matched_keyword":  None,
                        "AI_Valid":         None,
                        "AI_Reason":        None,
                        "ExpandedKeywords": "; ".join(expanded_kws),
                    })
            else:
                valid_df = df_result[df_result.get("AI_Valid", True) == True] if "AI_Valid" in df_result.columns else df_result
                found_qs = set(valid_df["normalized_query"].dropna().tolist())
                normal_queries_by_brand[theme] = found_qs
                for meta in metas:
                    for _, row in valid_df.iterrows():
                        all_result_rows.append({
                            "AccountId":        meta["account_id"],
                            "AccountName":      meta["account_name"],
                            "CoreBrandKeyword": meta["brand_kw"],
                            "Domains":          meta["domains"],
                            "KatakanaPhonetic": None,
                            "normalized_query": row.get("normalized_query"),
                            "Relevance":        row.get("Relevance"),
                            "SRPV":             row.get("SRPV"),
                            "AdClick":          row.get("AdClick"),
                            "revenue":          row.get("revenue"),
                            "Score":            row.get("Score"),
                            "matched_keyword":  row.get("matched_keyword"),
                            "AI_Valid":         row.get("AI_Valid"),
                            "AI_Reason":        row.get("AI_Reason"),
                            "ExpandedKeywords": "; ".join(expanded_kws),
                        })

        # ── phonetic expansion: supplement non-Japanese brands ─────────────────
        non_jp_brands_in_chunk = [
            brand for brand in theme_to_meta
            if not is_japanese(brand)
        ]

        if non_jp_brands_in_chunk:
            logger.info(f"[phonetic] Running phonetic expansion for "
                        f"{len(non_jp_brands_in_chunk)} non-Japanese brands in chunk")
            phonetic_job_id = uuid.uuid4().hex[:8]
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ph_executor:
                ph_futures = {
                    ph_executor.submit(
                        _run_phonetic_for_brand, brand, expander, matcher,
                        phonetic_job_id, normal_queries_by_brand.get(brand, set())
                    ): brand
                    for brand in non_jp_brands_in_chunk
                }
                for future in concurrent.futures.as_completed(ph_futures):
                    brand = ph_futures[future]
                    try:
                        _, katakana, ph_rows = future.result()
                        metas = theme_to_meta.get(brand, [])
                        for ph_row in ph_rows:
                            for meta in metas:
                                all_result_rows.append({
                                    "AccountId":        meta["account_id"],
                                    "AccountName":      meta["account_name"],
                                    "CoreBrandKeyword": brand,
                                    "Domains":          meta["domains"],
                                    "KatakanaPhonetic": katakana,
                                    "normalized_query": ph_row["normalized_query"],
                                    "Relevance":        ph_row["Relevance"],
                                    "SRPV":             ph_row["SRPV"],
                                    "AdClick":          ph_row["AdClick"],
                                    "revenue":          ph_row["revenue"],
                                    "Score":            ph_row["Score"],
                                    "matched_keyword":  ph_row["matched_keyword"],
                                    "AI_Valid":         ph_row["AI_Valid"],
                                    "AI_Reason":        ph_row["AI_Reason"],
                                    "ExpandedKeywords": None,
                                })
                    except Exception as exc:
                        logger.error(f"[phonetic] Brand '{brand}' exception: {exc}")

        # Write incremental results after each chunk (base data + new rows, no duplication)
        _write_output(all_result_rows, OUTPUT_FILE, df_base=df_base)
        logger.info(f"Checkpoint saved → {OUTPUT_FILE} ({len(df_base) + len(all_result_rows)} rows so far)")

    logger.info(f"Done. Total result rows: {len(df_base) + len(all_result_rows)}")
    logger.info(f"Output: {OUTPUT_FILE}")


def _write_output(rows: list, path: str, df_base: pd.DataFrame = None):
    """Write the output Excel file: base (already-done) data + new rows."""
    df_new = pd.DataFrame(rows)
    if df_base is not None and not df_base.empty:
        df_out = pd.concat([df_base, df_new], ignore_index=True)
    else:
        df_out = df_new
    df_out.to_excel(path, index=False, engine="openpyxl")


if __name__ == "__main__":
    main()
