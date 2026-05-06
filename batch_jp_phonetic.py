"""
Batch Japanese katakana phonetic expansion for Japan market.

For each unique non-Japanese CoreBrandKeyword, generates its katakana phonetic
reading via AI, runs matcher using that phonetic as the search keyword, validates
results with AI (noting that the keyword is a phonetic for the brand), and writes
all matches to an output xlsx.

Reads:  Data/TestData/bza_jp_whitelist_account_with_brand.xlsx
Output: Data/TestData/bza_jp_phonetic_results.xlsx

Usage:
    python batch_jp_phonetic.py [--workers N] [--limit N] [--resume]

Options:
    --workers N   Parallel brand workers (default 5).
    --limit N     Only process the first N unique brands (0 = all).
    --resume      Skip brands whose CoreBrandKeyword already appears in the output.
"""

import argparse
import ast
import os
import re
import uuid
import concurrent.futures

import pandas as pd

from db_manager import DBManager
from ai_expander import AIExpander
from matcher import QueryMatcher
from logger import get_logger
import config

logger = get_logger(__name__)

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE  = os.path.join(BASE_DIR, "Data", "TestData",
                           "bza_jp_whitelist_account_with_brand.xlsx")
OUTPUT_FILE = os.path.join(BASE_DIR, "Data", "TestData",
                           "bza_jp_phonetic_all_results.xlsx")

MARKET = "Japan"

# Unicode ranges: hiragana, katakana, CJK unified ideographs + compatibility ideographs
_JP_PATTERN = re.compile(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF\uF900-\uFAFF]')


def is_japanese(text: str) -> bool:
    return bool(_JP_PATTERN.search(text))


def parse_list_field(value) -> list:
    if not value:
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = ast.literal_eval(str(value))
        return parsed if isinstance(parsed, list) else [str(parsed)]
    except Exception:
        return [str(value)]


def process_brand(brand: str, metas: list, expander: AIExpander,
                  matcher: QueryMatcher, job_id: str) -> list:
    """Process one unique brand: get phonetic → match → validate → return rows."""
    # Step 1: get katakana phonetic
    katakana = expander.get_katakana_phonetic(brand)
    if not katakana:
        logger.warning(f"Brand '{brand}': failed to get katakana phonetic, skipping")
        return _empty_rows(metas, brand, katakana="")

    logger.info(f"Brand '{brand}' → katakana='{katakana}'")

    # Step 2: match using katakana as the sole keyword
    theme_id = uuid.uuid4().hex[:8]
    df_result = matcher.process_expanded_keywords(
        [katakana],
        job_id=job_id,
        theme_id=theme_id,
        device_types=["pc"],
    )

    if df_result is None or df_result.empty:
        logger.info(f"Brand '{brand}': no matches found")
        return _empty_rows(metas, brand, katakana)

    queries = df_result["normalized_query"].dropna().tolist()
    if not queries:
        return _empty_rows(metas, brand, katakana)

    # Step 3: AI validate with phonetic context
    phonetic_ctx = (
        f"The search keyword '{katakana}' is the Japanese katakana phonetic "
        f"reading of the brand '{brand}'. Queries that phonetically match "
        f"this brand name are valid even if they don't contain the Latin spelling."
    )
    validation = expander.validate_queries(
        brand,
        queries,
        market=MARKET,
        job_id=job_id,
        theme_id=theme_id,
        brand_keywords=[katakana],
        brand_context=phonetic_ctx,
    )

    # Step 4: build output rows, mapped to all associated accounts
    result_rows = []
    for _, row in df_result.iterrows():
        q = row.get("normalized_query")
        vr = validation.get(q, {})
        is_valid = vr.get("is_valid", True)
        reason   = vr.get("reason", "")

        for meta in metas:
            result_rows.append({
                "AccountId":        meta["account_id"],
                "AccountName":      meta["account_name"],
                "CoreBrandKeyword": brand,
                "Domains":          meta["domains"],
                "KatakanaPhonetic": katakana,
                "normalized_query": q,
                "Relevance":        row.get("Relevance"),
                "SRPV":             row.get("SRPV"),
                "AdClick":          row.get("AdClick"),
                "revenue":          row.get("revenue"),
                "Score":            row.get("Score"),
                "matched_keyword":  row.get("matched_keyword"),
                "AI_Valid":         is_valid,
                "AI_Reason":        reason,
            })

    valid_count = sum(1 for r in result_rows if r.get("AI_Valid"))
    logger.info(f"Brand '{brand}': {len(df_result)} matches, {valid_count} valid after AI")
    return result_rows


def _empty_rows(metas: list, brand: str, katakana: str) -> list:
    """One placeholder row per account when there are no matches."""
    return [
        {
            "AccountId":        meta["account_id"],
            "AccountName":      meta["account_name"],
            "CoreBrandKeyword": brand,
            "Domains":          meta["domains"],
            "KatakanaPhonetic": katakana,
            "normalized_query": None,
            "Relevance":        None,
            "SRPV":             None,
            "AdClick":          None,
            "revenue":          None,
            "Score":            None,
            "matched_keyword":  None,
            "AI_Valid":         None,
            "AI_Reason":        None,
        }
        for meta in metas
    ]


def main():
    parser = argparse.ArgumentParser(description="Batch JP katakana phonetic expansion")
    parser.add_argument("--workers", type=int, default=5,
                        help="Parallel brand workers (default 5)")
    parser.add_argument("--limit", type=int, default=0,
                        help="Only process first N unique brands (0 = all)")
    parser.add_argument("--resume", action="store_true",
                        help="Skip brands already present in the output file")
    args = parser.parse_args()

    # ── load input ─────────────────────────────────────────────────────────────
    logger.info(f"Reading input: {INPUT_FILE}")
    df_input = pd.read_excel(INPUT_FILE, engine="openpyxl")
    logger.info(f"Loaded {len(df_input)} rows")

    # ── resume: load existing output ───────────────────────────────────────────
    done_brands: set = set()
    df_base = pd.DataFrame()
    if args.resume and os.path.exists(OUTPUT_FILE):
        df_base = pd.read_excel(OUTPUT_FILE, engine="openpyxl")
        if "CoreBrandKeyword" in df_base.columns:
            done_brands = set(df_base["CoreBrandKeyword"].dropna().tolist())
            logger.info(f"Resume: {len(done_brands)} brands already processed")

    # ── group by unique CoreBrandKeyword ───────────────────────────────────────
    brand_map: dict[str, list] = {}   # brand → list of account meta dicts
    for rec in df_input.to_dict("records"):
        brand = str(rec.get("CoreBrandKeyword", "")).strip()
        if not brand:
            continue
        domains = parse_list_field(rec.get("Domains", ""))
        meta = {
            "account_id":   rec.get("AccountId"),
            "account_name": rec.get("AccountName", ""),
            "domains":      ", ".join(domains),
        }
        brand_map.setdefault(brand, []).append(meta)

    unique_brands = list(brand_map.keys())
    logger.info(f"Unique brands: {len(unique_brands)}")

    # ── resume filter ──────────────────────────────────────────────────────────
    if done_brands:
        before = len(unique_brands)
        unique_brands = [b for b in unique_brands if b not in done_brands]
        logger.info(f"Resume: skipping {before - len(unique_brands)}, "
                    f"{len(unique_brands)} remaining")

    # ── limit ─────────────────────────────────────────────────────────────────
    if args.limit > 0:
        unique_brands = unique_brands[:args.limit]
        logger.info(f"Limiting to first {args.limit} brands")

    non_jp_brands = unique_brands  # process all brands regardless of script

    if not non_jp_brands:
        logger.info("Nothing to process. Exiting.")
        return

    # ── initialise shared components ───────────────────────────────────────────
    logger.info("Initialising DBManager …")
    db_manager = DBManager(market=MARKET, use_azure=config.USE_AZURE_DATASOURCE)
    db_manager.initialize_db()

    logger.info("Initialising AIExpander …")
    expander = AIExpander()

    logger.info("Initialising QueryMatcher …")
    matcher = QueryMatcher(db_manager)

    # ── process brands in parallel ────────────────────────────────────────────
    job_id = uuid.uuid4().hex[:8]
    all_result_rows = []

    logger.info(f"Processing {len(non_jp_brands)} brands with max_workers={args.workers} …")
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_brand = {
            executor.submit(process_brand, brand, brand_map[brand],
                            expander, matcher, job_id): brand
            for brand in non_jp_brands
        }
        for future in concurrent.futures.as_completed(future_to_brand):
            brand = future_to_brand[future]
            try:
                rows = future.result()
                all_result_rows.extend(rows)
                logger.info(f"Brand '{brand}' done — {len(rows)} output rows")
            except Exception as exc:
                logger.error(f"Brand '{brand}' raised exception: {exc}")

    # ── write output ──────────────────────────────────────────────────────────
    df_new = pd.DataFrame(all_result_rows)
    if not df_base.empty:
        df_out = pd.concat([df_base, df_new], ignore_index=True)
    else:
        df_out = df_new

    df_out.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")
    logger.info(f"Done. Output rows: {len(df_out)}")
    logger.info(f"Output: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
