import argparse
import os
import uuid
import pandas as pd
import concurrent.futures
from db_manager import DBManager
from ai_expander import AIExpander
from matcher import QueryMatcher
from logger import get_logger
import config
import job_store

logger = get_logger(__name__)


def process_theme(theme, expander, matcher, market="Australia", job_id=None, theme_id=None):
    """
    Full pipeline for a single theme: Expand → Match → Validate.
    job_id  : ID of the parent expansion job (shared across all themes in the request)
    theme_id: ID unique to this theme within the job  (format: "<job_id>-t<n>")
    """
    if job_id is None:
        job_id = uuid.uuid4().hex[:8]
    if theme_id is None:
        theme_id = f"{job_id}-t0"

    ctx = {"job_id": job_id, "theme_id": theme_id}
    logger.info(f"Theme start | theme='{theme}' market={market}", extra=ctx)

    job_store.create_theme_task(theme_id=theme_id, job_id=job_id, theme=theme, market=market)

    try:
        # Step 1: AI Expansion
        logger.info("Step 1/3 Expansion start", extra=ctx)
        expanded_keywords = expander.expand_search_theme(theme, market=market, job_id=job_id, theme_id=theme_id)
        job_store.update_theme_expanded(theme_id=theme_id, expanded_keywords=expanded_keywords)

        # Step 2: Matching
        logger.info(f"Step 2/3 Matching start | keywords={len(expanded_keywords)}", extra=ctx)
        results_df = matcher.process_expanded_keywords(expanded_keywords, job_id=job_id, theme_id=theme_id)
        logger.info(f"Step 2/3 Matching done | matches={len(results_df)}", extra=ctx)

        matched_queries = results_df['normalized_query'].tolist() if not results_df.empty else []
        job_store.update_theme_matched(theme_id=theme_id, matched_queries=matched_queries)

        # Step 3: AI Validation
        validated_queries = []
        final_queries = []
        valid_count = 0
        invalid_count = 0

        if not results_df.empty:
            queries_to_validate = results_df['normalized_query'].unique().tolist()
            logger.info(f"Step 3/3 Validation start | unique_queries={len(queries_to_validate)}", extra=ctx)
            validation_results = expander.validate_queries(
                theme, queries_to_validate, market=market, job_id=job_id, theme_id=theme_id
            )

            results_df['AI_Valid'] = results_df['normalized_query'].map(
                lambda q: validation_results.get(q, {}).get('is_valid', True)
            )
            results_df['AI_Reason'] = results_df['normalized_query'].map(
                lambda q: validation_results.get(q, {}).get('reason', '')
            )

            valid_mask = results_df['AI_Valid'] == True
            validated_queries = results_df.loc[valid_mask, 'normalized_query'].tolist()
            final_queries = validated_queries
            valid_count = len(validated_queries)
            invalid_count = len(results_df) - valid_count
        else:
            logger.info("Step 3/3 Validation skipped (no matches)", extra=ctx)

        logger.info(
            f"Theme done | matches={len(results_df)} valid={valid_count} invalid={invalid_count}",
            extra=ctx
        )
        job_store.finish_theme_task(
            theme_id=theme_id,
            validated_queries=validated_queries,
            final_queries=final_queries,
            valid_count=valid_count,
            invalid_count=invalid_count,
            status="done",
        )

    except Exception as exc:
        logger.exception(f"Theme '{theme}' failed: {exc}", extra=ctx)
        job_store.finish_theme_task(
            theme_id=theme_id,
            validated_queries=[],
            final_queries=[],
            valid_count=0,
            invalid_count=0,
            status="error",
            error_msg=str(exc),
        )
        raise

    return results_df, expanded_keywords


def process_themes_parallel(themes, expander, matcher, market="Australia", max_workers=3, job_id=None):
    """
    Process multiple themes concurrently using a thread pool.
    Each theme runs its full pipeline (Expand → Match → Validate) in parallel.

    max_workers=3 is a conservative default: each theme internally spawns up to 10
    validation threads, so 3 themes × 10 = 30 concurrent Azure OpenAI calls at peak.
    Raise max_workers if your Azure quota allows more RPM.

    Returns: list of (theme, df, expanded_keywords) in the SAME ORDER as input themes.
    """
    if job_id is None:
        job_id = uuid.uuid4().hex[:8]

    ctx = {"job_id": job_id}
    logger.info(f"Job start | themes={len(themes)} market={market} max_workers={max_workers}", extra=ctx)

    job_store.create_job(job_id=job_id, market=market, themes=themes)

    results_map = {}
    final_status = "done"

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_theme = {
            executor.submit(
                process_theme, theme, expander, matcher, market,
                job_id, f"{job_id}-t{i}"
            ): theme
            for i, theme in enumerate(themes)
        }
        for future in concurrent.futures.as_completed(future_to_theme):
            theme = future_to_theme[future]
            try:
                df, expanded_keywords = future.result()
                results_map[theme] = (df, expanded_keywords)
            except Exception as e:
                logger.error(f"Theme '{theme}' raised exception: {e}", extra=ctx)
                final_status = "error"
                empty_df = pd.DataFrame(columns=[
                    'normalized_query', 'Relevance', 'SRPV', 'AdClick',
                    'revenue', 'Score', 'matched_keyword'
                ])
                results_map[theme] = (empty_df, [f"Error: {e}"])

    job_store.finish_job(job_id=job_id, status=final_status)
    logger.info(f"Job complete | status={final_status}", extra=ctx)
    # Preserve input order
    return [(theme, *results_map[theme]) for theme in themes]


def main():
    parser = argparse.ArgumentParser(description="Brand Ad Keyword Expansion Tool")
    parser.add_argument("--theme", type=str, help="Single search theme to process")
    parser.add_argument("--file",  type=str, help="File containing list of search themes (one per line)")
    parser.add_argument("--output", type=str, default="expansion_results.csv", help="Output CSV file path")
    parser.add_argument("--market", type=str, default=config.DEFAULT_MARKET,
                        choices=config.AVAILABLE_MARKETS,
                        help=f"Target market. Available: {', '.join(config.AVAILABLE_MARKETS)}")

    args = parser.parse_args()

    job_id = uuid.uuid4().hex[:8]
    ctx = {"job_id": job_id}

    logger.info(f"CLI job start | market={args.market}", extra=ctx)

    logger.info("Initializing Database Manager...", extra=ctx)
    db_manager = DBManager()
    db_manager.initialize_db()

    logger.info("Initializing AI Expander...", extra=ctx)
    expander = AIExpander()

    logger.info("Initializing Matcher...", extra=ctx)
    matcher = QueryMatcher(db_manager)

    all_results = []

    if args.theme:
        df, _ = process_theme(args.theme, expander, matcher, market=args.market,
                               job_id=job_id, theme_id=f"{job_id}-t0")
        df['SearchTheme'] = args.theme
        df['Market'] = args.market
        all_results.append(df)

    elif args.file:
        if not os.path.exists(args.file):
            logger.error(f"Input file not found: {args.file}", extra=ctx)
            return

        with open(args.file, 'r') as f:
            themes = [line.strip() for line in f if line.strip()]

        logger.info(f"Processing {len(themes)} themes in parallel (max_workers=3)...", extra=ctx)
        for theme, df, _ in process_themes_parallel(
            themes, expander, matcher, market=args.market, job_id=job_id
        ):
            df['SearchTheme'] = theme
            df['Market'] = args.market
            all_results.append(df)
    else:
        logger.error("No --theme or --file argument provided.", extra=ctx)
        return

    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        cols = ['SearchTheme'] + [c for c in final_df.columns if c != 'SearchTheme']
        final_df = final_df[cols]
        final_df.to_csv(args.output, index=False)
        logger.info(f"Results saved to {args.output}", extra=ctx)
    else:
        logger.warning("No results generated.", extra=ctx)


if __name__ == "__main__":
    main()
