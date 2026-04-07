import pandas as pd
import Levenshtein
from db_manager import DBManager
from logger import get_logger

logger = get_logger(__name__)


class QueryMatcher:
    def __init__(self, db_manager: DBManager):
        self.db = db_manager

    def calculate_relevance_hard(self, query, keyword):
        """
        Relevance = len(keyword) / max(len(query), len(keyword))
        Uses space-stripped versions for consistency with matching logic.
        Removes both half-width (U+0020) and full-width (U+3000) spaces.
        """
        q_clean = query.replace(" ", "").replace("\u3000", "")
        k_clean = keyword.replace(" ", "").replace("\u3000", "")

        if len(q_clean) == 0 or len(k_clean) == 0:
            return 0.0

        return len(k_clean) / max(len(q_clean), len(k_clean))

    def process_expanded_keywords(self, expanded_keywords, job_id=None, theme_id=None):
        """
        Process a list of expanded keywords and return a combined DataFrame of results.
        """
        ctx = {"job_id": job_id, "theme_id": theme_id}
        all_results = {}

        for keyword in expanded_keywords:
            clean_keyword = keyword.replace(" ", "").replace("\u3000", "")
            logger.debug(f"Matching keyword='{keyword}'", extra=ctx)

            # --- Method A: Hard Match (Score = 2) ---
            df_hard = self.db.query_sqlite_contains(keyword)
            hard_count = 0

            for _, row in df_hard.iterrows():
                q = row['normalized_query']
                relevance = self.calculate_relevance_hard(q, keyword)

                if q not in all_results:
                    all_results[q] = {
                        'normalized_query': q,
                        'SRPV':             row['SRPV'],
                        'AdClick':          row['AdClick'],
                        'revenue':          row['revenue'],
                        'score_hard':       0,
                        'score_vector':     0,
                        'relevance_accum':  0,
                        'match_count':      0,
                        'matched_keyword':  keyword,
                    }
                else:
                    if len(keyword) < len(all_results[q]['matched_keyword']):
                        all_results[q]['matched_keyword'] = keyword

                all_results[q]['score_hard'] = 2
                all_results[q]['relevance_accum'] = max(all_results[q]['relevance_accum'], relevance)
                hard_count += 1

            # --- Method B: Vector Match (Score = 1) ---
            df_vector = self.db.query_vector_similarity(keyword, n_results=100)
            vector_count = 0

            for _, row in df_vector.iterrows():
                q = row['normalized_query']
                distance   = row['distance']
                similarity = 1 - distance

                if similarity < 0.8:
                    continue

                clean_query  = q.replace(" ", "").replace("\u3000", "")
                is_contained = clean_keyword in clean_query

                edit_dist    = Levenshtein.distance(clean_query, clean_keyword)
                threshold    = len(clean_keyword) / 5.0
                is_typo_match = edit_dist < threshold

                if is_contained or is_typo_match:
                    if q not in all_results:
                        all_results[q] = {
                            'normalized_query': q,
                            'SRPV':             row['SRPV'],
                            'AdClick':          row['AdClick'],
                            'revenue':          row['revenue'],
                            'score_hard':       0,
                            'score_vector':     0,
                            'relevance_accum':  0,
                            'match_count':      0,
                            'matched_keyword':  keyword,
                        }
                    else:
                        if len(keyword) < len(all_results[q]['matched_keyword']):
                            all_results[q]['matched_keyword'] = keyword

                    all_results[q]['score_vector'] = 1
                    all_results[q]['relevance_accum'] = max(all_results[q]['relevance_accum'], similarity)
                    vector_count += 1

            logger.debug(
                f"Keyword='{keyword}' | hard={hard_count} vector={vector_count}",
                extra=ctx
            )

        # Build DataFrame
        results_list = []
        for q, data in all_results.items():
            data['Score'] = data['score_hard'] + data['score_vector']
            results_list.append(data)

        if not results_list:
            logger.info("No matches found for any keyword", extra=ctx)
            return pd.DataFrame(columns=[
                'normalized_query', 'Relevance', 'SRPV', 'AdClick',
                'revenue', 'Score', 'matched_keyword'
            ])

        final_df = pd.DataFrame(results_list)
        final_df.rename(columns={'relevance_accum': 'Relevance'}, inplace=True)

        before = len(final_df)
        final_df = final_df[final_df['Relevance'] >= 0.4]
        after  = len(final_df)

        if before != after:
            logger.debug(f"Relevance filter removed {before - after} low-relevance rows (<0.4)", extra=ctx)

        final_df.sort_values(by='Score', ascending=False, inplace=True)

        score_dist = final_df['Score'].value_counts().to_dict()
        logger.info(f"Matching done | total={after} score_dist={score_dist}", extra=ctx)

        return final_df[['normalized_query', 'Relevance', 'SRPV', 'AdClick', 'revenue', 'Score', 'matched_keyword']]
