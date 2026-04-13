import os
from openai import AzureOpenAI
import json
import config
import concurrent.futures
from logger import get_logger
import job_store

logger = get_logger(__name__)


class AIExpander:
    def __init__(self):
        self.deployment_name = config.AZURE_OPENAI_DEPLOYMENT_NAME

    def _get_client(self) -> AzureOpenAI:
        """Return a new AzureOpenAI client per call.

        The openai SDK's httpx connection pool is not safe to share across threads.
        Creating a lightweight client per request avoids cross-thread deadlocks.
        """
        return AzureOpenAI(
            api_key=config.AZURE_OPENAI_API_KEY,
            api_version=config.AZURE_OPENAI_API_VERSION,
            azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
        )

    def _expand_japanese_space_variants(self, keywords: list) -> list:
        """
        For Japanese market, generate both half-width and full-width space versions.
        This allows matching queries with either space type in the database.

        Half-width space: ' ' (U+0020)
        Full-width space: '\\u3000' or '　'
        """
        expanded = set()
        for kw in keywords:
            if not isinstance(kw, str):
                continue
            kw = kw.strip()
            if not kw:
                continue

            expanded.add(kw)

            if ' ' in kw:
                expanded.add(kw.replace(' ', '\u3000'))

            if '\u3000' in kw:
                expanded.add(kw.replace('\u3000', ' '))

        return list(expanded)

    def _get_language_instruction(self, market):
        """Get language-specific instruction based on market."""
        languages = config.MARKET_LANGUAGES.get(market, ["English"])

        if len(languages) == 1 and languages[0] == "English":
            return "Generate keywords in English only."

        lang_list = ", ".join(languages)
        primary_lang = languages[0]

        language_examples = {
            "Japanese":   'e.g., "ナイキ シューズ", "アップル アイフォン"',
            "Hindi":      'e.g., "नाइकी जूते", "एप्पल आईफोन"',
            "Chinese":    'e.g., "耐克鞋", "苹果手机"',
            "Malay":      'e.g., "kasut Nike", "telefon Apple"',
            "Thai":       'e.g., "รองเท้า Nike", "โทรศัพท์ Apple"',
            "Filipino":   'e.g., "sapatos Nike", "telepono Apple"',
            "Indonesian": 'e.g., "sepatu Nike", "telepon Apple"',
            "Vietnamese": 'e.g., "giày Nike", "điện thoại Apple"',
        }

        example_str = language_examples.get(primary_lang, "")

        return f"""Generate keywords in multiple languages for the {market} market.
        Target languages: {lang_list}
        Primary language: {primary_lang}

        IMPORTANT Language Rules:
        - Include keywords in {primary_lang} as the primary language
        - Also include English keywords for international brand recognition
        - For each concept, consider providing variations in different target languages
        - Local language keywords should reflect how local users actually search
        {f'- Examples in {primary_lang}: {example_str}' if example_str else ''}
        """

    def expand_search_themes_parallel(
        self,
        brands: list,
        market: str = "Australia",
        max_workers: int = 10,
        job_id=None,
        theme_id=None,
    ) -> list:
        """Expand multiple brands concurrently and merge results into a single seed list.

        Each brand is submitted to expand_search_theme in parallel. Results are
        flattened and deduplicated (order-preserving) before being returned.
        """
        ctx = {"job_id": job_id, "theme_id": theme_id}
        logger.info(
            f"Multi-brand expansion start | brands={brands} market={market}",
            extra=ctx,
        )

        all_keywords = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_brand = {
                executor.submit(
                    self.expand_search_theme, brand, market, job_id, theme_id
                ): brand
                for brand in brands
            }
            for future in concurrent.futures.as_completed(future_to_brand):
                brand = future_to_brand[future]
                try:
                    keywords = future.result()
                    all_keywords.extend(keywords)
                except Exception as exc:
                    logger.error(
                        f"Multi-brand expansion failed for brand='{brand}': {exc}",
                        extra=ctx,
                    )

        # Deduplicate while preserving insertion order
        seen = {}
        for kw in all_keywords:
            if kw not in seen:
                seen[kw] = None
        merged = list(seen.keys())

        logger.info(
            f"Multi-brand expansion done | brands={len(brands)} unique_keywords={len(merged)}",
            extra=ctx,
        )
        return merged

    def expand_search_theme(self, search_theme, market="Australia", job_id=None, theme_id=None):
        """Expands a search theme into a list of related keywords.
        Returns a list of strings.
        """
        ctx = {"job_id": job_id, "theme_id": theme_id}
        language_instruction = self._get_language_instruction(market)

        system_prompt = f"""
        You are an expert in digital marketing and keyword research.
        Your task is to analyze a user-provided "search theme" for the {market} market.

        {language_instruction}

        1. Determine if the search theme is a "Brand" or a specific "Product".
        2. If it is a **Brand**: Expand it to include the brand name itself and its key product lines or sub-brands.
        3. If it is a **Product**: Expand it to include related product terms, synonyms, or variations.

        Constraints:
        - Generate a maximum of 15 expanded keywords (to accommodate multi-language variations).
        - Keywords must be tightly related.
        - Prefer keywords with shorter roots (broad match modifiers).
        - **Multi-language Support**: Include keywords in the target market's primary language(s) AND English.
        - **Ambiguous Brand Names**: If the brand name is a common daily word (e.g., "Compare", "Market", "Ink"), the expanded keywords MUST include the brand name to preserve the specific brand meaning. Do not generate generic terms that lose the brand context.
        - **No Upward Expansion**: Do not expand a specific product to its parent category or suite if the parent includes other distinct products. (e.g., Input "Gmail" -> Output "Google Workspace" is INVALID because Workspace includes Drive, Docs, etc.). Keep the expansion focused on the specific input scope.
        - If a product term has a clear independent meaning (e.g., "iPhone" for "Apple"), the expanded keyword does NOT need to include the original brand name, UNLESS it violates the ambiguous brand name rule.

        **IMPORTANT - Validation Rules for Generated Keywords (must follow to ensure keywords pass validation check):**
        - **Brand Identity**: Each keyword MUST represent the target Brand. Valid forms include: exact brand name, close variations, plural forms, typos, URL/domain variations, commonly accepted aliases, or brand name in local language scripts.
        - **No Competitor Brands**: Do NOT generate keywords that refer to a different specific brand, even if it contains the target brand word.
        - **No Comparisons**: Do NOT generate comparison keywords (e.g., "Brand A vs Brand B").
        - **No Unrelated Entities**: Do NOT combine the Brand with terms that create unrelated entities. (e.g., If Brand is "AAMI" insurance, do NOT generate "AAMI Park" which is a stadium).
        - **Parent Brand Exception**: Combining with parent company is OK (e.g., "Google Gmail" for brand "Gmail" is valid).
        - **Generic Word Caution**: If the brand name is a common word, ensure generated keywords clearly reference the brand, not the generic concept (e.g., for brand "Apple", do NOT generate "apple fruit" or "apple pie").

        - Output ONLY a JSON array of strings. No other text.

        Example Input (Australia market): "Nike"
        Example Output: ["nike", "nike shoes", "nike air max", "nike running", "nike jordan", "nike store"]

        Example Input (Japan market): "Nike"
        Example Output: ["nike", "ナイキ", "ナイキ シューズ", "nike shoes", "ナイキ エアマックス", "nike air max", "ナイキ ランニング"]

        Example Input (Vietnam market): "Nike"
        Example Output: ["nike", "giày nike", "nike shoes", "nike running", "giày chạy bộ nike", "cửa hàng nike"]
        """

        full_prompt = f"{system_prompt}\n\nUser Input: {search_theme}"
        logger.info(f"Expansion request | theme='{search_theme}' market={market} prompt_len={len(full_prompt)}", extra=ctx)

        try:
            response = self._get_client().chat.completions.create(
                model=self.deployment_name,
                messages=[{"role": "user", "content": full_prompt}],
                max_completion_tokens=config.MAX_COMPLETION_TOKENS_EXPAND
            )

            if not response.choices:
                logger.error("Expansion failed: no choices in response", extra=ctx)
                return [search_theme]

            choice = response.choices[0]
            content = choice.message.content
            logger.debug(f"Expansion finish_reason={choice.finish_reason}", extra=ctx)

            if not content:
                logger.error(f"Expansion failed: empty content, finish_reason={choice.finish_reason}", extra=ctx)
                return [search_theme]

            content = content.strip()
            logger.debug(f"Expansion raw response:\n{content}", extra=ctx)

            # Clean up markdown code blocks if present
            if "```json" in content:
                content = content.replace("```json", "").replace("```", "")
            elif "```" in content:
                content = content.replace("```", "")
            content = content.strip()

            start_index = content.find('[')
            end_index = content.rfind(']')

            if start_index != -1 and end_index != -1 and end_index > start_index:
                json_str = content[start_index:end_index + 1]
                try:
                    expanded_keywords = json.loads(json_str)
                    if isinstance(expanded_keywords, list) and len(expanded_keywords) > 0:
                        if market == "Japan":
                            expanded_keywords = self._expand_japanese_space_variants(expanded_keywords)
                            logger.debug(f"Japanese space variants expanded to {len(expanded_keywords)} keywords", extra=ctx)
                        logger.info(f"Expansion success | {len(expanded_keywords)} keywords: {expanded_keywords}", extra=ctx)
                        return expanded_keywords
                except json.JSONDecodeError as je:
                    logger.warning(f"JSON decode error in expansion: {je}", extra=ctx)

            # Fallback
            try:
                expanded_keywords = json.loads(content)
                if isinstance(expanded_keywords, list) and len(expanded_keywords) > 0:
                    if market == "Japan":
                        expanded_keywords = self._expand_japanese_space_variants(expanded_keywords)
                    logger.info(f"Expansion success (fallback parse) | {len(expanded_keywords)} keywords: {expanded_keywords}", extra=ctx)
                    return expanded_keywords
            except json.JSONDecodeError:
                pass

            logger.error(f"Expansion failed: no JSON array found in response:\n{content}", extra=ctx)
            return [search_theme]

        except Exception as e:
            logger.exception(f"Expansion exception: {e}", extra=ctx)
            return [search_theme]

    def _validate_batch(self, brand, batch_queries, batch_index, market="Australia", job_id=None, theme_id=None):
        """Validate a single batch of queries against the brand."""
        ctx = {"job_id": job_id, "theme_id": theme_id}
        languages = config.MARKET_LANGUAGES.get(market, ["English"])
        lang_list = ", ".join(languages)

        system_prompt = f"""
            You are a query validation assistant for the {market} market.
            Target languages: {lang_list}

            Your task is to validate if a list of user search queries are relevant to a specific Brand.
            The queries may be in any of the target languages ({lang_list}).

            Validation Rules:
            1. **Brand Identity**: The query MUST represent the target Brand. It is VALID if it is the exact brand name, a close variation, plural form (e.g. "Booking" -> "Bookings"), typo, **URL/Domain variation** (e.g. "www.bookings.com", "booking.com au"), a **commonly accepted alias/expression** for the brand, or the brand name in a **local language script** (e.g. "ナイキ" for Nike in Japanese, "耐克" for Nike in Chinese). It is **INVALID** if the query refers to a **different specific brand**, even if it contains the target brand word.
            2. **Multi-language Support**: Brand names in local scripts (Japanese, Chinese, Thai, Vietnamese, Hindi, etc.) that represent the same brand are VALID. For example, "ナイキ" (Nike in Japanese), "アップル" (Apple in Japanese), "蘋果" (Apple in Chinese) are valid for their respective brands.
            3. **Parent Brand / Ownership**: It is **VALID** if the query combines the target Brand with its parent company or owner (e.g. "Google Gmail" for brand "Gmail", "Microsoft Copilot" for brand "Copilot"). This is an exception to the "other brand" rule.
            4. **Not a Comparison**: The query must not be comparing the Brand with another brand. (e.g. if Brand is "A", "A vs B" is invalid if B is another brand). If removing the Brand from the query leaves another Brand name (that is NOT the parent/owner), it is invalid.
            5. **Unrelated Specific Intent**: If the query combines the Brand with a term that creates a specific entity, location, or concept unrelated to the Brand's core business, it is **INVALID**. (e.g. If Brand is "AAMI" (insurance), "AAMI Park" is INVALID because it refers to a stadium, not the insurance service. If Brand is "Delta" (airline), "Delta Faucet" is INVALID).
            6. **Generic Brand Ambiguity**: If the Brand name is a common word (e.g. "Apple", "Orange", "Gap", "Booking"), the query is VALID if it is the brand name itself, its plural, or a clear reference to the brand's service. It is INVALID only if it clearly refers to a completely unrelated common object or concept (e.g. "apple pie" for brand "Apple"). For example, if Brand is "Booking", "bookings" is VALID.

            For each query in the list, determine if it is "Valid" or "Invalid".

            Output ONLY a JSON object where keys are the queries and values are objects with "is_valid" (boolean) and "reason" (string).
            Example:
            {{
                "query1": {{"is_valid": true, "reason": "Relevant brand query"}},
                "query2": {{"is_valid": false, "reason": "Comparison with Brand X"}},
                "ナイキ シューズ": {{"is_valid": true, "reason": "Brand name in Japanese with product term"}}
            }}
            """

        user_prompt = f"Brand: {brand}\n\nInput Queries:\n{json.dumps(batch_queries)}"
        batch_results = {}

        batch_id = f"{theme_id}-b{batch_index}" if theme_id else None
        if batch_id:
            job_store.create_validation_batch(
                batch_id=batch_id,
                theme_id=theme_id,
                job_id=job_id,
                batch_index=batch_index,
                queries=batch_queries,
            )

        logger.debug(f"Validation batch {batch_index} start | brand='{brand}' queries={len(batch_queries)}", extra=ctx)

        try:
            response = self._get_client().chat.completions.create(
                model=self.deployment_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_completion_tokens=config.MAX_COMPLETION_TOKENS_VALIDATE
            )

            if not response.choices:
                raise Exception("No choices in response")

            choice = response.choices[0]
            content = choice.message.content

            if not content:
                logger.warning(f"Validation batch {batch_index}: empty content, finish_reason={choice.finish_reason}", extra=ctx)
                for q in batch_queries:
                    batch_results[q] = {"is_valid": True, "reason": f"Validation failed (Empty Response: {choice.finish_reason})"}
                return batch_results

            content = content.strip()

            start_index = content.find('{')
            end_index = content.rfind('}')

            if start_index != -1 and end_index != -1 and end_index > start_index:
                json_str = content[start_index:end_index + 1]
                batch_results = json.loads(json_str)
                valid_count   = sum(1 for v in batch_results.values() if v.get("is_valid"))
                invalid_count = len(batch_results) - valid_count
                logger.debug(
                    f"Validation batch {batch_index} done | valid={valid_count} invalid={invalid_count}",
                    extra=ctx
                )
            else:
                logger.warning(f"Validation batch {batch_index}: no JSON object found. Raw:\n{content}", extra=ctx)
                for q in batch_queries:
                    batch_results[q] = {"is_valid": True, "reason": "Validation failed (Parse Error)"}

        except Exception as e:
            logger.exception(f"Validation batch {batch_index} exception: {e}", extra=ctx)
            for q in batch_queries:
                batch_results[q] = {"is_valid": True, "reason": "Validation failed (API Error)"}

        if batch_id:
            job_store.finish_validation_batch(batch_id=batch_id, results=batch_results)

        return batch_results

    def validate_queries(self, brand, queries, market="Australia", job_id=None, theme_id=None):
        """
        Validate all queries in parallel batches.
        Returns dict: query -> {"is_valid": bool, "reason": str}
        """
        ctx = {"job_id": job_id, "theme_id": theme_id}
        if not queries:
            return {}

        batch_size = 25
        batches = [(queries[i:i + batch_size], i // batch_size)
                   for i in range(0, len(queries), batch_size)]

        logger.info(
            f"Validation start | brand='{brand}' total_queries={len(queries)} batches={len(batches)}",
            extra=ctx
        )

        all_results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            future_to_batch = {
                executor.submit(
                    self._validate_batch, brand, batch, idx, market, job_id, theme_id
                ): idx
                for batch, idx in batches
            }
            for future in concurrent.futures.as_completed(future_to_batch):
                try:
                    all_results.update(future.result())
                except Exception as exc:
                    logger.error(f"Validation batch future exception: {exc}", extra=ctx)

        valid_count   = sum(1 for v in all_results.values() if v.get("is_valid"))
        invalid_count = len(all_results) - valid_count
        logger.info(
            f"Validation complete | total={len(all_results)} valid={valid_count} invalid={invalid_count}",
            extra=ctx
        )
        return all_results
