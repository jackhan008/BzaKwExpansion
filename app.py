import os
import uuid
import json
import asyncio
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import List, Optional
from contextlib import asynccontextmanager

from db_manager import DBManager
from ai_expander import AIExpander
from matcher import QueryMatcher
from main import process_theme, process_themes_parallel
from logger import get_logger
import config
import job_store

logger = get_logger(__name__)

# Global instances
db_managers = {}   # market -> DBManager
expander    = None


def get_db_manager(market: str) -> DBManager:
    """Get or create DBManager for the specified market (cached)."""
    if market not in db_managers:
        logger.info(f"Initializing DBManager for market: {market} (azure={config.USE_AZURE_DATASOURCE})")
        db_manager = DBManager(market=market, use_azure=config.USE_AZURE_DATASOURCE)
        db_manager.initialize_db()
        db_managers[market] = db_manager
    return db_managers[market]


def get_matcher(market: str) -> QueryMatcher:
    return QueryMatcher(get_db_manager(market))


@asynccontextmanager
async def lifespan(app: FastAPI):
    global expander
    logger.info("Server starting — initializing resources...")
    job_store.init_db()
    get_db_manager(config.DEFAULT_MARKET)
    get_db_manager("China")
    expander = AIExpander()
    logger.info("Resources initialized.")
    yield
    logger.info("Server shutting down.")


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


# ---------- Request / Response models ----------

class ExpandRequest(BaseModel):
    themes: List[str]
    market: Optional[str] = "Australia"


class ThemeDetail(BaseModel):
    theme:             str
    expanded_keywords: List[str]
    match_count:       int


class ProcessResponse(BaseModel):
    job_id:  str
    details: List[ThemeDetail]
    csv_content: str


class MarketInfo(BaseModel):
    markets:          List[str]
    default_market:   str
    market_languages: dict


# ---------- Endpoints ----------

@app.get("/")
async def read_root():
    return FileResponse('static/index.html')


@app.get("/api/markets")
async def get_markets():
    return MarketInfo(
        markets=config.AVAILABLE_MARKETS,
        default_market=config.DEFAULT_MARKET,
        market_languages=config.MARKET_LANGUAGES
    )


@app.post("/api/expand_stream")
async def expand_themes_stream(request: ExpandRequest):
    if not request.themes:
        raise HTTPException(status_code=400, detail="No themes provided")

    job_id = uuid.uuid4().hex[:8]
    market = request.market or config.DEFAULT_MARKET
    themes = [t for t in request.themes if t.strip()]
    ctx    = {"job_id": job_id}

    logger.info(f"Stream job received | themes={themes} market={market}", extra=ctx)
    matcher = get_matcher(market)

    async def event_generator():
        # First line: announce job_id to the client
        yield json.dumps({"type": "job_start", "job_id": job_id}) + "\n"

        all_results_dfs = []
        loop = asyncio.get_running_loop()

        ordered_results = await loop.run_in_executor(
            None, process_themes_parallel, themes, expander, matcher, market, 3, job_id
        )

        for theme, df, expanded_keywords in ordered_results:
            match_count = len(df) if not df.empty else 0
            yield json.dumps({
                "type": "theme_result",
                "data": {
                    "theme":             theme,
                    "expanded_keywords": expanded_keywords,
                    "match_count":       match_count,
                }
            }) + "\n"

            if not df.empty:
                df['SearchTheme'] = theme
                all_results_dfs.append(df)

        # Final CSV
        if not all_results_dfs:
            df_empty = pd.DataFrame(columns=[
                'SearchTheme', 'normalized_query', 'Relevance',
                'SRPV', 'AdClick', 'revenue', 'Score'
            ])
            csv_content = df_empty.to_csv(index=False)
        else:
            final_df = pd.concat(all_results_dfs, ignore_index=True)
            cols     = ['SearchTheme'] + [c for c in final_df.columns if c != 'SearchTheme']
            final_df = final_df[cols]
            csv_content = final_df.to_csv(index=False)

        logger.info(f"Stream job complete | total_rows={sum(len(d) for d in all_results_dfs)}", extra=ctx)
        yield json.dumps({"type": "complete", "job_id": job_id, "csv_content": csv_content}) + "\n"

    return StreamingResponse(event_generator(), media_type="application/x-ndjson")


@app.post("/api/expand", response_model=ProcessResponse)
async def expand_themes(request: ExpandRequest):
    if not request.themes:
        raise HTTPException(status_code=400, detail="No themes provided")

    job_id = uuid.uuid4().hex[:8]
    market = request.market or config.DEFAULT_MARKET
    themes = [t for t in request.themes if t.strip()]
    ctx    = {"job_id": job_id}

    logger.info(f"Expand job received | themes={themes} market={market}", extra=ctx)
    matcher = get_matcher(market)

    all_results_dfs = []
    theme_details   = []

    for theme, df, expanded_keywords in process_themes_parallel(
        themes, expander, matcher, market, job_id=job_id
    ):
        match_count = len(df) if not df.empty else 0
        theme_details.append(ThemeDetail(
            theme=theme,
            expanded_keywords=expanded_keywords,
            match_count=match_count
        ))
        if not df.empty:
            df['SearchTheme'] = theme
            all_results_dfs.append(df)

    if not all_results_dfs:
        df_empty = pd.DataFrame(columns=[
            'SearchTheme', 'normalized_query', 'Relevance',
            'SRPV', 'AdClick', 'revenue', 'Score'
        ])
        csv_content = df_empty.to_csv(index=False)
    else:
        final_df = pd.concat(all_results_dfs, ignore_index=True)
        cols     = ['SearchTheme'] + [c for c in final_df.columns if c != 'SearchTheme']
        final_df = final_df[cols]
        csv_content = final_df.to_csv(index=False)

    logger.info(f"Expand job complete | total_rows={sum(len(d) for d in all_results_dfs)}", extra=ctx)
    return ProcessResponse(job_id=job_id, details=theme_details, csv_content=csv_content)


# ---------- History API ----------

@app.get("/api/jobs")
async def list_jobs(limit: int = 50):
    """List recent jobs, newest first."""
    return job_store.list_jobs(limit=limit)


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    """Return job header + all its theme tasks."""
    job = job_store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    theme_tasks = job_store.get_theme_tasks(job_id)
    return {**job, "theme_tasks": theme_tasks}


@app.get("/api/jobs/{job_id}/themes/{theme_id}")
async def get_theme(job_id: str, theme_id: str):
    """Return a theme task with all its validation batches."""
    themes = job_store.get_theme_tasks(job_id)
    theme = next((t for t in themes if t["theme_id"] == theme_id), None)
    if not theme:
        raise HTTPException(status_code=404, detail=f"Theme {theme_id} not found")
    batches = job_store.get_validation_batches(theme_id)
    return {**theme, "batches": batches}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7888)
