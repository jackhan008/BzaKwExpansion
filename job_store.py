"""
job_store.py — SQLite-based job history store for BZA Keywords Expand.

Database: logs/jobs.db

Schema (3 tables):

  jobs
    job_id       TEXT  PK          e.g. "a3f2c1b8"
    market       TEXT
    themes       TEXT  JSON list   e.g. '["Nike","Adidas"]'
    status       TEXT              pending | running | done | error
    created_at   TEXT  ISO8601
    finished_at  TEXT  ISO8601 (nullable)

  theme_tasks
    theme_id     TEXT  PK          e.g. "a3f2c1b8-t0"
    job_id       TEXT  FK → jobs
    theme        TEXT              the input brand/keyword
    market       TEXT
    status       TEXT              pending | running | done | error
    expanded_keywords   TEXT  JSON list   (after AI expansion)
    matched_queries     TEXT  JSON list   (after matching, before validation)
    validated_queries   TEXT  JSON list   (queries where AI_Valid=True)
    final_queries       TEXT  JSON list   (same as validated; included for clarity)
    match_count         INTEGER
    valid_count         INTEGER
    invalid_count       INTEGER
    error_msg    TEXT  (nullable)
    created_at   TEXT
    finished_at  TEXT (nullable)

  validation_batches
    batch_id     TEXT  PK          e.g. "a3f2c1b8-t0-b2"
    theme_id     TEXT  FK → theme_tasks
    job_id       TEXT
    batch_index  INTEGER
    queries      TEXT  JSON list   (input to this batch)
    results      TEXT  JSON object {query: {is_valid, reason}}
    status       TEXT              running | done | error
    created_at   TEXT
    finished_at  TEXT (nullable)
"""

import sqlite3
import json
import os
from datetime import datetime, timezone
from typing import Optional
from logger import get_logger

logger = get_logger(__name__)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", "jobs.db")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create tables if they don't exist. Safe to call on every startup."""
    with _conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id      TEXT PRIMARY KEY,
                market      TEXT NOT NULL,
                themes      TEXT NOT NULL,
                status      TEXT NOT NULL DEFAULT 'pending',
                created_at  TEXT NOT NULL,
                finished_at TEXT
            );

            CREATE TABLE IF NOT EXISTS theme_tasks (
                theme_id          TEXT PRIMARY KEY,
                job_id            TEXT NOT NULL,
                theme             TEXT NOT NULL,
                market            TEXT NOT NULL,
                status            TEXT NOT NULL DEFAULT 'pending',
                expanded_keywords TEXT,
                matched_queries   TEXT,
                validated_queries TEXT,
                final_queries     TEXT,
                match_count       INTEGER DEFAULT 0,
                valid_count       INTEGER DEFAULT 0,
                invalid_count     INTEGER DEFAULT 0,
                error_msg         TEXT,
                created_at        TEXT NOT NULL,
                finished_at       TEXT,
                FOREIGN KEY (job_id) REFERENCES jobs(job_id)
            );

            CREATE TABLE IF NOT EXISTS validation_batches (
                batch_id    TEXT PRIMARY KEY,
                theme_id    TEXT NOT NULL,
                job_id      TEXT NOT NULL,
                batch_index INTEGER NOT NULL,
                queries     TEXT NOT NULL,
                results     TEXT,
                status      TEXT NOT NULL DEFAULT 'running',
                created_at  TEXT NOT NULL,
                finished_at TEXT,
                FOREIGN KEY (theme_id) REFERENCES theme_tasks(theme_id)
            );

            CREATE INDEX IF NOT EXISTS idx_theme_job    ON theme_tasks(job_id);
            CREATE INDEX IF NOT EXISTS idx_batch_theme  ON validation_batches(theme_id);
            CREATE INDEX IF NOT EXISTS idx_batch_job    ON validation_batches(job_id);
        """)
    logger.info(f"job_store initialized: {DB_PATH}")


# ─────────────────────────────────────────────
# jobs
# ─────────────────────────────────────────────

def create_job(job_id: str, market: str, themes: list):
    with _conn() as conn:
        conn.execute(
            "INSERT INTO jobs (job_id, market, themes, status, created_at) VALUES (?,?,?,?,?)",
            (job_id, market, json.dumps(themes, ensure_ascii=False), "running", _now())
        )


def finish_job(job_id: str, status: str = "done"):
    with _conn() as conn:
        conn.execute(
            "UPDATE jobs SET status=?, finished_at=? WHERE job_id=?",
            (status, _now(), job_id)
        )


# ─────────────────────────────────────────────
# theme_tasks
# ─────────────────────────────────────────────

def create_theme_task(theme_id: str, job_id: str, theme: str, market: str):
    with _conn() as conn:
        conn.execute(
            """INSERT INTO theme_tasks
               (theme_id, job_id, theme, market, status, created_at)
               VALUES (?,?,?,?,?,?)""",
            (theme_id, job_id, theme, market, "running", _now())
        )


def update_theme_expanded(theme_id: str, expanded_keywords: list):
    with _conn() as conn:
        conn.execute(
            "UPDATE theme_tasks SET expanded_keywords=? WHERE theme_id=?",
            (json.dumps(expanded_keywords, ensure_ascii=False), theme_id)
        )


def update_theme_matched(theme_id: str, matched_queries: list):
    with _conn() as conn:
        conn.execute(
            "UPDATE theme_tasks SET matched_queries=?, match_count=? WHERE theme_id=?",
            (json.dumps(matched_queries, ensure_ascii=False), len(matched_queries), theme_id)
        )


def finish_theme_task(
    theme_id: str,
    validated_queries: list,
    final_queries: list,
    valid_count: int,
    invalid_count: int,
    status: str = "done",
    error_msg: Optional[str] = None,
):
    with _conn() as conn:
        conn.execute(
            """UPDATE theme_tasks SET
               status=?, validated_queries=?, final_queries=?,
               valid_count=?, invalid_count=?, error_msg=?, finished_at=?
               WHERE theme_id=?""",
            (
                status,
                json.dumps(validated_queries, ensure_ascii=False),
                json.dumps(final_queries, ensure_ascii=False),
                valid_count,
                invalid_count,
                error_msg,
                _now(),
                theme_id,
            )
        )


# ─────────────────────────────────────────────
# validation_batches
# ─────────────────────────────────────────────

def create_validation_batch(batch_id: str, theme_id: str, job_id: str, batch_index: int, queries: list):
    with _conn() as conn:
        conn.execute(
            """INSERT INTO validation_batches
               (batch_id, theme_id, job_id, batch_index, queries, status, created_at)
               VALUES (?,?,?,?,?,?,?)""",
            (batch_id, theme_id, job_id, batch_index,
             json.dumps(queries, ensure_ascii=False), "running", _now())
        )


def finish_validation_batch(batch_id: str, results: dict, status: str = "done"):
    with _conn() as conn:
        conn.execute(
            "UPDATE validation_batches SET results=?, status=?, finished_at=? WHERE batch_id=?",
            (json.dumps(results, ensure_ascii=False), status, _now(), batch_id)
        )


# ─────────────────────────────────────────────
# Query helpers (for history API)
# ─────────────────────────────────────────────

def list_jobs(limit: int = 50) -> list[dict]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


def get_job(job_id: str) -> Optional[dict]:
    with _conn() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
    return dict(row) if row else None


def get_theme_tasks(job_id: str) -> list[dict]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM theme_tasks WHERE job_id=? ORDER BY created_at", (job_id,)
        ).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        for field in ("expanded_keywords", "matched_queries", "validated_queries", "final_queries"):
            if d.get(field):
                d[field] = json.loads(d[field])
        result.append(d)
    return result


def get_validation_batches(theme_id: str) -> list[dict]:
    with _conn() as conn:
        rows = conn.execute(
            "SELECT * FROM validation_batches WHERE theme_id=? ORDER BY batch_index", (theme_id,)
        ).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        for field in ("queries", "results"):
            if d.get(field):
                d[field] = json.loads(d[field])
        result.append(d)
    return result
