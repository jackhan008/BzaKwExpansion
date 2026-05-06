# BZA Keyword Expansion API

Brand keyword expansion tool: input a brand/product name → AI expands to related keywords → database match → AI validation → structured results.

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure `.env`

```ini
# Azure OpenAI (required)
AZURE_OPENAI_API_KEY=your_key
AZURE_OPENAI_ENDPOINT=https://your-resource.openai.azure.com/
AZURE_OPENAI_DEPLOYMENT_NAME=gpt-4o-mini
AZURE_OPENAI_DEPLOYMENT_NAME_EXPAND=gpt-4o-mini
AZURE_OPENAI_API_VERSION=2024-08-01-preview

# Public API key (optional — leave empty for open access)
API_KEY=your_secret_key

# Data source: true = Azure SQL + Azure AI Search, false = local SQLite + FAISS
USE_AZURE_DATASOURCE=false
```

### 3. Start the server

```bash
python app.py
# or
uvicorn app:app --host 0.0.0.0 --port 7888
```

- Web UI: http://localhost:7888
- Interactive API docs: http://localhost:7888/docs

---

## Public API (`/v1/`)

Base URL: `http://localhost:7888`

### Authentication

When `API_KEY` is set in the environment, all `/v1/` requests must include the header:

```
X-API-Key: your_secret_key
```

If `API_KEY` is not set (default), the endpoints are open.

---

### GET `/v1/markets`

Returns the list of supported markets and their languages.

**Request**

```bash
curl http://localhost:7888/v1/markets \
  -H "X-API-Key: your_secret_key"
```

**Response**

```json
{
  "markets": ["Australia", "Japan", "China", "India", "Singapore", "Malaysia", "Thailand", "Philippines", "Indonesia", "Vietnam"],
  "default_market": "Australia",
  "market_languages": {
    "Australia": ["English"],
    "Japan": ["Japanese", "English"],
    "China": ["Chinese", "English"]
  }
}
```

---

### POST `/v1/expand`

Expand themes and return all matched queries as structured JSON.

**Request body**

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `themes` | `string[]` | yes | — | Brand or product names |
| `market` | `string` | no | `"Australia"` | Target market (see `/v1/markets`) |
| `device_types` | `string[]` | no | `["pc"]` | `"pc"`, `"mobile"`, or both |
| `landing_pages` | `{theme: url}` | no | `null` | Brand landing page URLs for richer context |

**Example — single theme**

```bash
curl -X POST http://localhost:7888/v1/expand \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your_secret_key" \
  -d '{
    "themes": ["Nike"],
    "market": "Australia",
    "device_types": ["pc"]
  }'
```

**Example — multiple themes with landing pages**

```bash
curl -X POST http://localhost:7888/v1/expand \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your_secret_key" \
  -d '{
    "themes": ["Nike", "Adidas"],
    "market": "Australia",
    "device_types": ["pc", "mobile"],
    "landing_pages": {
      "Nike": "https://www.nike.com/au"
    }
  }'
```

**Response**

```json
{
  "job_id": "a1b2c3d4",
  "market": "Australia",
  "results": [
    {
      "theme": "Nike",
      "expanded_keywords": ["nike shoes", "nike running shoes", "nike air max", "..."],
      "match_count": 42,
      "matched_queries": [
        {
          "query": "nike shoes online",
          "score": 3,
          "relevance": 0.92,
          "srpv": 1500,
          "ad_clicks": 60,
          "revenue": 240.0,
          "ai_valid": true,
          "ai_reason": "Directly refers to Nike brand"
        },
        {
          "query": "nike air max",
          "score": 2,
          "relevance": 1.0,
          "srpv": 800,
          "ad_clicks": 30,
          "revenue": 120.5
        }
      ]
    }
  ]
}
```

**Response fields**

| Field | Description |
|---|---|
| `job_id` | Unique identifier for this job (8 hex chars) |
| `market` | Market used |
| `results[].theme` | Input theme name |
| `results[].expanded_keywords` | AI-generated keyword variants |
| `results[].match_count` | Total matched queries |
| `results[].matched_queries[].query` | Matched search query |
| `results[].matched_queries[].score` | Match strength: 3 = hard+vector, 2 = hard only, 1 = vector only |
| `results[].matched_queries[].relevance` | Keyword coverage ratio (0–1) |
| `results[].matched_queries[].srpv` | Search result page views |
| `results[].matched_queries[].ad_clicks` | Ad click count |
| `results[].matched_queries[].revenue` | Revenue estimate |
| `results[].matched_queries[].ai_valid` | AI validation result (bool, present when validation ran) |
| `results[].matched_queries[].ai_reason` | AI validation reason (string, present when validation ran) |

---

### POST `/v1/expand/stream`

Same as `/v1/expand` but streams results incrementally as newline-delimited JSON (NDJSON). Each line is one complete JSON object.

**Request body**: identical to `/v1/expand`.

**Event types**

| `type` | When | Extra fields |
|---|---|---|
| `job_start` | Immediately | `job_id` |
| `theme_result` | After each theme finishes | `data.theme`, `data.expanded_keywords`, `data.match_count`, `data.matched_queries` |
| `complete` | All themes done | `job_id` |
| `error` | On failure | `message` |

**Example — Python**

```python
import requests, json

resp = requests.post(
    "http://localhost:7888/v1/expand/stream",
    headers={"X-API-Key": "your_secret_key"},
    json={"themes": ["Nike", "Adidas"], "market": "Australia"},
    stream=True,
)

for line in resp.iter_lines():
    if line:
        event = json.loads(line)
        if event["type"] == "theme_result":
            d = event["data"]
            print(f"{d['theme']}: {d['match_count']} queries matched")
        elif event["type"] == "complete":
            print("Done")
```

**Example — curl**

```bash
curl -X POST http://localhost:7888/v1/expand/stream \
  -H "Content-Type: application/json" \
  -H "X-API-Key: your_secret_key" \
  -d '{"themes": ["Nike"], "market": "Australia"}' \
  --no-buffer
```

**Sample stream output**

```
{"type": "job_start", "job_id": "a1b2c3d4"}
{"type": "theme_result", "data": {"theme": "Nike", "expanded_keywords": ["nike shoes", "..."], "match_count": 42, "matched_queries": [...]}}
{"type": "complete", "job_id": "a1b2c3d4"}
```

---

## Error Responses

| HTTP Status | Cause |
|---|---|
| `400 Bad Request` | Empty themes list or unknown market |
| `401 Unauthorized` | Missing or invalid `X-API-Key` |
| `500 Internal Server Error` | Unexpected server error |

Error body:

```json
{"detail": "Invalid or missing API key"}
```

---

## Python SDK Example

```python
import requests

BASE_URL = "http://localhost:7888"
API_KEY  = "your_secret_key"


def expand_keywords(themes, market="Australia", device_types=None):
    resp = requests.post(
        f"{BASE_URL}/v1/expand",
        headers={"X-API-Key": API_KEY},
        json={
            "themes": themes,
            "market": market,
            "device_types": device_types or ["pc"],
        },
        timeout=300,
    )
    resp.raise_for_status()
    return resp.json()


result = expand_keywords(["Nike", "Adidas"], market="Australia")
for theme_result in result["results"]:
    print(f"{theme_result['theme']}: {theme_result['match_count']} matches")
    for q in theme_result["matched_queries"][:5]:
        print(f"  [{q['score']}] {q['query']}")
```

---

## Internal API (`/api/`)

The `/api/` endpoints are for the built-in web UI. They are not authenticated and return different response shapes (including embedded CSV strings). Use `/v1/` for external integrations.

| Endpoint | Description |
|---|---|
| `GET /api/markets` | Market list (same as `/v1/markets`) |
| `POST /api/expand` | Batch expand, returns `{job_id, details, csv_content}` |
| `POST /api/expand_stream` | Stream expand (NDJSON, includes final CSV) |
| `GET /api/jobs` | Job history list |
| `GET /api/jobs/{job_id}` | Job detail + theme tasks |
| `GET /api/jobs/{job_id}/themes/{theme_id}` | Theme task + validation batches |

---

## Architecture Overview

```
Input themes
    │
    ▼
Step 1: AI Expand (Azure OpenAI)
    → generates ≤15 keyword variants per theme (multilingual)
    │
    ▼
Step 2: Match (dual-path)
    ├─ Hard Match: SQL/SQLite LIKE query  → Score +2
    └─ Vector Match: FAISS cos≥0.8       → Score +1
    │
    ▼
Step 3: AI Validate (Azure OpenAI)
    → batch-validates 25 queries per call, filters irrelevant results
    │
    ▼
Structured JSON response
```

**Supported markets:** Australia, Japan, India, Singapore, Malaysia, Thailand, Philippines, Indonesia, Vietnam, China
