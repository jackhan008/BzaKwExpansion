import os
from dotenv import load_dotenv

load_dotenv()

# Base Directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Azure OpenAI Configuration (loaded from .env)
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_DEPLOYMENT_NAME = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-5-mini")  # Or gpt-35-turbo
AZURE_OPENAI_DEPLOYMENT_NAME_EXPAND = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME_EXPAND", "gpt-5-mini")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview")

# Token Limits for Azure OpenAI
MAX_COMPLETION_TOKENS_EXPAND = 4000  # For keyword expansion; reasoning model needs budget for CoT + output
MAX_COMPLETION_TOKENS_VALIDATE = 8000  # For query validation

# Database Configuration (Default - Australia)
SQLITE_DB_PATH = os.path.join(BASE_DIR, "keywords.db")
VECTOR_DB_PATH = os.path.join(BASE_DIR, "keywords.index")
CSV_FILE_PATH = os.path.join(BASE_DIR, "en-au-query-7.csv")

# Market-specific Data Files Configuration
# Maps market to (csv_file, sqlite_db, faiss_index)
MARKET_DATA_FILES = {
    "Australia": {
        "csv": os.path.join(BASE_DIR, "en-au-query-7.csv"),
        "sqlite": os.path.join(BASE_DIR, "keywords_au.db"),
        "faiss": os.path.join(BASE_DIR, "keywords_au.index")
    },
    "Japan": {
        "csv": os.path.join(BASE_DIR, "jp-query.csv"),
        "sqlite": os.path.join(BASE_DIR, "keywords_jp.db"),
        "faiss": os.path.join(BASE_DIR, "keywords_jp.index")
    },
    "India": {
        "csv": os.path.join(BASE_DIR, "hi-in-query.csv"),
        "sqlite": os.path.join(BASE_DIR, "keywords_in.db"),
        "faiss": os.path.join(BASE_DIR, "keywords_in.index")
    },
    "Singapore": {
        "csv": os.path.join(BASE_DIR, "en-sg-query.csv"),
        "sqlite": os.path.join(BASE_DIR, "keywords_sg.db"),
        "faiss": os.path.join(BASE_DIR, "keywords_sg.index")
    },
    "Malaysia": {
        "csv": os.path.join(BASE_DIR, "ms-my-query.csv"),
        "sqlite": os.path.join(BASE_DIR, "keywords_my.db"),
        "faiss": os.path.join(BASE_DIR, "keywords_my.index")
    },
    "Thailand": {
        "csv": os.path.join(BASE_DIR, "th-th-query.csv"),
        "sqlite": os.path.join(BASE_DIR, "keywords_th.db"),
        "faiss": os.path.join(BASE_DIR, "keywords_th.index")
    },
    "Philippines": {
        "csv": os.path.join(BASE_DIR, "fil-ph-query.csv"),
        "sqlite": os.path.join(BASE_DIR, "keywords_ph.db"),
        "faiss": os.path.join(BASE_DIR, "keywords_ph.index")
    },
    "Indonesia": {
        "csv": os.path.join(BASE_DIR, "id-id-query.csv"),
        "sqlite": os.path.join(BASE_DIR, "keywords_id.db"),
        "faiss": os.path.join(BASE_DIR, "keywords_id.index")
    },
    "Vietnam": {
        "csv": os.path.join(BASE_DIR, "vi-vn-query.csv"),
        "sqlite": os.path.join(BASE_DIR, "keywords_vn.db"),
        "faiss": os.path.join(BASE_DIR, "keywords_vn.index")
    },
    "China": {
        "csv": os.path.join(BASE_DIR, "zh-cn-query.csv"),
        "sqlite": os.path.join(BASE_DIR, "keywords_cn.db"),
        "faiss": os.path.join(BASE_DIR, "keywords_cn.index")
    },
}

# Embedding Model
EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2" # Efficient and good for English

# Multilingual embedding model for non-English markets
EMBEDDING_MODEL_MULTILINGUAL = "paraphrase-multilingual-MiniLM-L12-v2"

# Market to Embedding Model mapping
MARKET_EMBEDDING_MODEL = {
    "Australia": EMBEDDING_MODEL_NAME,
    "Japan": EMBEDDING_MODEL_MULTILINGUAL,
    "India": EMBEDDING_MODEL_MULTILINGUAL,
    "Singapore": EMBEDDING_MODEL_MULTILINGUAL,
    "Malaysia": EMBEDDING_MODEL_MULTILINGUAL,
    "Thailand": EMBEDDING_MODEL_MULTILINGUAL,
    "Philippines": EMBEDDING_MODEL_NAME,  # Primarily English
    "Indonesia": EMBEDDING_MODEL_MULTILINGUAL,
    "Vietnam": EMBEDDING_MODEL_MULTILINGUAL,
    "China": EMBEDDING_MODEL_MULTILINGUAL,
}

# Market Configuration - Maps market to primary languages
MARKET_LANGUAGES = {
    "Australia": ["English"],
    "Japan": ["Japanese", "English"],
    "India": ["English", "Hindi"],
    "Singapore": ["English", "Chinese", "Malay"],
    "Malaysia": ["Malay", "English", "Chinese"],
    "Thailand": ["Thai", "English"],
    "Philippines": ["English", "Filipino"],
    "Indonesia": ["Indonesian", "English"],
    "Vietnam": ["Vietnamese", "English"],
    "China": ["Chinese", "English"],
}

# Default market
DEFAULT_MARKET = "Australia"

# Available markets list
AVAILABLE_MARKETS = list(MARKET_LANGUAGES.keys())

# ---------------------------------------------------------------------------
# Azure Data Source Configuration
# ---------------------------------------------------------------------------

# Environment: "local" -> AzureCliCredential, "cloud" -> ManagedIdentityCredential
APP_ENV = os.getenv("APP_ENV", "local")

# Toggle: set USE_AZURE_DATASOURCE=true to use Azure SQL + AI Search instead of
# local SQLite + FAISS files. Existing local DB files are NOT deleted.
USE_AZURE_DATASOURCE = os.getenv("USE_AZURE_DATASOURCE", "false").lower() == "true"

# Azure SQL Database (Hard Match)
# Required when USE_AZURE_DATASOURCE=true
# AZURE_SQL_SERVER   -> e.g. "bza-keywords.database.windows.net"
# AZURE_SQL_DATABASE -> e.g. "keywords-db"
# AZURE_SQL_DRIVER   -> must match installed ODBC driver; default covers most setups
AZURE_SQL_SERVER   = os.getenv("AZURE_SQL_SERVER")
AZURE_SQL_DATABASE = os.getenv("AZURE_SQL_DATABASE")
AZURE_SQL_DRIVER   = os.getenv("AZURE_SQL_DRIVER", "ODBC Driver 18 for SQL Server")
AZURE_SQL_PORT     = int(os.getenv("AZURE_SQL_PORT", "1433"))

# Azure SQL table configuration
# Schema: Id, TargetMarket, DeviceType, Query, Srpv, WindowStartDate, WindowEndDate, ...
# All markets use the default table filtered by TargetMarket + DeviceType.
AZURE_SQL_TABLE_DEFAULT = os.getenv("AZURE_SQL_TABLE_DEFAULT", "KeywordExpansion_30d_srpv_gt_50")

# Per-market table overrides (optional). If a market is listed here, its queries go to this table.
AZURE_SQL_TABLE_BY_MARKET = {
    "China": os.getenv("AZURE_SQL_TABLE_CN", "KeywordExpansion_30d_query_cn_gt_50"),
}

# Market code mapping for TargetMarket column
AZURE_SQL_MARKET_CODE = {
    "Australia":   "au",
    "Japan":       "jp",
    "India":       "in",
    "Singapore":   "sg",
    "Malaysia":    "my",
    "Thailand":    "th",
    "Philippines": "ph",
    "Indonesia":   "id",
    "Vietnam":     "vn",
    "China":       "cn",
}


# Azure AI Search (Vector / Keyword Match) — query text sent directly, no local embeddings
# Required when USE_AZURE_DATASOURCE=true
# AZURE_SEARCH_ENDPOINT -> e.g. "https://bza-keywords-search.search.windows.net"
AZURE_SEARCH_ENDPOINT    = os.getenv("AZURE_SEARCH_ENDPOINT")
AZURE_SEARCH_API_VERSION = os.getenv("AZURE_SEARCH_API_VERSION", "2024-05-01-preview")
# Optional API key — if set, used instead of Azure credential (no RBAC required)
AZURE_SEARCH_API_KEY     = os.getenv("AZURE_SEARCH_API_KEY")

# Per-market AI Search index names, keyed by device type.
# Markets with a single index use {"pc": "..."} — queried for any device type request.
# Markets with separate indexes per device type use {"pc": "...", "mobile": "..."}.
# Values may be overridden via environment variables.
MARKET_SEARCH_INDEX = {
    "Australia":   {"pc": os.getenv("AZURE_SEARCH_INDEX_AU", "keywords-au")},
    "Japan": {
        "pc":     os.getenv("AZURE_SEARCH_INDEX_JP_PC",     "kw-30d-query-jp-pc-top-300k"),
        "mobile": os.getenv("AZURE_SEARCH_INDEX_JP_MOBILE", "kw-30d-query-jp-mobile-top-300k"),
    },
    "India":       {"pc": os.getenv("AZURE_SEARCH_INDEX_IN", "keywords-in")},
    "Singapore":   {"pc": os.getenv("AZURE_SEARCH_INDEX_SG", "keywords-sg")},
    "Malaysia":    {"pc": os.getenv("AZURE_SEARCH_INDEX_MY", "keywords-my")},
    "Thailand":    {"pc": os.getenv("AZURE_SEARCH_INDEX_TH", "keywords-th")},
    "Philippines": {"pc": os.getenv("AZURE_SEARCH_INDEX_PH", "keywords-ph")},
    "Indonesia":   {"pc": os.getenv("AZURE_SEARCH_INDEX_ID", "keywords-id")},
    "Vietnam":     {"pc": os.getenv("AZURE_SEARCH_INDEX_VN", "keywords-vn")},
    "China": {
        "pc":     os.getenv("AZURE_SEARCH_INDEX_CN_PC",     "kw-30d-query-cn-pc-top-600k"),
        "mobile": os.getenv("AZURE_SEARCH_INDEX_CN_MOBILE", "kw-30d-query-cn-mobile-top-300k"),
    },
}

# AI Search field names (must match the index schema)
AZURE_SEARCH_QUERY_FIELD   = os.getenv("AZURE_SEARCH_QUERY_FIELD",   "normalized_query")
AZURE_SEARCH_SRPV_FIELD    = os.getenv("AZURE_SEARCH_SRPV_FIELD",    "SRPV")
AZURE_SEARCH_ADCLICK_FIELD = os.getenv("AZURE_SEARCH_ADCLICK_FIELD", "AdClick")
AZURE_SEARCH_REVENUE_FIELD = os.getenv("AZURE_SEARCH_REVENUE_FIELD", "revenue")
AZURE_SEARCH_TOP_K         = int(os.getenv("AZURE_SEARCH_TOP_K", "100"))


def get_azure_credential():
    """Return the appropriate Azure credential based on APP_ENV.

    APP_ENV=local  -> AzureCliCredential  (requires `az login`)
    APP_ENV=cloud  -> ManagedIdentityCredential
    """
    from azure.identity import AzureCliCredential, ManagedIdentityCredential
    if APP_ENV == "cloud":
        return ManagedIdentityCredential()
    return AzureCliCredential(process_timeout=60)
