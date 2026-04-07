import os
from dotenv import load_dotenv

load_dotenv()

# Base Directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Azure OpenAI Configuration (loaded from .env)
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
AZURE_OPENAI_ENDPOINT = os.getenv("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_DEPLOYMENT_NAME = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-5-mini")  # Or gpt-35-turbo
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview")

# Token Limits for Azure OpenAI
MAX_COMPLETION_TOKENS_EXPAND = 8000  # For keyword expansion
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
