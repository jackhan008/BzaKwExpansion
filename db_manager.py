import sqlite3
import pandas as pd
import faiss
import numpy as np
from sentence_transformers import SentenceTransformer
import os
import struct
import config


class DBManager:
    def __init__(self, market="Australia", use_azure=False):
        """Initialize DBManager for a specific market.

        use_azure=True  -> Hard Match via Azure SQL, Vector Match via Azure AI Search.
        use_azure=False -> Hard Match via local SQLite, Vector Match via local FAISS.
        """
        self.market = market
        self.use_azure = use_azure
        self._load_market_config(market)

        if not use_azure:
            # Local mode: load embedding model and FAISS index
            embedding_model_name = config.MARKET_EMBEDDING_MODEL.get(
                market, config.EMBEDDING_MODEL_NAME
            )
            self.embedding_model = SentenceTransformer(embedding_model_name)

            if os.path.exists(self.vector_db_path):
                self.index = faiss.read_index(self.vector_db_path)
            else:
                self.index = None
        else:
            # Azure mode: no local embedding model or FAISS index needed
            self.embedding_model = None
            self.index = None
            self._credential = config.get_azure_credential()
            self._search_index = config.MARKET_SEARCH_INDEX.get(market, f"keywords-{market.lower()}")

    
    def _load_market_config(self, market):
        """Load file paths for the specified market."""
        if market in config.MARKET_DATA_FILES:
            market_config = config.MARKET_DATA_FILES[market]
            self.csv_path = market_config["csv"]
            self.sqlite_path = market_config["sqlite"]
            self.vector_db_path = market_config["faiss"]
        else:
            # Fallback to Australia/default if market data not available
            fallback_market = "Australia"
            if fallback_market in config.MARKET_DATA_FILES:
                market_config = config.MARKET_DATA_FILES[fallback_market]
                self.csv_path = market_config["csv"]
                self.sqlite_path = market_config["sqlite"]
                self.vector_db_path = market_config["faiss"]
            else:
                # Use legacy default paths
                self.sqlite_path = config.SQLITE_DB_PATH
                self.vector_db_path = config.VECTOR_DB_PATH
                self.csv_path = config.CSV_FILE_PATH
            print(f"Warning: No data files configured for market '{market}'. Using {fallback_market} data.")

    def initialize_db(self):
        """Initializes SQLite and FAISS Index from CSV if not already populated.

        In Azure mode this is a no-op — data lives in the cloud.
        """
        if self.use_azure:
            return
        
        print(f"Checking database for market: {self.market}")
        print(f"  SQLite: {self.sqlite_path}")
        print(f"  FAISS: {self.vector_db_path}")
        print(f"  CSV: {self.csv_path}")
        
        # Check if SQLite is populated
        conn = sqlite3.connect(self.sqlite_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='keywords'")
        table_exists = cursor.fetchone()
        
        if table_exists and os.path.exists(self.vector_db_path):
            print(f"Database and Index for {self.market} already initialized. Skipping ingestion.")
            conn.close()
            if self.index is None:
                 self.index = faiss.read_index(self.vector_db_path)
            return

        print(f"Initializing databases for {self.market}... This may take a while.")
        
        # Load CSV
        if not os.path.exists(self.csv_path):
            raise FileNotFoundError(f"CSV file not found at {self.csv_path}")
            
        df = pd.read_csv(self.csv_path)
        
        # 1. Setup SQLite
        # We add an explicit ID column to ensure mapping is stable
        df['id'] = df.index
        df.to_sql('keywords', conn, if_exists='replace', index=False)
        cursor.execute("CREATE INDEX idx_query ON keywords(normalized_query)")
        cursor.execute("CREATE INDEX idx_id ON keywords(id)")
        conn.commit()
        conn.close()
        print("SQLite initialized.")

        # 2. Setup FAISS
        print(f"Generating embeddings for {len(df)} rows...")
        
        # Generate embeddings in batches
        documents = df['normalized_query'].astype(str).tolist()
        embeddings = self.embedding_model.encode(documents, show_progress_bar=True)
        
        # Convert to float32 and normalize for Cosine Similarity
        embeddings = np.array(embeddings).astype('float32')
        faiss.normalize_L2(embeddings)
        
        # Create Index (Inner Product for Cosine Similarity on normalized vectors)
        dimension = embeddings.shape[1]
        self.index = faiss.IndexFlatIP(dimension)
        self.index.add(embeddings)
        
        # Save Index
        faiss.write_index(self.index, self.vector_db_path)
        print("FAISS Index initialized and saved.")

    def query_sqlite_contains(self, term):
        """Finds queries containing the term (Hard Match).

        Routes to Azure SQL or local SQLite based on use_azure flag.
        """
        if self.use_azure:
            return self._query_azure_sql_contains(term)

        conn = sqlite3.connect(self.sqlite_path)
        # Remove both half-width and full-width spaces from the search term
        clean_term = term.replace(" ", "").replace("\u3000", "")
        
        # For Japanese market, need to handle both space types in the database
        # Use nested REPLACE to remove both half-width space and full-width space (U+3000)
        if self.market == "Japan":
            query = f"""
            SELECT normalized_query, SRPV, AdClick, revenue 
            FROM keywords 
            WHERE REPLACE(REPLACE(normalized_query, ' ', ''), '　', '') LIKE '%{clean_term}%'
            """
        else:
            query = f"""
            SELECT normalized_query, SRPV, AdClick, revenue 
            FROM keywords 
            WHERE REPLACE(normalized_query, ' ', '') LIKE '%{clean_term}%'
            """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def query_vector_similarity(self, term, n_results=100):
        """Finds semantically similar queries.

        Routes to Azure AI Search or local FAISS based on use_azure flag.
        """
        if self.use_azure:
            return self._query_azure_search_similarity(term, n_results)

        if self.index is None:
             # Try to load if not loaded
             if os.path.exists(self.vector_db_path):
                 self.index = faiss.read_index(self.vector_db_path)
             else:
                 raise Exception("Index not initialized. Run initialize_db first.")

        # Generate and normalize query embedding
        query_embedding = self.embedding_model.encode([term])
        query_embedding = np.array(query_embedding).astype('float32')
        faiss.normalize_L2(query_embedding)
        
        # Search
        distances, indices = self.index.search(query_embedding, n_results)
        
        # Retrieve metadata from SQLite based on indices
        # indices[0] contains the IDs (which match our DataFrame index/id column)
        found_ids = indices[0].tolist()
        found_scores = distances[0].tolist() # These are cosine similarities
        
        if not found_ids:
            return pd.DataFrame()

        # Fetch details from SQLite
        conn = sqlite3.connect(self.sqlite_path)
        id_list = ",".join(map(str, found_ids))
        query = f"SELECT id, normalized_query, SRPV, AdClick, revenue FROM keywords WHERE id IN ({id_list})"
        df_results = pd.read_sql_query(query, conn)
        conn.close()
        
        # Map scores back to the dataframe
        id_to_score = dict(zip(found_ids, found_scores))
        
        # Convert Cosine Similarity to "Distance" (1 - Similarity) to match matcher.py expectation
        # matcher.py expects: similarity = 1 - distance
        # So: distance = 1 - similarity
        df_results['distance'] = df_results['id'].map(lambda x: 1 - id_to_score.get(x, 0))

        return df_results

    # ------------------------------------------------------------------
    # Azure backend — private methods
    # ------------------------------------------------------------------

    def _get_azure_sql_connection(self):
        """Return a pyodbc connection to Azure SQL using token-based auth."""
        import pyodbc
        token = self._credential.get_token("https://database.windows.net/.default").token
        token_bytes = token.encode("utf-16-le")
        token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
        conn_str = (
            f"Driver={{{config.AZURE_SQL_DRIVER}}};"
            f"Server={config.AZURE_SQL_SERVER},{config.AZURE_SQL_PORT};"
            f"Database={config.AZURE_SQL_DATABASE};"
            "Encrypt=yes;TrustServerCertificate=no;"
        )
        SQL_COPT_SS_ACCESS_TOKEN = 1256
        return pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})

    def _query_azure_sql_contains(self, term) -> pd.DataFrame:
        """Hard Match via Azure SQL — parameterized query, returns same schema as SQLite path.

        Uses a dedicated per-market table if configured, otherwise filters the default
        table by TargetMarket code. Field mapping:
            Query  -> normalized_query
            Srpv   -> SRPV
            (AdClick and revenue not available, defaulted to 0)
        """
        clean_term = term.replace(" ", "").replace("\u3000", "")

        # Resolve table and optional market filter
        table = config.AZURE_SQL_MARKET_TABLE.get(self.market) or config.AZURE_SQL_TABLE_DEFAULT
        market_code = config.AZURE_SQL_MARKET_CODE.get(self.market)
        dedicated_table = bool(config.AZURE_SQL_MARKET_TABLE.get(self.market))

        if self.market == "Japan":
            where_query = "REPLACE(REPLACE(Query, ' ', ''), N'\u3000', '') LIKE ?"
        else:
            where_query = "REPLACE(Query, ' ', '') LIKE ?"

        if dedicated_table:
            sql = f"SELECT Query, Srpv FROM dbo.[{table}] WHERE {where_query}"
            params = (f"%{clean_term}%",)
        else:
            sql = f"SELECT Query, Srpv FROM dbo.[{table}] WHERE TargetMarket = ? AND {where_query}"
            params = (market_code, f"%{clean_term}%")

        conn = self._get_azure_sql_connection()
        df = pd.read_sql(sql, conn, params=params)
        conn.close()

        # Align to expected schema
        df = df.rename(columns={"Query": "normalized_query", "Srpv": "SRPV"})
        df["AdClick"] = 0
        df["revenue"] = 0
        return df

    def _query_azure_search_similarity(self, term, n_results=100) -> pd.DataFrame:
        """Vector Match via Azure AI Search — full-text / semantic search on keyword text.

        No local embedding is generated; the term is passed directly to AI Search.
        Auth: uses API key if AZURE_SEARCH_API_KEY is set, otherwise Azure credential (RBAC).
        Results are returned in the same schema as the FAISS path so matcher.py
        requires no changes:  [id, normalized_query, SRPV, AdClick, revenue, distance]
        distance = 1 - score  (AI Search returns relevance scores 0..1)
        """
        from azure.search.documents import SearchClient
        from azure.core.credentials import AzureKeyCredential

        if config.AZURE_SEARCH_API_KEY:
            credential = AzureKeyCredential(config.AZURE_SEARCH_API_KEY)
        else:
            credential = self._credential

        client = SearchClient(
            endpoint=config.AZURE_SEARCH_ENDPOINT,
            index_name=self._search_index,
            credential=credential,
            api_version=config.AZURE_SEARCH_API_VERSION,
        )

        qf   = config.AZURE_SEARCH_QUERY_FIELD
        srpv = config.AZURE_SEARCH_SRPV_FIELD

        # Only select fields that exist in the index; AdClick/revenue default to 0
        select_fields = [qf]
        if srpv:
            select_fields.append(srpv)

        results = client.search(
            search_text=term,
            select=select_fields,
            top=n_results,
        )

        rows = []
        for i, r in enumerate(results):
            score = r.get("@search.score", 0.0)
            rows.append({
                "id":               i,
                "normalized_query": r.get(qf, ""),
                "SRPV":             r.get(srpv, 0) if srpv else 0,
                "AdClick":          0,
                "revenue":          0,
                "distance":         1 - score,
            })

        if not rows:
            return pd.DataFrame(
                columns=["id", "normalized_query", "SRPV", "AdClick", "revenue", "distance"]
            )
        return pd.DataFrame(rows)

