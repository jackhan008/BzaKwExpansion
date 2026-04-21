import sqlite3
import concurrent.futures
import pandas as pd
import faiss
import numpy as np
from sentence_transformers import SentenceTransformer
import os
import struct
import threading
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
            # _search_indexes_by_device: {device_type: index_name}
            # e.g. {"pc": "kw-...-pc", "mobile": "kw-...-mobile"} or {"pc": "keywords-au"}
            idx_config = config.MARKET_SEARCH_INDEX.get(market, {"pc": f"keywords-{market.lower()}"})
            if isinstance(idx_config, dict):
                self._search_indexes_by_device = idx_config
            elif isinstance(idx_config, list):
                # legacy list format — treat as pc + mobile in order
                devices = ["pc", "mobile"]
                self._search_indexes_by_device = {devices[i]: v for i, v in enumerate(idx_config)}
            else:
                self._search_indexes_by_device = {"pc": idx_config}
            self._local = threading.local()   # thread-local Azure SQL connection cache

    
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

    def query_sqlite_contains(self, term, device_types=None):
        """Finds queries containing the term (Hard Match).

        Routes to Azure SQL or local SQLite based on use_azure flag.
        device_types: list of device types to filter (Azure mode only); None means ["pc"].
        """
        if self.use_azure:
            return self._query_azure_sql_contains(term, device_types or ["pc"])

        conn = sqlite3.connect(self.sqlite_path)
        # Remove both half-width and full-width spaces from the search term
        clean_term = term.replace(" ", "").replace("\u3000", "")
        
        # For Japanese market, need to handle both space types in the database
        # Use nested REPLACE to remove both half-width space and full-width space (U+3000)
        query = f"""
            SELECT normalized_query, SRPV, AdClick, revenue
            FROM keywords
            WHERE normalized_query LIKE '{term}%'
               OR normalized_query LIKE '%{term}'
            """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df

    def query_vector_similarity(self, term, n_results=100, device_types=None):
        """Finds semantically similar queries.

        Routes to Azure AI Search or local FAISS based on use_azure flag.
        device_types: list of device types to filter (Azure mode only); None means ["pc"].
        """
        if self.use_azure:
            return self._query_azure_search_similarity(term, n_results, device_types or ["pc"])

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
        """Return a thread-local pyodbc connection to Azure SQL (token-based auth).

        The connection is created once per thread and reused across queries.
        A lightweight liveness check (SELECT 1) recycles stale connections.
        """
        import pyodbc
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            try:
                conn.execute("SELECT 1")
            except Exception:
                conn = None
        if conn is None:
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
            conn = pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
            self._local.conn = conn
        return conn

    def _query_azure_sql_contains(self, term, device_types) -> pd.DataFrame:
        """Hard Match via Azure SQL — parameterized query, returns same schema as SQLite path.

        Filters by TargetMarket and DeviceType.
        Field mapping: Query -> normalized_query, Srpv -> SRPV.
        AdClick and revenue are not available in this table, defaulted to 0.
        """
        clean_term  = term.replace(" ", "").replace("\u3000", "")
        table       = config.AZURE_SQL_TABLE_BY_MARKET.get(self.market, config.AZURE_SQL_TABLE_DEFAULT)
        market_code = config.AZURE_SQL_MARKET_CODE.get(self.market)

        placeholders = ", ".join("?" * len(device_types))

        if self.market in config.AZURE_SQL_TABLE_BY_MARKET:
            # Market-specific table (e.g. China): prefix-only match until index is ready.
            sql = (
                f"SELECT Query, Srpv FROM dbo.[{table}] "
                f"WHERE DeviceType IN ({placeholders}) AND Query LIKE ?"
            )
            params = (*device_types, f"{clean_term}%")
        else:
            # Default table has QueryClean persisted computed column (spaces stripped).
            sql = (
                f"SELECT Query, Srpv FROM dbo.[{table}] "
                f"WHERE TargetMarket = ? AND DeviceType IN ({placeholders}) "
                f"AND QueryClean LIKE ?"
            )
            params = (market_code, *device_types, f"%{clean_term}%")

        conn = self._get_azure_sql_connection()
        df = pd.read_sql(sql, conn, params=params)

        df = df.rename(columns={"Query": "normalized_query", "Srpv": "SRPV"})
        df["AdClick"] = 0
        df["revenue"] = 0
        return df

    def _query_azure_search_similarity(self, term, n_results=100, device_types=None) -> pd.DataFrame:
        """Vector Match via Azure AI Search — full-text / semantic search on keyword text.

        Queries the indexes matching the requested device_types in parallel and merges results,
        deduplicating by normalized_query (keeping the row with the lowest distance).
        For markets with a single index (no device split), that index is always queried.
        No local embedding is generated; the term is passed directly to AI Search.
        Auth: uses API key if AZURE_SEARCH_API_KEY is set, otherwise Azure credential (RBAC).
        Results: [id, normalized_query, SRPV, AdClick, revenue, distance]
        distance = 1 - score  (AI Search returns relevance scores 0..1)
        """
        from azure.search.documents import SearchClient
        from azure.core.credentials import AzureKeyCredential

        if config.AZURE_SEARCH_API_KEY:
            credential = AzureKeyCredential(config.AZURE_SEARCH_API_KEY)
        else:
            credential = self._credential

        qf   = config.AZURE_SEARCH_QUERY_FIELD
        srpv = config.AZURE_SEARCH_SRPV_FIELD
        select_fields = [qf] + ([srpv] if srpv else [])

        # Resolve which indexes to query based on device_types
        if device_types:
            indexes_to_query = [
                self._search_indexes_by_device[dt]
                for dt in device_types
                if dt in self._search_indexes_by_device
            ]
            if not indexes_to_query:
                # Requested device type not available for this market → query all
                indexes_to_query = list(self._search_indexes_by_device.values())
        else:
            indexes_to_query = list(self._search_indexes_by_device.values())

        def _query_one(index_name):
            client = SearchClient(
                endpoint=config.AZURE_SEARCH_ENDPOINT,
                index_name=index_name,
                credential=credential,
                api_version=config.AZURE_SEARCH_API_VERSION,
            )
            results = client.search(
                search_text=term,
                select=select_fields,
                top=n_results,
            )
            rows = []
            for r in results:
                score = r.get("@search.score", 0.0)
                rows.append({
                    "normalized_query": r.get(qf, ""),
                    "SRPV":             r.get(srpv, 0) if srpv else 0,
                    "AdClick":          0,
                    "revenue":          0,
                    "distance":         1 - score,
                })
            return rows

        # Query all selected indexes in parallel
        all_rows = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(indexes_to_query)) as executor:
            futures = [executor.submit(_query_one, idx) for idx in indexes_to_query]
            for f in concurrent.futures.as_completed(futures):
                all_rows.extend(f.result())

        if not all_rows:
            return pd.DataFrame(
                columns=["id", "normalized_query", "SRPV", "AdClick", "revenue", "distance"]
            )

        df = pd.DataFrame(all_rows)
        # Deduplicate by normalized_query, keep lowest distance (highest score)
        df = df.sort_values("distance").drop_duplicates(subset="normalized_query").reset_index(drop=True)
        df.insert(0, "id", df.index)
        return df

