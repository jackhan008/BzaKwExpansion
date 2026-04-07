import sqlite3
import pandas as pd
import faiss
import numpy as np
from sentence_transformers import SentenceTransformer
import os
import config

class DBManager:
    def __init__(self, market="Australia"):
        """Initialize DBManager for a specific market."""
        self.market = market
        self._load_market_config(market)
        
        # Initialize Embedding Function based on market
        embedding_model_name = config.MARKET_EMBEDDING_MODEL.get(
            market, config.EMBEDDING_MODEL_NAME
        )
        self.embedding_model = SentenceTransformer(embedding_model_name)
        
        # Load FAISS Index if exists
        if os.path.exists(self.vector_db_path):
            self.index = faiss.read_index(self.vector_db_path)
        else:
            self.index = None
    
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
        """Initializes SQLite and FAISS Index from CSV if not already populated."""
        
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
        
        For Japanese market, also handles full-width spaces (U+3000).
        """
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
        """Finds semantically similar queries using FAISS."""
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
