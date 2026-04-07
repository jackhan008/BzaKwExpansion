# Brand Ad Keyword Expansion Tool

This tool expands search themes into related keywords using Azure OpenAI and matches them against a query database using both exact matching and semantic search.

## Setup

1.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Configuration:**
    -   Open `config.py` or create a `.env` file.
    -   Set your Azure OpenAI API keys and endpoint.
    ```python
    AZURE_OPENAI_API_KEY = "your_key"
    AZURE_OPENAI_ENDPOINT = "your_endpoint"
    AZURE_OPENAI_DEPLOYMENT_NAME = "gpt-4o"
    ```

3.  **Data:**
    -   Ensure `en-au-query.csv` is in this directory.

## Web Interface

To run the web interface:

1.  Start the server:
    ```bash
    python app.py
    # OR
    uvicorn app:app --host 0.0.0.0 --port 7888 --reload
    ```
2.  Open your browser and navigate to `http://127.0.0.1:7888`.
3.  Enter your search themes (one per line) and click "Expand & Download CSV".

## CLI Usage

### Single Theme
Run the tool for a single search theme:
```bash
python main.py --theme "your brand name"
```

### Batch Processing
Run the tool for a list of themes (one per line in a text file):
```bash
python main.py --file themes.txt
```

### Output
Results are saved to `expansion_results.csv` by default. You can specify a custom output file:
```bash
python main.py --theme "nike" --output nike_results.csv
```

## Logic Overview

1.  **AI Expansion:** The tool uses Azure OpenAI to generate 10 related keywords for the input theme.
2.  **Database:** It maintains a SQLite database for metadata and a FAISS vector index for semantic search. (Initialized on first run).
3.  **Matching:**
    -   **Hard Match (Score 2):** Finds queries containing the keyword (ignoring spaces).
    -   **Vector Match (Score 1):** Finds semantically similar queries (Top 100, Similarity > 0.8) that also match a typo-tolerance check (Edit distance < 20% of length).
4.  **Ranking:** Results are aggregated and sorted by total Score.
