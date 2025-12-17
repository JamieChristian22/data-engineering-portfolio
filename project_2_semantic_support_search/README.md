# Project 2 — Support Ticket Knowledge Base + Semantic Search (AI Data Engineering)

## Business problem
Support and operations teams waste time searching for the right knowledge base article to resolve issues.
We want a pipeline that:
- ingests tickets and KB articles
- cleans and redacts PII (emails, phone numbers)
- builds semantic search embeddings
- exposes search via CLI and optional API
- measures search quality with an offline evaluation set

## What’s included
- Data generation (tickets + KB + labeled “gold” matches)
- ETL: normalize text, dedupe, redact
- Embeddings: **TF‑IDF** (strong baseline) + cosine similarity
- Retrieval: top‑K recommendations with confidence scores
- Evaluation: Recall@K, MRR
- Optional FastAPI endpoint for `/search`

## Architecture
```mermaid
flowchart LR
A[Raw tickets + KB] --> B[Clean + Redact + Dedupe]
B --> C[TF-IDF Vectorizer]
C --> D[Vector Index + Metadata]
D --> E[CLI Search / API Search]
E --> F[Offline Evaluation]
```

## Run it
```bash
cd project_2_semantic_support_search
pip install -r requirements.txt

# 1) Generate data
python -m src.generate_data --n_tickets 1200 --n_kb 80

# 2) Build searchable index
python -m src.build_index

# 3) Search from the CLI
python -m src.search --query "password reset not working for mobile app" --top_k 5

# 4) Evaluate retrieval quality
python -m src.evaluate --top_k 5

# Optional API
uvicorn src.api:app --reload --port 8008
# then POST http://127.0.0.1:8008/search {"query":"vpn keeps disconnecting", "top_k":5}
```

## Outputs
- `indexes/vectorizer.joblib` + `indexes/tfidf_matrix.npz`
- `indexes/metadata.csv`
- `reports/eval.json` — Recall@K + MRR + examples


## API Key (optional)
If you set an API key, requests must include `X-API-Key`.

```bash
export API_KEY='my_key'
uvicorn src.api:app --host 0.0.0.0 --port 8000
curl -H "X-API-Key: my_key" -X POST http://localhost:8000/search -H "Content-Type: application/json" -d '{"query":"refund not received","top_k":5}'
```

## Benchmark
```bash
python -m src.benchmark
```
