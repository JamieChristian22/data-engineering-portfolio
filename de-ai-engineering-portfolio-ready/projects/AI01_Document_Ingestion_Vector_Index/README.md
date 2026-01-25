# AI01 — Document Ingestion + Vector Index (SQLite)

## Scenario
You need a **RAG-ready** ingestion pipeline:
- ingest documents (txt/markdown)
- chunk them
- compute embeddings
- store metadata + vectors
- provide fast search + retrieval

This project uses **HashingVectorizer** (no external model downloads) and stores vectors in SQLite.

## Run
```bash
python src/seed_documents.py
python src/build_index.py
python src/search.py --query "refund policy" --topk 5
```

## Outputs
- `index/vector_index.sqlite` (tables: documents, chunks, vectors)
- `outputs/search_results.json`


---
## Executive Summary (Recruiter-Friendly)

**Problem:** Simulated real-world business pipeline challenge  
**Solution:** Designed and implemented a production-style data pipeline  
**Skills Demonstrated:** Data modeling, ETL/ELT, validation, automation, analytics thinking  
**Artifacts Produced:** Reproducible code, real outputs, reports, metrics  
**Interview Talking Points:**
- Why this architecture was chosen
- How idempotency / data quality / monitoring is handled
- How this would scale in production
- Tradeoffs considered in design

## Architecture Diagram
See `architecture.mmd` for a Mermaid-renderable pipeline diagram.

---
