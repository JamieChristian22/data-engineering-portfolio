from __future__ import annotations

import os
from pathlib import Path
from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel

from .search import build_or_load_index, semantic_search

app = FastAPI(title="Support KB Semantic Search", version="1.1")

API_KEY = os.getenv("API_KEY")

class SearchRequest(BaseModel):
    query: str
    top_k: int = 5

def _auth(x_api_key: str | None) -> None:
    # If API_KEY is set, require clients to send X-API-Key header
    if API_KEY and (not x_api_key or x_api_key != API_KEY):
        raise HTTPException(status_code=401, detail="Unauthorized")

@app.on_event("startup")
def _startup() -> None:
    # Load/build index once on startup
    root = Path(__file__).resolve().parents[1]
    build_or_load_index(root)

@app.post("/search")
def search(req: SearchRequest, x_api_key: str | None = Header(default=None, alias="X-API-Key")):
    _auth(x_api_key)
    if not req.query.strip():
        raise HTTPException(status_code=400, detail="Query must be non-empty")
    return {
        "query": req.query,
        "top_k": req.top_k,
        "results": semantic_search(req.query, top_k=req.top_k),
    }

@app.get("/health")
def health():
    return {"ok": True}
