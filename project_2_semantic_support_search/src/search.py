from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import joblib
import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from .clean import clean, Paths
from .logutil import get_logger

log = get_logger("p2.search")

@dataclass(frozen=True)
class Index:
    vectorizer: TfidfVectorizer
    matrix: sparse.csr_matrix
    meta: pd.DataFrame

_INDEX: Index | None = None

def build_or_load_index(root: Path) -> Index:
    """Load TF-IDF index from disk; build it if missing."""
    global _INDEX
    if _INDEX is not None:
        return _INDEX

    indexes = root / "indexes"
    vec_path = indexes / "vectorizer.joblib"
    mat_path = indexes / "tfidf_matrix.npz"
    meta_path = indexes / "metadata.csv"

    if vec_path.exists() and mat_path.exists() and meta_path.exists():
        vectorizer = joblib.load(vec_path)
        matrix = sparse.load_npz(mat_path)
        meta = pd.read_csv(meta_path)
        _INDEX = Index(vectorizer=vectorizer, matrix=matrix, meta=meta)
        return _INDEX

    # Build from cleaned tickets + KB docs
    log.info("Index artifacts missing; building index into %s", indexes)
    indexes.mkdir(parents=True, exist_ok=True)

    paths = Paths.from_root(root)
    clean(paths)

    kb = pd.read_csv(paths.kb_out)
    # Combine title + body for retrieval
    kb["doc"] = kb["title"].fillna("") + "\n" + kb["body"].fillna("")
    docs = kb["doc"].astype(str).tolist()

    vectorizer = TfidfVectorizer(stop_words="english", max_features=5000, ngram_range=(1,2))
    matrix = vectorizer.fit_transform(docs)

    joblib.dump(vectorizer, vec_path)
    sparse.save_npz(mat_path, matrix)
    kb[["kb_id","title","product","body"]].to_csv(meta_path, index=False)

    _INDEX = Index(vectorizer=vectorizer, matrix=matrix, meta=kb[["kb_id","title","product","body"]])
    log.info("Built index: docs=%s features=%s", matrix.shape[0], matrix.shape[1])
    return _INDEX

def semantic_search(query: str, top_k: int = 5) -> list[dict]:
    root = Path(__file__).resolve().parents[1]
    idx = build_or_load_index(root)
    qv = idx.vectorizer.transform([query])
    sims = cosine_similarity(qv, idx.matrix).ravel()
    top = np.argsort(-sims)[:top_k]
    out = []
    for i in top:
        row = idx.meta.iloc[int(i)]
        out.append({
            "kb_id": int(row["kb_id"]),
            "title": str(row["title"]),
            "product": str(row.get("product","")),
            "score": float(sims[int(i)]),
            "snippet": str(row["body"])[:240] + ("..." if len(str(row["body"])) > 240 else ""),
        })
    return out

def search(query: str, top_k: int = 5) -> list[dict]:
    # Backwards-compatible alias
    return semantic_search(query, top_k=top_k)

def main() -> None:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--query", type=str, required=True)
    ap.add_argument("--top_k", type=int, default=5)
    args = ap.parse_args()
    results = semantic_search(args.query, top_k=args.top_k)
    for r in results:
        print(f"[{r['score']:.3f}] ({r['product']}) {r['title']}")
        print(f"  {r['snippet']}\n")

if __name__ == "__main__":
    main()
