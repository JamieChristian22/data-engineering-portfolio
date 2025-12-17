
from __future__ import annotations

import argparse
from pathlib import Path
import joblib
import numpy as np
import pandas as pd
from scipy import sparse
from sklearn.feature_extraction.text import TfidfVectorizer

from .clean import clean, Paths
from .logutil import get_logger

log = get_logger("p2.build_index")

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max_features", type=int, default=50000)
    ap.add_argument("--ngram_max", type=int, default=2)
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    raw = root / "data" / "raw"
    processed = root / "data" / "processed"
    indexes = root / "indexes"

    tickets_raw = raw / "tickets.csv"
    kb_raw = raw / "kb_articles.csv"
    if not tickets_raw.exists() or not kb_raw.exists():
        raise FileNotFoundError("Missing raw data. Run: python -m src.generate_data")

    tickets, kb = clean(Paths(
        tickets_raw=tickets_raw,
        kb_raw=kb_raw,
        out_tickets=processed/"tickets_clean.csv",
        out_kb=processed/"kb_clean.csv",
    ))

    vectorizer = TfidfVectorizer(
        max_features=args.max_features,
        ngram_range=(1, args.ngram_max),
        min_df=2,
        stop_words="english",
    )

    kb_text = kb["doc_text"].astype(str).tolist()
    X_kb = vectorizer.fit_transform(kb_text)  # (n_kb, vocab)

    indexes.mkdir(parents=True, exist_ok=True)
    joblib.dump(vectorizer, indexes/"vectorizer.joblib")
    sparse.save_npz(indexes/"tfidf_matrix.npz", X_kb)

    # metadata for retrieval
    meta = kb[["kb_id","topic","title"]].copy()
    meta.to_csv(indexes/"metadata.csv", index=False)

    log.info("Index built: kb=%s vocab=%s -> %s", X_kb.shape[0], X_kb.shape[1], indexes)

if __name__ == "__main__":
    main()
