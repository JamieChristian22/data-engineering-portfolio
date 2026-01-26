from __future__ import annotations
import json, sqlite3, math
from pathlib import Path
from datetime import datetime, timezone
from sklearn.feature_extraction.text import HashingVectorizer
import numpy as np
from shared.utils import ensure_dir, utcnow_iso

def chunk_text(text: str, chunk_size: int, overlap: int) -> list[str]:
    text = text.replace("\r\n", "\n")
    words = text.split()
    chunks = []
    i = 0
    while i < len(words):
        chunk = words[i:i+chunk_size]
        chunks.append(" ".join(chunk))
        i += max(1, chunk_size - overlap)
    return chunks

DDL = [
"""CREATE TABLE IF NOT EXISTS documents(
  doc_id INTEGER PRIMARY KEY AUTOINCREMENT,
  filename TEXT UNIQUE,
  added_at TEXT
);""",
"""CREATE TABLE IF NOT EXISTS chunks(
  chunk_id INTEGER PRIMARY KEY AUTOINCREMENT,
  doc_id INTEGER,
  chunk_index INTEGER,
  text TEXT,
  FOREIGN KEY(doc_id) REFERENCES documents(doc_id)
);""",
"""CREATE TABLE IF NOT EXISTS vectors(
  chunk_id INTEGER PRIMARY KEY,
  dim INTEGER,
  vec BLOB,
  l2norm REAL,
  FOREIGN KEY(chunk_id) REFERENCES chunks(chunk_id)
);"""
]

def main() -> None:
    cfg = json.loads(Path("config/config.json").read_text())
    docs_dir = Path(cfg["docs_dir"])
    index_path = Path(cfg["index_path"])
    ensure_dir(index_path.parent)

    con = sqlite3.connect(str(index_path))
    cur = con.cursor()
    for ddl in DDL:
        cur.execute(ddl)

    vectorizer = HashingVectorizer(n_features=2**12, alternate_sign=False, norm=None)

    # Ingest docs
    files = sorted(docs_dir.glob("*.md")) + sorted(docs_dir.glob("*.txt"))
    for f in files:
        cur.execute("INSERT OR IGNORE INTO documents(filename, added_at) VALUES (?,?)", (f.name, utcnow_iso()))
        con.commit()

    # Build chunks + vectors
    cur.execute("SELECT doc_id, filename FROM documents")
    docs = cur.fetchall()

    total_chunks = 0
    for doc_id, filename in docs:
        text = (docs_dir / filename).read_text(encoding="utf-8")
        chunks = chunk_text(text, int(cfg["chunk_size"]), int(cfg["chunk_overlap"]))
        for idx, ch in enumerate(chunks):
            cur.execute("INSERT INTO chunks(doc_id, chunk_index, text) VALUES (?,?,?)", (doc_id, idx, ch))
        con.commit()

    cur.execute("SELECT chunk_id, text FROM chunks")
    all_chunks = cur.fetchall()
    texts = [t for _, t in all_chunks]
    X = vectorizer.transform(texts)  # sparse
    X = X.astype(np.float32)

    # Store as bytes (dense) for simple cosine search
    for (chunk_id, _), row in zip(all_chunks, X):
        dense = row.toarray().ravel()
        norm = float(np.linalg.norm(dense) + 1e-9)
        cur.execute("INSERT OR REPLACE INTO vectors(chunk_id, dim, vec, l2norm) VALUES (?,?,?,?)",
                    (chunk_id, dense.size, dense.tobytes(), norm))
    con.commit()

    total_chunks = len(all_chunks)
    print(json.dumps({"docs": len(files), "chunks": total_chunks, "dim": 2**12, "index": str(index_path)}, indent=2))
    con.close()

if __name__ == "__main__":
    main()
