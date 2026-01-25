from __future__ import annotations
import argparse, json, sqlite3
from pathlib import Path
import numpy as np
from sklearn.feature_extraction.text import HashingVectorizer
from shared.utils import ensure_dir, utcnow_iso

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--query", required=True)
    ap.add_argument("--topk", type=int, default=5)
    args = ap.parse_args()

    cfg = json.loads(Path("config/config.json").read_text())
    index_path = Path(cfg["index_path"])
    out_dir = ensure_dir(Path("outputs"))

    con = sqlite3.connect(str(index_path))
    cur = con.cursor()

    vec = HashingVectorizer(n_features=2**12, alternate_sign=False, norm=None)
    qv = vec.transform([args.query]).toarray().astype(np.float32).ravel()
    qn = float(np.linalg.norm(qv) + 1e-9)

    cur.execute("""SELECT v.chunk_id, v.dim, v.vec, v.l2norm, c.text, d.filename
                   FROM vectors v
                   JOIN chunks c ON c.chunk_id = v.chunk_id
                   JOIN documents d ON d.doc_id = c.doc_id""")
    rows = cur.fetchall()

    scored = []
    for chunk_id, dim, blob, l2, text, filename in rows:
        dv = np.frombuffer(blob, dtype=np.float32, count=dim)
        score = float(np.dot(qv, dv) / (qn * float(l2)))
        scored.append((score, chunk_id, filename, text))

    scored.sort(reverse=True, key=lambda x: x[0])
    top = [{"score": s, "chunk_id": cid, "filename": fn, "text": txt[:600]} for s, cid, fn, txt in scored[:args.topk]]

    result = {"run_at": utcnow_iso(), "query": args.query, "topk": args.topk, "results": top}
    (out_dir / "search_results.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))
    con.close()

if __name__ == "__main__":
    main()
