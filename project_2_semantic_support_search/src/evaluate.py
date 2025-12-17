
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from .search import search
from .logutil import get_logger

log = get_logger("p2.evaluate")

def recall_at_k(results: list[int], gold: int, k: int) -> int:
    return int(gold in results[:k])

def mrr(results: list[int], gold: int) -> float:
    for i, kb_id in enumerate(results, start=1):
        if kb_id == gold:
            return 1.0 / i
    return 0.0

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top_k", type=int, default=5)
    ap.add_argument("--max_examples", type=int, default=8)
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    tickets_path = root / "data" / "raw" / "tickets.csv"
    if not tickets_path.exists():
        raise FileNotFoundError("Missing tickets. Run: python -m src.generate_data and python -m src.build_index")

    tickets = pd.read_csv(tickets_path)
    recalls = []
    mrrs = []
    examples = []

    for _, row in tickets.sample(min(len(tickets), 400), random_state=42).iterrows():
        q = str(row["subject"]) + " " + str(row["body"])
        gold = int(row["kb_id_gold"])
        res = search(q, top_k=args.top_k)
        ranked_ids = [int(r["kb_id"]) for r in res]
        recalls.append(recall_at_k(ranked_ids, gold, args.top_k))
        mrrs.append(mrr(ranked_ids, gold))

        if len(examples) < args.max_examples:
            examples.append({
                "query_subject": row["subject"],
                "gold_kb_id": gold,
                "predicted_topk": ranked_ids,
                "top_scores": [round(float(r["score"]), 4) for r in res],
            })

    out = {
        "top_k": args.top_k,
        "n_eval": len(recalls),
        "recall_at_k": round(sum(recalls) / max(1, len(recalls)), 4),
        "mrr": round(sum(mrrs) / max(1, len(mrrs)), 4),
        "examples": examples,
    }

    out_path = root / "reports" / "eval.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2))
    log.info("Wrote %s", out_path)
    log.info("Recall@%s=%.4f | MRR=%.4f (n=%s)", args.top_k, out["recall_at_k"], out["mrr"], out["n_eval"])

if __name__ == "__main__":
    main()
