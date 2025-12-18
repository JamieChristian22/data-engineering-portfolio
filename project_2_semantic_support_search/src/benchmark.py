from __future__ import annotations

import time
from pathlib import Path
import numpy as np

from .search import build_or_load_index, semantic_search

def main() -> None:
    root = Path(__file__).resolve().parents[1]
    _ = build_or_load_index(root)
    queries = [
        "refund not received after return",
        "account locked after password reset",
        "how to change shipping address",
        "invoice missing for order",
        "unable to download receipt",
    ]
    t0 = time.perf_counter()
    latencies = []
    for q in queries * 50:
        s0 = time.perf_counter()
        _ = semantic_search(q, top_k=5)
        latencies.append((time.perf_counter() - s0) * 1000)
    total = (time.perf_counter() - t0) * 1000
    lat = np.array(latencies)
    print(f"Ran {len(latencies)} searches in {total:.1f} ms")
    print(f"Latency ms: p50={np.percentile(lat,50):.2f} p95={np.percentile(lat,95):.2f} p99={np.percentile(lat,99):.2f}")

if __name__ == "__main__":
    main()
