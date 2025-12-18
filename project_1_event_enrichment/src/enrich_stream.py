
from __future__ import annotations

import argparse
from pathlib import Path
import time

import joblib
import pandas as pd

from .features import build_session_features
from .logutil import get_logger
from .validate import require_columns, require_nonnull

log = get_logger("p1.enrich_stream")


def enrich_events(events: pd.DataFrame, model_path: Path) -> pd.DataFrame:
    """
    Enrich events with session-level propensity scores.
    """
    pipe = joblib.load(model_path)
    feats = build_session_features(events)

    target_cols_drop = ["label_purchase", "start_time", "end_time"]
    X = feats.drop(columns=[c for c in target_cols_drop if c in feats.columns])

    scores = pipe.predict_proba(X.drop(columns=["session_id", "user_id"]))[:, 1]
    session_scores = pd.DataFrame({"session_id": feats["session_id"], "propensity_score": scores})

    enriched = events.merge(session_scores, on="session_id", how="left")
    return enriched


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch_size", type=int, default=5000)
    ap.add_argument("--sleep_ms", type=int, default=0, help="Optional delay to emulate streaming")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    raw_path = root / "data" / "raw" / "events.csv"
    model_path = root / "models" / "propensity_model.joblib"
    out_path = root / "data" / "processed" / "enriched_events.csv"

    if not raw_path.exists():
        raise FileNotFoundError(f"Missing {raw_path}. Run generate_data first.")
    if not model_path.exists():
        raise FileNotFoundError(f"Missing {model_path}. Run train_model first.")

    events = pd.read_csv(raw_path)
    require_columns(events, ["event_id","session_id","user_id","event_time","event_type"])
    require_nonnull(events, ["event_id","session_id","user_id","event_time","event_type"])
    events["event_time"] = pd.to_datetime(events["event_time"])

    batches = []
    n = len(events)
    for start in range(0, n, args.batch_size):
        end = min(start + args.batch_size, n)
        batch = events.iloc[start:end].copy()
        enriched = enrich_events(batch, model_path)
        batches.append(enriched)
        log.info("Processed batch %s-%s (%s rows)", start, end, len(batch))
        if args.sleep_ms:
            time.sleep(args.sleep_ms / 1000.0)

    out = pd.concat(batches, ignore_index=True).sort_values(["event_time","event_id"])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)
    log.info("Wrote enriched events -> %s (%s rows)", out_path, len(out))


if __name__ == "__main__":
    main()
