
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import pandas as pd

from .logutil import get_logger

log = get_logger("p1.features")


@dataclass(frozen=True)
class Paths:
    raw_events: Path
    out_features: Path


def build_session_features(events: pd.DataFrame) -> pd.DataFrame:
    """
    Session-level features suitable for propensity scoring.
    Label: did the session include a purchase event?
    """
    events = events.copy()
    events["event_time"] = pd.to_datetime(events["event_time"], errors="raise")

    # session start/end + duration
    agg = events.groupby("session_id").agg(
        user_id=("user_id", "first"),
        device=("device", "first"),
        referrer=("referrer", "first"),
        country=("country", "first"),
        n_events=("event_id", "count"),
        n_page_views=("event_type", lambda s: (s == "page_view").sum()),
        n_product_views=("event_type", lambda s: (s == "product_view").sum()),
        n_search=("event_type", lambda s: (s == "search").sum()),
        n_add_to_cart=("event_type", lambda s: (s == "add_to_cart").sum()),
        n_checkout=("event_type", lambda s: (s == "checkout").sum()),
        n_purchase=("event_type", lambda s: (s == "purchase").sum()),
        start_time=("event_time", "min"),
        end_time=("event_time", "max"),
    ).reset_index()

    agg["session_seconds"] = (agg["end_time"] - agg["start_time"]).dt.total_seconds().clip(lower=1)
    agg["events_per_min"] = (agg["n_events"] / (agg["session_seconds"] / 60.0)).clip(lower=0)
    agg["cart_rate"] = (agg["n_add_to_cart"] / agg["n_events"]).fillna(0.0)
    agg["checkout_rate"] = (agg["n_checkout"] / agg["n_events"]).fillna(0.0)
    agg["product_view_rate"] = (agg["n_product_views"] / agg["n_events"]).fillna(0.0)

    agg["label_purchase"] = (agg["n_purchase"] > 0).astype(int)
    return agg


def run(paths: Paths) -> pd.DataFrame:
    events = pd.read_csv(paths.raw_events)
    features = build_session_features(events)
    paths.out_features.parent.mkdir(parents=True, exist_ok=True)
    features.to_csv(paths.out_features, index=False)
    log.info("Wrote features: %s rows -> %s", len(features), paths.out_features)
    return features
