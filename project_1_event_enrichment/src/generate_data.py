
from __future__ import annotations

import argparse
import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import random
import pandas as pd

from .logutil import get_logger

log = get_logger("p1.generate_data")


EVENT_TYPES = ["page_view", "product_view", "search", "add_to_cart", "checkout", "purchase"]
DEVICES = ["mobile", "desktop", "tablet"]
REFERRERS = ["direct", "google", "facebook", "tiktok", "email", "affiliate"]
COUNTRIES = ["US", "CA", "GB", "DE", "AU"]


@dataclass(frozen=True)
class Config:
    n_users: int
    days: int
    out_dir: Path
    seed: int


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def simulate_events(cfg: Config) -> pd.DataFrame:
    """
    Generates realistic clickstream-like events with session boundaries.
    Each session ends when inactivity exceeds 30 minutes.
    """
    rng = random.Random(cfg.seed)
    start = datetime.now().replace(hour=9, minute=0, second=0, microsecond=0) - timedelta(days=cfg.days)

    rows = []
    session_id = 0
    for user_id in range(1, cfg.n_users + 1):
        # baseline shopping intent varies per user
        user_intent = rng.uniform(-1.2, 1.2)
        n_sessions = rng.randint(max(2, cfg.days // 2), cfg.days * 2)
        t = start + timedelta(minutes=rng.randint(0, 8 * 60))

        for _ in range(n_sessions):
            session_id += 1
            device = rng.choices(DEVICES, weights=[0.55, 0.38, 0.07])[0]
            ref = rng.choices(REFERRERS, weights=[0.28, 0.34, 0.12, 0.09, 0.12, 0.05])[0]
            country = rng.choices(COUNTRIES, weights=[0.70, 0.08, 0.08, 0.06, 0.08])[0]

            depth = rng.randint(3, 25)
            # session velocity (seconds between events)
            base_gap = rng.uniform(6, 40) if device != "desktop" else rng.uniform(4, 30)

            # convert probability driven by behavior + user intent + referrer/device effects
            ref_bonus = {"email": 0.5, "affiliate": 0.25, "direct": 0.15, "google": 0.1, "facebook": -0.05, "tiktok": -0.10}[ref]
            device_bonus = {"desktop": 0.25, "mobile": -0.05, "tablet": 0.0}[device]
            depth_bonus = (depth - 10) / 20.0

            p_purchase = _sigmoid(-1.1 + user_intent + ref_bonus + device_bonus + depth_bonus)
            will_purchase = rng.random() < p_purchase

            cart_events = 0
            checkout_events = 0

            event_time = t
            for i in range(depth):
                # event type progression
                if i == 0:
                    et = "page_view"
                else:
                    # increase likelihood of cart/checkout as depth grows
                    w = [0.40, 0.32, 0.18, 0.07, 0.02, 0.01]
                    if i > depth * 0.45:
                        w = [0.32, 0.28, 0.14, 0.16, 0.07, 0.03]
                    et = rng.choices(EVENT_TYPES, weights=w)[0]

                if et == "add_to_cart":
                    cart_events += 1
                if et == "checkout":
                    checkout_events += 1

                rows.append({
                    "event_id": f"e_{session_id}_{i}",
                    "user_id": user_id,
                    "session_id": session_id,
                    "event_time": event_time.isoformat(timespec="seconds"),
                    "event_type": et,
                    "device": device,
                    "referrer": ref,
                    "country": country,
                    "event_index": i,
                })
                event_time += timedelta(seconds=abs(rng.gauss(base_gap, base_gap * 0.35)))

            # ensure a plausible funnel if session purchases
            if will_purchase:
                rows.append({
                    "event_id": f"e_{session_id}_checkout",
                    "user_id": user_id,
                    "session_id": session_id,
                    "event_time": (event_time + timedelta(seconds=15)).isoformat(timespec="seconds"),
                    "event_type": "checkout",
                    "device": device,
                    "referrer": ref,
                    "country": country,
                    "event_index": depth,
                })
                rows.append({
                    "event_id": f"e_{session_id}_purchase",
                    "user_id": user_id,
                    "session_id": session_id,
                    "event_time": (event_time + timedelta(seconds=55)).isoformat(timespec="seconds"),
                    "event_type": "purchase",
                    "device": device,
                    "referrer": ref,
                    "country": country,
                    "event_index": depth + 1,
                })

            # next session start: hours to days later
            t = event_time + timedelta(hours=rng.uniform(2, 20))

    df = pd.DataFrame(rows).sort_values(["event_time", "event_id"]).reset_index(drop=True)
    log.info("Generated %s events for %s users across ~%s days", len(df), cfg.n_users, cfg.days)
    return df


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n_users", type=int, default=1500)
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    out_dir = Path(__file__).resolve().parents[1] / "data" / "raw"
    out_dir.mkdir(parents=True, exist_ok=True)

    df = simulate_events(Config(args.n_users, args.days, out_dir, args.seed))
    out_path = out_dir / "events.csv"
    df.to_csv(out_path, index=False)
    log.info("Wrote %s", out_path)


if __name__ == "__main__":
    main()
