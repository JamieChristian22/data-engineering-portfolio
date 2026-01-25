from __future__ import annotations
import argparse, json, time
from pathlib import Path
from datetime import datetime, timedelta, timezone
import numpy as np
from shared.utils import ensure_dir, utcnow_iso

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=int, default=1)
    ap.add_argument("--devices", type=int, default=50)
    args = ap.parse_args()

    rng = np.random.default_rng(7)
    landing = ensure_dir(Path("data/landing"))

    start = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    total_seconds = args.minutes * 60

    device_ids = [f"dev-{i:04d}" for i in range(1, args.devices+1)]

    for sec in range(total_seconds):
        now = start + timedelta(seconds=sec)
        # Every 10 seconds, write a file containing a small batch
        if sec % 10 == 0:
            fname = landing / f"telemetry_{now.strftime('%Y%m%dT%H%M%SZ')}.jsonl"
            rows = []
            for _ in range(rng.integers(30, 60)):
                dev = rng.choice(device_ids)
                # event time can be late by up to 120s
                event_time = now - timedelta(seconds=int(rng.integers(0, 120)))
                rows.append({
                    "event_id": f"{dev}-{now.strftime('%Y%m%d%H%M%S')}-{int(rng.integers(1,1_000_000))}",
                    "device_id": dev,
                    "event_time": event_time.isoformat(),
                    "ingest_time": utcnow_iso(),
                    "temperature_c": float(np.round(rng.normal(35, 4), 2)),
                    "vibration_rms": float(np.round(abs(rng.normal(0.8, 0.35)), 3)),
                    "battery_pct": int(np.clip(rng.normal(72, 18), 0, 100)),
                    "lat": float(np.round(rng.uniform(25.0, 49.0), 6)),
                    "lon": float(np.round(rng.uniform(-124.0, -67.0), 6)),
                })
            with fname.open("w", encoding="utf-8") as f:
                for r in rows:
                    f.write(json.dumps(r) + "\n")
            print(f"Wrote {fname.name} ({len(rows)} events)")
        time.sleep(0.01)

if __name__ == "__main__":
    main()
