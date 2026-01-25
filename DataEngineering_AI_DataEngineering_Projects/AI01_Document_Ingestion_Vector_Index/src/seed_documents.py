from __future__ import annotations
from pathlib import Path
from shared.utils import ensure_dir

DOCS = {
  "refund_policy.md": """# Refund Policy
We offer refunds within 30 days for unused items.
Chargebacks are investigated and may lead to account suspension.
Refunds are processed to the original payment method within 5-10 business days.
""",
  "device_telemetry_spec.md": """# Telemetry Spec
Devices emit events with temperature_c, vibration_rms, battery_pct, lat, lon and timestamps.
Late arrivals occur when devices are offline and upload buffered events later.
""",
  "billing_faq.md": """# Billing FAQ
Plans: basic ($19), pro ($49), enterprise ($199).
MRR is monthly recurring revenue.
Trials last 14 days and convert automatically unless canceled.
"""
}

def main() -> None:
    docs_dir = ensure_dir(Path("data/docs"))
    for name, content in DOCS.items():
        (docs_dir / name).write_text(content, encoding="utf-8")
    print(f"Seeded {len(DOCS)} documents in {docs_dir.resolve()}")

if __name__ == "__main__":
    main()
