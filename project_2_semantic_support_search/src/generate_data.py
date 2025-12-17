
from __future__ import annotations

import argparse
import random
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .logutil import get_logger

log = get_logger("p2.generate_data")

TOPICS = [
    ("password_reset", "Password reset and account recovery", [
        "reset link expired", "cannot receive reset email", "2FA code not arriving", "password rules", "locked out"
    ]),
    ("billing_refund", "Billing, invoices and refunds", [
        "duplicate charge", "refund status", "invoice download", "payment failed", "update card"
    ]),
    ("vpn_connect", "VPN connection troubleshooting", [
        "disconnects frequently", "cannot authenticate", "split tunneling", "dns issues", "slow speeds"
    ]),
    ("mobile_crash", "Mobile app crashes and performance", [
        "crashes on launch", "blank screen", "slow scrolling", "push notifications missing", "battery drain"
    ]),
    ("sso_setup", "Single sign-on (SSO) setup", [
        "SAML configuration", "Okta integration", "Azure AD", "certificate mismatch", "redirect loop"
    ]),
    ("data_export", "Data exports and reports", [
        "CSV export missing rows", "timezone mismatch", "scheduled report", "download fails", "columns incorrect"
    ]),
]

@dataclass(frozen=True)
class Config:
    n_tickets: int
    n_kb: int
    seed: int
    out_raw: Path

def _pii_email(rng: random.Random, i: int) -> str:
    dom = rng.choice(["gmail.com","outlook.com","company.com"])
    return f"user{i}@{dom}"

def _pii_phone(rng: random.Random) -> str:
    return f"({rng.randint(200,999)})-{rng.randint(200,999)}-{rng.randint(1000,9999)}"

def generate_kb(cfg: Config) -> pd.DataFrame:
    rng = random.Random(cfg.seed)
    rows = []
    kb_id = 0
    while len(rows) < cfg.n_kb:
        topic_key, title_base, issues = rng.choice(TOPICS)
        issue = rng.choice(issues)
        kb_id += 1
        title = f"{title_base}: {issue.title()}"
        body = (
            f"Summary: This article helps resolve '{issue}' for {title_base.lower()}.\n"
            f"Steps:\n"
            f"1) Verify prerequisites.\n"
            f"2) Reproduce the issue and capture logs.\n"
            f"3) Apply the fix for '{issue}'.\n"
            f"4) Confirm resolution and document outcome.\n"
            f"Notes: If the issue persists, escalate with timestamps and environment details."
        )
        rows.append({
            "kb_id": kb_id,
            "topic": topic_key,
            "title": title,
            "body": body,
        })
    return pd.DataFrame(rows)

def generate_tickets(cfg: Config, kb: pd.DataFrame) -> pd.DataFrame:
    rng = random.Random(cfg.seed + 7)
    rows = []
    for i in range(1, cfg.n_tickets + 1):
        kb_row = kb.sample(1, random_state=rng.randint(1, 10_000)).iloc[0]
        # Create a ticket that is semantically close to the KB topic, but noisy.
        noise = rng.choice([
            "happens after the latest update", "only on wifi", "started yesterday", "affects multiple users",
            "customer is angry", "high priority", "need resolution ASAP"
        ])
        pii = rng.choice([_pii_email(rng, i), _pii_phone(rng)])
        subject = f"{kb_row['topic'].replace('_',' ').title()} issue — {noise}"
        text = (
            f"Subject: {subject}\n"
            f"Customer reports: {kb_row['title'].split(':',1)[-1].strip().lower()}.\n"
            f"Details: {noise}. Contact: {pii}.\n"
            f"Environment: app_version={rng.choice(['1.8.2','1.9.0','2.0.1'])}, os={rng.choice(['iOS','Android','Windows','macOS'])}.\n"
            f"Observed behavior: {rng.choice(['error message','timeout','crash','unexpected logout','incorrect data'])}."
        )
        rows.append({
            "ticket_id": i,
            "kb_id_gold": int(kb_row["kb_id"]),
            "created_at": pd.Timestamp.utcnow().isoformat(),
            "subject": subject,
            "body": text,
            "priority": rng.choice(["P1","P2","P3"]),
        })
    return pd.DataFrame(rows)

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n_tickets", type=int, default=1200)
    ap.add_argument("--n_kb", type=int, default=80)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    out_raw = root / "data" / "raw"
    out_raw.mkdir(parents=True, exist_ok=True)

    cfg = Config(args.n_tickets, args.n_kb, args.seed, out_raw)
    kb = generate_kb(cfg)
    tickets = generate_tickets(cfg, kb)

    kb_path = out_raw / "kb_articles.csv"
    t_path = out_raw / "tickets.csv"
    kb.to_csv(kb_path, index=False)
    tickets.to_csv(t_path, index=False)

    log.info("Wrote KB: %s rows -> %s", len(kb), kb_path)
    log.info("Wrote tickets: %s rows -> %s", len(tickets), t_path)

if __name__ == "__main__":
    main()
