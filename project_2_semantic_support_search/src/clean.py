
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
import pandas as pd

from .logutil import get_logger

log = get_logger("p2.clean")

EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
PHONE_RE = re.compile(r"\b\(?\d{3}\)?[-.\s]\d{3}[-.\s]\d{4}\b")

@dataclass(frozen=True)
class Paths:
    tickets_raw: Path
    kb_raw: Path
    out_tickets: Path
    out_kb: Path

def redact_pii(text: str) -> str:
    text = EMAIL_RE.sub("[REDACTED_EMAIL]", text)
    text = PHONE_RE.sub("[REDACTED_PHONE]", text)
    return text

def normalize_text(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s\-\_:/\.]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def clean(paths: Paths) -> tuple[pd.DataFrame, pd.DataFrame]:
    tickets = pd.read_csv(paths.tickets_raw)
    kb = pd.read_csv(paths.kb_raw)

    tickets["body_redacted"] = tickets["body"].astype(str).map(redact_pii).map(normalize_text)
    tickets["subject_norm"] = tickets["subject"].astype(str).map(normalize_text)
    tickets["doc_text"] = (tickets["subject_norm"] + " " + tickets["body_redacted"]).str.strip()

    kb["title_norm"] = kb["title"].astype(str).map(normalize_text)
    kb["body_norm"] = kb["body"].astype(str).map(normalize_text)
    kb["doc_text"] = (kb["title_norm"] + " " + kb["body_norm"]).str.strip()

    # Dedupe exact duplicates (rare but realistic)
    tickets = tickets.drop_duplicates(subset=["doc_text"]).reset_index(drop=True)
    kb = kb.drop_duplicates(subset=["doc_text"]).reset_index(drop=True)

    paths.out_tickets.parent.mkdir(parents=True, exist_ok=True)
    tickets.to_csv(paths.out_tickets, index=False)
    kb.to_csv(paths.out_kb, index=False)

    log.info("Cleaned tickets=%s -> %s", len(tickets), paths.out_tickets)
    log.info("Cleaned kb=%s -> %s", len(kb), paths.out_kb)
    return tickets, kb
