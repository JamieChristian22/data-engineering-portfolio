from __future__ import annotations

from dataclasses import dataclass
import pandas as pd

@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    message: str

def require_columns(df: pd.DataFrame, required: list[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

def require_nonnull(df: pd.DataFrame, cols: list[str]) -> None:
    bad = [c for c in cols if df[c].isna().any()]
    if bad:
        raise ValueError(f"Nulls found in required non-null columns: {bad}")

def require_unique(df: pd.DataFrame, col: str) -> None:
    if df[col].duplicated().any():
        raise ValueError(f"Duplicate values found in unique column: {col}")
