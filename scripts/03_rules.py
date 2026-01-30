"""03_rules.py
Adds rule-based flags to the fact table and splits into hard_issue vs warning.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import pandas as pd

from config import (
    FACT_ORDERS_FILE,
    HOLD_PO_CREATION_DAYS,
    HOLD_PREPARATION_DAYS,
    LOG_DIR,
    STUCK_IN_ERROR_DAYS,
    SUBSTATUSES_BY_STATUS,
    STATUSES,
)


def _ensure_dirs() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)


def _log(msg: str) -> None:
    _ensure_dirs()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with (LOG_DIR / "rules.log").open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def _to_dt(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def _norm_str(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip()


def compute_age_days(df: pd.DataFrame) -> pd.Series:
    upd_a = _to_dt(df["updated_at_a"]) if "updated_at_a" in df.columns else pd.Series(pd.NaT, index=df.index)
    upd_b = _to_dt(df["updated_at_b"]) if "updated_at_b" in df.columns else pd.Series(pd.NaT, index=df.index)
    upd = upd_a.fillna(upd_b)
    snap = pd.to_datetime(df["snapshot_date"], errors="coerce").dt.tz_localize("UTC")
    return (snap - upd).dt.total_seconds() / 86400.0


def unknown_status_substatus(df: pd.DataFrame) -> pd.Series:
    status = _norm_str(df.get("status_a", pd.Series(pd.NA, index=df.index)))
    sub = _norm_str(df.get("substatus_a", pd.Series(pd.NA, index=df.index)))

    known_status = status.isin(list(STATUSES))
    unknown_status = (~known_status) & status.notna()

    allowed_sets = status.map(lambda s: SUBSTATUSES_BY_STATUS.get(str(s), set()) if pd.notna(s) else set())

    bad_sub = []
    for st, su, allowed in zip(status.tolist(), sub.tolist(), allowed_sets.tolist()):
        if pd.isna(st) or pd.isna(su):
            bad_sub.append(False)
            continue
        bad_sub.append(str(su) not in set(allowed))

    return unknown_status | pd.Series(bad_sub, index=df.index)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")  # ignored; pipeline compatibility
    _ = parser.parse_args()

    _ensure_dirs()
    if not FACT_ORDERS_FILE.exists():
        _log(f"ERROR: fact table not found: {FACT_ORDERS_FILE}")
        return 1

    df = pd.read_parquet(FACT_ORDERS_FILE)
    _log(f"Loaded fact table rows={len(df)} cols={len(df.columns)}")

    status_a = _norm_str(df.get("status_a", pd.Series(pd.NA, index=df.index)))
    sub_a = _norm_str(df.get("substatus_a", pd.Series(pd.NA, index=df.index)))

    df["age_days"] = compute_age_days(df)

    df["inprep_on_hold_gt_30d"] = (status_a == "In Preparation") & (sub_a == "On Hold") & (df["age_days"] > float(HOLD_PREPARATION_DAYS))
    df["inpo_on_hold_gt_4d"] = (status_a == "In PO Creation") & (sub_a == "On Hold") & (df["age_days"] > float(HOLD_PO_CREATION_DAYS))
    df["inpo_error_stuck_gt_4d"] = (status_a == "In PO Creation") & (sub_a == "Error") & (df["age_days"] > float(STUCK_IN_ERROR_DAYS))
    df["completed_missing_po_copy"] = (status_a == "Completed") & (sub_a == "Missing PO Copy")
    df["completed_unequal_values"] = (status_a == "Completed") & (sub_a == "Unequal Values")
    df["cancelled_unsynchronized"] = (status_a == "Cancelled") & (sub_a == "Cancelled – Unsynchronized") & (df.get("exists_in_b", False) == False)

    df["cancelled_synchronized"] = (status_a == "Cancelled") & (sub_a == "Cancelled – Synchronized") & (df.get("exists_in_b", False) == True)
    df["unknown_status_or_substatus"] = unknown_status_substatus(df)

    hard_issue_cols = [
        "inprep_on_hold_gt_30d",
        "inpo_on_hold_gt_4d",
        "inpo_error_stuck_gt_4d",
        "completed_missing_po_copy",
        "completed_unequal_values",
        "cancelled_unsynchronized",
    ]
    warning_cols = [
        "cancelled_synchronized",
        "unknown_status_or_substatus",
        "status_mismatch",
        "substatus_mismatch",
        "value_mismatch",
    ]

    df["hard_issue"] = df[hard_issue_cols].any(axis=1)

    warning_missing = pd.Series(False, index=df.index)
    if "exists_in_a" in df.columns and "exists_in_b" in df.columns:
        warning_missing = ((df["exists_in_a"] == True) & (df["exists_in_b"] == False)) | ((df["exists_in_a"] == False) & (df["exists_in_b"] == True))

    df["warning"] = df[warning_cols].any(axis=1) | warning_missing
    df["has_issue"] = df["hard_issue"] | df["warning"]

    df.to_parquet(FACT_ORDERS_FILE, engine="pyarrow", index=False)
    _log(f"Wrote updated fact table: {FACT_ORDERS_FILE.name} rows={len(df)} cols={len(df.columns)}")
    _log(f"Hard issues flagged: {int(df['hard_issue'].sum())}")
    _log(f"Warnings flagged: {int(df['warning'].sum())}")
    _log(f"Any flagged: {int(df['has_issue'].sum())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
