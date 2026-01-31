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


def _norm_one(x) -> str | None:
    if x is None or (isinstance(x, float) and pd.isna(x)) or pd.isna(x):
        return None
    s = str(x).strip()
    s = s.replace("–", "-").replace("—", "-")
    s = " ".join(s.split())
    return s.lower()


def _norm_str(series: pd.Series) -> pd.Series:
    s = series.astype("string").str.strip()
    s = s.str.replace("–", "-", regex=False).str.replace("—", "-", regex=False)
    s = s.str.replace(r"\s+", " ", regex=True)
    s = s.str.lower()
    return s


def compute_age_days(df: pd.DataFrame) -> pd.Series:
    upd_a = _to_dt(df["updated_at_a"]) if "updated_at_a" in df.columns else pd.Series(pd.NaT, index=df.index)
    upd_b = _to_dt(df["updated_at_b"]) if "updated_at_b" in df.columns else pd.Series(pd.NaT, index=df.index)
    upd = upd_a.fillna(upd_b)
    snap = pd.to_datetime(df["snapshot_date"], errors="coerce").dt.tz_localize("UTC")
    return (snap - upd).dt.total_seconds() / 86400.0


def unknown_status_substatus(df: pd.DataFrame) -> pd.Series:
    status = _norm_str(df.get("status_a", pd.Series(pd.NA, index=df.index)))
    sub = _norm_str(df.get("substatus_a", pd.Series(pd.NA, index=df.index)))

    # normalize config values defensively (so you don't have to edit config.py)
    statuses_norm = set(filter(None, (_norm_one(s) for s in STATUSES)))

    sub_by_status_norm: dict[str, set[str]] = {}
    for k, vals in SUBSTATUSES_BY_STATUS.items():
        kn = _norm_one(k)
        if not kn:
            continue
        vs = set(filter(None, (_norm_one(v) for v in (vals or []))))
        sub_by_status_norm[kn] = vs

    known_status = status.isin(list(statuses_norm))
    unknown_status = (~known_status) & status.notna()

    allowed_sets = status.map(lambda s: sub_by_status_norm.get(str(s), set()) if pd.notna(s) else set())

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

    # IMPORTANT: strings are now normalized to lowercase + '-' dash
    df["inprep_on_hold_gt_30d"] = (status_a == "in preparation") & (sub_a == "on hold") & (
        df["age_days"] > float(HOLD_PREPARATION_DAYS)
    )
    df["inpo_on_hold_gt_4d"] = (status_a == "in po creation") & (sub_a == "on hold") & (
        df["age_days"] > float(HOLD_PO_CREATION_DAYS)
    )
    df["inpo_error_stuck_gt_4d"] = (status_a == "in po creation") & (sub_a == "error") & (
        df["age_days"] > float(STUCK_IN_ERROR_DAYS)
    )
    df["completed_missing_po_copy"] = (status_a == "completed") & (sub_a == "missing po copy")
    df["completed_unequal_values"] = (status_a == "completed") & (sub_a == "unequal values")
    df["cancelled_unsynchronized"] = (status_a == "cancelled") & (sub_a == "cancelled - unsynchronized") & (
        df.get("exists_in_b", False) == False
    )

    df["cancelled_synchronized"] = (status_a == "cancelled") & (sub_a == "cancelled - synchronized") & (
        df.get("exists_in_b", False) == True
    )
    df["unknown_status_or_substatus"] = unknown_status_substatus(df)

    hard_issue_cols = [
        "inprep_on_hold_gt_30d",
        "inpo_on_hold_gt_4d",
        "inpo_error_stuck_gt_4d",
        "completed_missing_po_copy",
        "completed_unequal_values",
        "cancelled_unsynchronized",
    ]

    df["hard_issue"] = df[hard_issue_cols].any(axis=1)

    # -------- FIXED WARNING LOGIC --------
    exists_a = (
        df["exists_in_a"].fillna(False).astype(bool)
        if "exists_in_a" in df.columns
        else pd.Series(False, index=df.index)
    )
    exists_b = (
        df["exists_in_b"].fillna(False).astype(bool)
        if "exists_in_b" in df.columns
        else pd.Series(False, index=df.index)
    )

    both_exist = exists_a & exists_b
    missing_one_side = exists_a ^ exists_b

    warning_cols_core = [
        "cancelled_synchronized",
        "unknown_status_or_substatus",
        "status_mismatch",
        "substatus_mismatch",
        "value_mismatch",
    ]
    core_any = df.reindex(columns=warning_cols_core, fill_value=False).any(axis=1)

    df["warning"] = missing_one_side | (both_exist & core_any)
    df["has_issue"] = df["hard_issue"] | df["warning"]
    # ------------------------------------

    df.to_parquet(FACT_ORDERS_FILE, engine="pyarrow", index=False)
    _log(f"Wrote updated fact table: {FACT_ORDERS_FILE.name} rows={len(df)} cols={len(df.columns)}")
    _log(f"Hard issues flagged: {int(df['hard_issue'].sum())}")
    _log(f"Warnings flagged: {int(df['warning'].sum())}")
    _log(f"Any flagged: {int(df['has_issue'].sum())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
