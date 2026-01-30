"""04_aggregate.py
Exports small CSVs for Power BI.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import pandas as pd

from config import (
    FACT_ORDERS_FILE,
    LOG_DIR,
    PBI_AGING_BUCKETS,
    PBI_DAILY_KPIS,
    PBI_ISSUE_CASES,
    PBI_STATUS_DISTRIBUTION,
    PBI_DIR,
)


def _ensure_dirs() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    PBI_DIR.mkdir(parents=True, exist_ok=True)


def _log(msg: str) -> None:
    _ensure_dirs()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with (LOG_DIR / "aggregate.log").open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def build_daily_kpis(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("snapshot_date", dropna=False)

    def _sum_bool(x: pd.Series) -> int:
        return int(x.fillna(False).astype(bool).sum())

    out = pd.DataFrame({
        "snapshot_date": g.size().index,
        "rows": g.size().values,
        "orders_a": g["exists_in_a"].apply(_sum_bool).values if "exists_in_a" in df.columns else 0,
        "orders_b": g["exists_in_b"].apply(_sum_bool).values if "exists_in_b" in df.columns else 0,
        "missing_in_b": g.apply(lambda x: int(((x.get("exists_in_a", False) == True) & (x.get("exists_in_b", False) == False)).sum())).values,
        "missing_in_a": g.apply(lambda x: int(((x.get("exists_in_a", False) == False) & (x.get("exists_in_b", False) == True)).sum())).values,
        "any_mismatch": g["any_mismatch"].apply(_sum_bool).values if "any_mismatch" in df.columns else 0,
        "hard_issue": g["hard_issue"].apply(_sum_bool).values if "hard_issue" in df.columns else 0,
        "warning": g["warning"].apply(_sum_bool).values if "warning" in df.columns else 0,
        "has_issue": g["has_issue"].apply(_sum_bool).values if "has_issue" in df.columns else 0,
    })

    out["issue_rate"] = out.apply(lambda r: (r["has_issue"] / r["rows"]) if r["rows"] else 0.0, axis=1)
    out["hard_issue_rate"] = out.apply(lambda r: (r["hard_issue"] / r["rows"]) if r["rows"] else 0.0, axis=1)
    out["warning_rate"] = out.apply(lambda r: (r["warning"] / r["rows"]) if r["rows"] else 0.0, axis=1)
    out["mismatch_rate"] = out.apply(lambda r: (r["any_mismatch"] / r["rows"]) if r["rows"] else 0.0, axis=1)
    out["missing_in_b_rate"] = out.apply(lambda r: (r["missing_in_b"] / r["orders_a"]) if r["orders_a"] else 0.0, axis=1)

    return out.sort_values("snapshot_date")


def build_status_distribution(df: pd.DataFrame) -> pd.DataFrame:
    tmp = df[["snapshot_date"]].copy()
    tmp["status"] = df.get("status_a")
    tmp["substatus"] = df.get("substatus_a")
    return tmp.groupby(["snapshot_date", "status", "substatus"], dropna=False).size().reset_index(name="count")


def build_aging_buckets(df: pd.DataFrame) -> pd.DataFrame:
    if "age_days" not in df.columns:
        return pd.DataFrame(columns=["snapshot_date", "status", "substatus", "age_bucket", "count"])

    tmp = df[["snapshot_date", "age_days"]].copy()
    tmp["status"] = df.get("status_a")
    tmp["substatus"] = df.get("substatus_a")

    bins = [-1e9, 1, 4, 30, 1e9]
    labels = ["0-1d", "1-4d", "4-30d", ">30d"]
    tmp["age_bucket"] = pd.cut(_to_num(tmp["age_days"]).fillna(-1), bins=bins, labels=labels)

    return tmp.groupby(["snapshot_date", "status", "substatus", "age_bucket"], dropna=False).size().reset_index(name="count")


def build_issue_cases(df: pd.DataFrame) -> pd.DataFrame:
    cond = pd.Series(False, index=df.index)
    for c in ["has_issue", "any_mismatch"]:
        if c in df.columns:
            cond = cond | df[c].fillna(False).astype(bool)

    if "exists_in_a" in df.columns and "exists_in_b" in df.columns:
        cond = cond | ((df["exists_in_a"] == True) & (df["exists_in_b"] == False))
        cond = cond | ((df["exists_in_a"] == False) & (df["exists_in_b"] == True))

    issues = df.loc[cond].copy()
    if "hard_issue" in issues.columns:
        issues["case_type"] = issues["hard_issue"].fillna(False).astype(bool).map(lambda v: "hard" if v else "warning")
    else:
        issues["case_type"] = "warning"

    reason_cols = [
        "inprep_on_hold_gt_30d","inpo_on_hold_gt_4d","inpo_error_stuck_gt_4d",
        "completed_missing_po_copy","completed_unequal_values","cancelled_unsynchronized",
        "cancelled_synchronized","unknown_status_or_substatus",
        "status_mismatch","substatus_mismatch","value_mismatch",
    ]
    present = [c for c in reason_cols if c in issues.columns]

    # Some flag columns use pandas' nullable boolean dtype and can contain <NA>.
    # Normalize them so boolean checks don't crash.
    if present:
        issues.loc[:, present] = issues.loc[:, present].fillna(False)

    def _is_true(v) -> bool:
        if v is True:
            return True
        try:
            if pd.isna(v):
                return False
        except Exception:
            return False
        try:
            return bool(v)
        except Exception:
            return False

    def _is_false(v) -> bool:
        if v is False:
            return True
        try:
            if pd.isna(v):
                return False
        except Exception:
            return False
        try:
            return bool(v) is False
        except Exception:
            return False

    def build_reason(row) -> str:
        parts: list[str] = []
        for c in present:
            if _is_true(row.get(c, False)):
                parts.append(c)

        exists_a = row.get("exists_in_a", False)
        exists_b = row.get("exists_in_b", False)

        if _is_true(exists_a) and _is_false(exists_b):
            parts.append("missing_in_b")
        if _is_true(exists_b) and _is_false(exists_a):
            parts.append("missing_in_a")

        return ",".join(parts)

    issues["reasons"] = issues.apply(build_reason, axis=1)

    keep = ["snapshot_date","case_type","cart_id","status_a","substatus_a","status_b","substatus_b","age_days",
            "exists_in_a","exists_in_b","hard_issue","warning","has_issue","any_mismatch","reasons","source_file_a","source_file_b"]
    keep = [c for c in keep if c in issues.columns]
    return issues[keep]


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

    build_daily_kpis(df).to_csv(PBI_DAILY_KPIS, index=False)
    build_status_distribution(df).to_csv(PBI_STATUS_DISTRIBUTION, index=False)
    build_aging_buckets(df).to_csv(PBI_AGING_BUCKETS, index=False)
    build_issue_cases(df).to_csv(PBI_ISSUE_CASES, index=False)

    _log("Wrote Power BI CSVs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
