"""02_match.py

Builds the central fact table by matching Instance A and B carts.

Input:
  data/clean/clean_a_YYYY-MM-DD.parquet
  data/clean/clean_b_YYYY-MM-DD.parquet

Output:
  data/model/fact_orders.parquet
"""

from __future__ import annotations

import argparse
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd

from config import CLEAN_DIR, FACT_ORDERS_FILE, JOIN_KEYS, LOG_DIR, VALUE_MISMATCH_ABS_TOLERANCE

DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def _ensure_dirs() -> None:
    for p in [FACT_ORDERS_FILE.parent, LOG_DIR]:
        p.mkdir(parents=True, exist_ok=True)


def _log(msg: str) -> None:
    _ensure_dirs()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with (LOG_DIR / "match.log").open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def _extract_date(name: str) -> Optional[str]:
    m = DATE_RE.search(name)
    return m.group(1) if m else None


def _list_clean_files(prefix: str) -> List[Path]:
    return sorted(CLEAN_DIR.glob(f"{prefix}_*.parquet"))


def _read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def _suffix_columns(df: pd.DataFrame, suffix: str, keep: Set[str]) -> pd.DataFrame:
    rename = {c: f"{c}{suffix}" for c in df.columns if c not in keep}
    return df.rename(columns=rename)


def _coerce_join_keys(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for k in JOIN_KEYS:
        if k in out.columns:
            out[k] = out[k].astype(str).str.strip()
    return out


def _value_mismatch(a: pd.Series, b: pd.Series, tol: float) -> pd.Series:
    a_num = pd.to_numeric(a, errors="coerce")
    b_num = pd.to_numeric(b, errors="coerce")
    return (a_num - b_num).abs() > tol


def match_one_day(date_str: str, a_path: Optional[Path], b_path: Optional[Path]) -> pd.DataFrame:
    a_df = _read_parquet(a_path) if a_path else pd.DataFrame(columns=list(JOIN_KEYS))
    b_df = _read_parquet(b_path) if b_path else pd.DataFrame(columns=list(JOIN_KEYS))

    a_df = _coerce_join_keys(a_df)
    b_df = _coerce_join_keys(b_df)

    for k in JOIN_KEYS:
        if k not in a_df.columns:
            a_df[k] = pd.Series(dtype="string")
        if k not in b_df.columns:
            b_df[k] = pd.Series(dtype="string")

    keep = set(JOIN_KEYS) | {"file_date"}

    a_df = _suffix_columns(a_df, "_a", keep=keep)
    b_df = _suffix_columns(b_df, "_b", keep=keep)

    fact = a_df.merge(b_df, how="outer", on=list(JOIN_KEYS), indicator=True)
    fact["snapshot_date"] = date_str

    fact["exists_in_a"] = fact["_merge"].isin(["both", "left_only"])
    fact["exists_in_b"] = fact["_merge"].isin(["both", "right_only"])

    if "status_a" in fact.columns and "status_b" in fact.columns:
        sa = fact["status_a"].astype("string")
        sb = fact["status_b"].astype("string")
        fact["status_mismatch"] = fact["exists_in_a"] & fact["exists_in_b"] & (sa.fillna("") != sb.fillna(""))
    else:
        fact["status_mismatch"] = False

    if "substatus_a" in fact.columns and "substatus_b" in fact.columns:
        ssa = fact["substatus_a"].astype("string")
        ssb = fact["substatus_b"].astype("string")
        fact["substatus_mismatch"] = fact["exists_in_a"] & fact["exists_in_b"] & (ssa.fillna("") != ssb.fillna(""))
    else:
        fact["substatus_mismatch"] = False

    if "total_value_a" in fact.columns and "total_value_b" in fact.columns:
        fact["value_mismatch"] = fact["exists_in_a"] & fact["exists_in_b"] & _value_mismatch(
            fact["total_value_a"], fact["total_value_b"], VALUE_MISMATCH_ABS_TOLERANCE
        )
    else:
        fact["value_mismatch"] = False

    fact["any_mismatch"] = fact["status_mismatch"] | fact["substatus_mismatch"] | fact["value_mismatch"]
    return fact.drop(columns=["_merge"])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    _ensure_dirs()
    _log("Starting match")

    if FACT_ORDERS_FILE.exists() and not args.force:
        _log(f"fact table exists, skipping: {FACT_ORDERS_FILE}")
        return 0

    a_files = _list_clean_files("clean_a")
    b_files = _list_clean_files("clean_b")
    _log(f"Found clean files: A={len(a_files)} B={len(b_files)} in {CLEAN_DIR}")

    a_by_date: Dict[str, Path] = {d: p for p in a_files if (d := _extract_date(p.name))}
    b_by_date: Dict[str, Path] = {d: p for p in b_files if (d := _extract_date(p.name))}

    dates = sorted(set(a_by_date) | set(b_by_date))
    if not dates:
        _log("No clean parquet files found")
        return 0

    parts: List[pd.DataFrame] = []
    for d in dates:
        part = match_one_day(d, a_by_date.get(d), b_by_date.get(d))
        parts.append(part)
        _log(f"snapshot {d}: rows={len(part)}")

    fact_all = pd.concat(parts, ignore_index=True).drop_duplicates(subset=["snapshot_date", *JOIN_KEYS], keep="last")
    FACT_ORDERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    fact_all.to_parquet(FACT_ORDERS_FILE, engine="pyarrow", index=False)
    _log(f"Wrote fact table: {FACT_ORDERS_FILE.name} rows={len(fact_all)} cols={len(fact_all.columns)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
