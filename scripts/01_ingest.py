"""01_ingest.py

Reads daily XLSX dumps from Instance A and Instance B.
- Normalizes column names to canonical names (config.COLUMN_SYNONYMS)
- Ensures required columns exist (source-specific)
- Parses timestamps
- Writes cleaned per-day parquet files to data/clean/

Expected raw filenames:
  data/raw/instance_a/instance_a_YYYY-MM-DD.xlsx
  data/raw/instance_b/instance_b_YYYY-MM-DD.xlsx

Output:
  data/clean/clean_a_YYYY-MM-DD.parquet
  data/clean/clean_b_YYYY-MM-DD.parquet
"""

from __future__ import annotations

import argparse
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

from config import (
    CLEAN_DIR,
    COLUMN_SYNONYMS,
    LOG_DIR,
    RAW_A_DIR,
    RAW_A_GLOB,
    RAW_B_DIR,
    RAW_B_GLOB,
    REQUIRED_COLUMNS_A,
    REQUIRED_COLUMNS_B,
)


DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def _ensure_dirs() -> None:
    for p in [CLEAN_DIR, LOG_DIR]:
        p.mkdir(parents=True, exist_ok=True)


def _log(msg: str) -> None:
    _ensure_dirs()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    (LOG_DIR / "ingest.log").open("a", encoding="utf-8").write(line + "\n")


def extract_date_from_filename(path: Path) -> Optional[str]:
    m = DATE_RE.search(path.name)
    return m.group(1) if m else None


def _lower_strip(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())


def build_reverse_synonyms_map(column_synonyms: Dict[str, set[str]]) -> Dict[str, str]:
    rev: Dict[str, str] = {}
    for canonical, synonyms in column_synonyms.items():
        for raw in synonyms:
            rev[_lower_strip(raw)] = canonical
    return rev


REV_SYNONYMS = build_reverse_synonyms_map(COLUMN_SYNONYMS)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map: Dict[str, str] = {}
    for c in df.columns:
        key = _lower_strip(str(c))
        if key in REV_SYNONYMS:
            rename_map[c] = REV_SYNONYMS[key]
    df = df.rename(columns=rename_map)

    if df.columns.duplicated().any():
        df = df.loc[:, ~df.columns.duplicated()]

    return df


def ensure_required_columns(
    df: pd.DataFrame,
    required_cols: list[str],
    source_label: str,
    file_path: Path,
) -> pd.DataFrame:
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required columns {missing} in {source_label} file {file_path.name}. "
            f"Available columns: {list(df.columns)}"
        )
    return df


def parse_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["created_at", "updated_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    return df


def add_metadata(df: pd.DataFrame, source: str, file_date: str, file_path: Path) -> pd.DataFrame:
    df = df.copy()
    df["source"] = source
    df["file_date"] = file_date
    df["source_file"] = file_path.name
    return df


def read_xlsx(path: Path) -> pd.DataFrame:
    return pd.read_excel(path, engine="openpyxl")


def clean_one_file(path: Path, source: str) -> Tuple[str, pd.DataFrame]:
    file_date = extract_date_from_filename(path)
    if not file_date:
        raise ValueError(f"Could not extract YYYY-MM-DD from filename: {path.name}")

    df = read_xlsx(path)
    df = normalize_columns(df)

    required = REQUIRED_COLUMNS_A if source.upper() == "A" else REQUIRED_COLUMNS_B
    df = ensure_required_columns(df, required_cols=required, source_label=source, file_path=path)

    df = parse_timestamps(df)
    df = add_metadata(df, source=source, file_date=file_date, file_path=path)

    df["cart_id"] = df["cart_id"].astype(str).str.strip()

    return file_date, df


def output_path(source: str, file_date: str) -> Path:
    s = source.lower()
    if s == "a":
        return CLEAN_DIR / f"clean_a_{file_date}.parquet"
    if s == "b":
        return CLEAN_DIR / f"clean_b_{file_date}.parquet"
    raise ValueError(f"Unknown source {source!r}")


def write_parquet(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, engine="pyarrow", index=False)


def ingest_folder(folder: Path, glob_pattern: str, source: str, force: bool) -> int:
    files = sorted(folder.glob(glob_pattern))
    if not files:
        _log(f"No files found for {source} in {folder} ({glob_pattern})")
        return 0

    processed = 0
    for f in files:
        try:
            file_date, df = clean_one_file(f, source=source)
            out = output_path(source, file_date)

            if out.exists() and not force:
                continue

            write_parquet(df, out)
            _log(f"Wrote {out.name} rows={len(df)} cols={len(df.columns)} from {f.name}")
            processed += 1
        except Exception as e:
            _log(f"ERROR ingesting {source} file {f.name}: {e}")

    return processed


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest raw XLSX dumps into clean parquet files.")
    parser.add_argument("--force", action="store_true", help="Rebuild clean parquet even if it exists")
    args = parser.parse_args()

    _ensure_dirs()

    n_a = ingest_folder(RAW_A_DIR, RAW_A_GLOB, source="A", force=args.force)
    n_b = ingest_folder(RAW_B_DIR, RAW_B_GLOB, source="B", force=args.force)

    _log(f"Done. processed A={n_a} B={n_b}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())