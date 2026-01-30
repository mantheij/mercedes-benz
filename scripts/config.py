"""Central configuration for the daily monitoring pipeline.

Keep this file simple and explicit.
Adjust only here when schemas change.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Set


# ---------- Paths ----------

PROJECT_ROOT = Path.cwd()
DATA_DIR = PROJECT_ROOT / "data"

RAW_A_DIR = DATA_DIR / "raw" / "instance_a"
RAW_B_DIR = DATA_DIR / "raw" / "instance_b"

CLEAN_DIR = DATA_DIR / "clean"
MODEL_DIR = DATA_DIR / "model"
PBI_DIR = DATA_DIR / "pbi"

LOG_DIR = PROJECT_ROOT / "logs"


# ---------- File naming ----------

# Expected input filename format:
#   instance_a_YYYY-MM-DD.xlsx
#   instance_b_YYYY-MM-DD.xlsx
RAW_A_GLOB = "instance_a_*.xlsx"
RAW_B_GLOB = "instance_b_*.xlsx"


# ---------- Core keys ----------

# Primary join key between A and B.
# If you later find that Order IDs are not unique across towers,
# set JOIN_KEYS = ["cart_id", "tower"]
JOIN_KEYS: List[str] = ["cart_id"]


# ---------- Column mapping ----------

# Map many possible raw column names (from dumps) to one canonical name.
# Add synonyms as you encounter them.
COLUMN_SYNONYMS: Dict[str, Set[str]] = {
    # identifiers
    "cart_id": {
        "cart_id",
        "cartid",
        "cart",
        "shopping_cart_id",
        "shoppingcartid",
        "order_id",
        "orderid",
        "Cart Number",
        "Cart number",
        "Shopping Cart",
        "Shopping cart",
    },
    "tower": {"tower", "category", "product_tower", "cart_tower", "Service Tower"},

    # status fields
    "status": {"status", "cart_status", "shopping_cart_status"},
    "substatus": {"substatus", "sub_status", "cart_substatus", "shopping_cart_substatus", "sub-state", "substate"},

    # timestamps
    "created_at": {"created_at", "created", "creation_date", "createddate", "created_on", "createdon"},
    "updated_at": {"updated_at", "updated", "last_updated", "lastupdate", "modified_at", "modified", "changed_at"},

    # business values (optional, for mismatch checks)
    "total_value": {
        "total_value",
        "total",
        "total_amount",
        "amount_total",
        "cart_total",
        "totalprice",
        "Shopping Cart Total",
        "Total cost",
    },
    "currency": {"currency", "curr", "Local Currency"},

    # PO / document fields (optional)
    "po_number": {
        "po_number",
        "po",
        "purchase_order",
        "purchase_order_number",
        "ponumber",
        "Purchase Order Number",
        "PO Number",
    },
    "po_pdf_sent": {"po_pdf_sent", "pdf_sent", "po_copy_sent", "po_document_sent"},
}

# Canonical columns that must exist after normalization.
# Keep this small so ingestion doesn't fail due to nice-to-have fields.
REQUIRED_COLUMNS_A: List[str] = [
    "cart_id",
    "status",
    "substatus",
    "updated_at",
]

# Instance B dumps may not contain statuses/substatuses.
REQUIRED_COLUMNS_B: List[str] = [
    "cart_id",
    "updated_at",
]

# Backward-compatible alias (do not use for new code)
REQUIRED_COLUMNS: List[str] = REQUIRED_COLUMNS_A

# Optional columns used by some rules/checks.
OPTIONAL_COLUMNS: List[str] = [
    "tower",
    "created_at",
    "total_value",
    "currency",
    "po_number",
    "po_pdf_sent",
]


# ---------- Status taxonomy ----------

# Normalize status/substatus to these exact strings (case-insensitive compare).
# If the source uses different naming, keep those as-is in raw, but map to these in code.

STATUSES: Set[str] = {
    "In Preparation",
    "In PO Creation",
    "Completed",
    "Cancelled",
}

SUBSTATUSES_BY_STATUS: Dict[str, Set[str]] = {
    "In Preparation": {"Preparing", "On Hold"},
    "In PO Creation": {"Creating", "Error", "On Hold"},
    "Completed": {"Provider PO Assigned", "Missing PO Copy", "Unequal Values", "Error"},
    "Cancelled": {"Cancelled – Synchronized", "Cancelled – Unsynchronized"},
}


# ---------- Aging / SLA thresholds ----------

# In Preparation -> On Hold: no changes for over 1 month
HOLD_PREPARATION_DAYS = 30

# In PO Creation -> On Hold: no changes for over 4 days
HOLD_PO_CREATION_DAYS = 4

# "Stuck in error" threshold for In PO Creation / Error
STUCK_IN_ERROR_DAYS = 4


# ---------- Value mismatch settings ----------

# If total_value differs more than this tolerance (absolute), flag mismatch.
VALUE_MISMATCH_ABS_TOLERANCE = 0.01


# ---------- Export filenames (Power BI inputs) ----------

FACT_ORDERS_FILE = MODEL_DIR / "fact_orders.parquet"

PBI_DAILY_KPIS = PBI_DIR / "daily_kpis.csv"
PBI_STATUS_DISTRIBUTION = PBI_DIR / "status_distribution_daily.csv"
PBI_AGING_BUCKETS = PBI_DIR / "aging_buckets.csv"
PBI_ISSUE_CASES = PBI_DIR / "issue_cases.csv"


@dataclass(frozen=True)
class PipelineConfig:
    """Typed access if you prefer passing one config object around."""

    project_root: Path = PROJECT_ROOT
    raw_a_dir: Path = RAW_A_DIR
    raw_b_dir: Path = RAW_B_DIR
    clean_dir: Path = CLEAN_DIR
    model_dir: Path = MODEL_DIR
    pbi_dir: Path = PBI_DIR
    log_dir: Path = LOG_DIR

    raw_a_glob: str = RAW_A_GLOB
    raw_b_glob: str = RAW_B_GLOB

    join_keys: List[str] = tuple(JOIN_KEYS)  # type: ignore[assignment]

    required_columns_a: List[str] = tuple(REQUIRED_COLUMNS_A)  # type: ignore[assignment]
    required_columns_b: List[str] = tuple(REQUIRED_COLUMNS_B)  # type: ignore[assignment]

    optional_columns: List[str] = tuple(OPTIONAL_COLUMNS)  # type: ignore[assignment]

    hold_preparation_days: int = HOLD_PREPARATION_DAYS
    hold_po_creation_days: int = HOLD_PO_CREATION_DAYS
    stuck_in_error_days: int = STUCK_IN_ERROR_DAYS

    value_mismatch_abs_tolerance: float = VALUE_MISMATCH_ABS_TOLERANCE
