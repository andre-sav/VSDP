from __future__ import annotations

import os
from typing import List, Optional

import pandas as pd
import vanillasoft_automation as auto

# Hardcoded output schemas derived from the official template CSV headers.
SALES_GENIE_TEMPLATE_COLUMNS: List[str] = ['List Source', 'Company Name', 'Business', 'Number of Employees', 'Web site', 'First Name', 'Last Name', 'Title', 'Address', 'City', 'State', 'ZIP code', 'Primary SIC', 'Primary Line of Business', 'Square Footage', 'Contact Owner', 'Lead Source', 'Vending Business Name', 'Operator Name', 'Operator Phone #', 'Operator Email Address', 'Operator Zip Code', 'Operator Website Address', 'Best Appts Time', 'Unavailable for appointments', 'Team', 'Call Priority']
ZOOMINFO_TEMPLATE_COLUMNS: List[str] = ['List Source', 'Last Name', 'First Name', 'Title', 'Home', 'Email', 'Mobile', 'Company', 'Web site', 'Business', 'Number of Employees', 'Primary SIC', 'Primary Line of Business', 'Address', 'City', 'State', 'ZIP code', 'Square Footage', 'Contact Owner', 'Lead Source', 'Vending Business Name', 'Operator Name', 'Operator Phone #', 'Operator Email Address', 'Operator Zip Code', 'Operator Website Address', 'Best Appts Time', 'Unavailable for appointments', 'Team', 'Call Priority']

IMPORT_NOTES_COL = "Import Notes"  # only additional output column allowed


def _apply_streamlit_secrets_to_env() -> None:
    """Populate env vars from st.secrets (keeps Config() behavior unchanged)."""
    try:
        import streamlit as st  # type: ignore
    except Exception:
        return

    for key in [
        "ZOHO_ACCOUNTS_URL",
        "ZOHO_API_BASE",
        "ZOHO_CLIENT_ID",
        "ZOHO_CLIENT_SECRET",
        "ZOHO_REFRESH_TOKEN",
    ]:
        if key in st.secrets and st.secrets.get(key):
            os.environ.setdefault(key, str(st.secrets.get(key)))


def _template_cols(data_source: str) -> List[str]:
    return ZOOMINFO_TEMPLATE_COLUMNS if data_source == "ZoomInfo" else SALES_GENIE_TEMPLATE_COLUMNS


def _desired_cols(data_source: str) -> List[str]:
    cols = list(_template_cols(data_source))
    if IMPORT_NOTES_COL not in cols:
        cols.append(IMPORT_NOTES_COL)
    return cols


def _enforce_schema(df: pd.DataFrame, data_source: str) -> pd.DataFrame:
    desired = _desired_cols(data_source)
    df = df.copy()
    for c in desired:
        if c not in df.columns:
            df[c] = ""
    return df[desired].copy()


def run_pipeline(
    *,
    data_source: str,
    raw_csv_path: str,
    master_xlsx_path: str,
    contact_owner_email: Optional[str] = None,
    use_zoho: bool = False,
    output_dir: str,
) -> List[str]:
    """
    File-path-based pipeline wrapper (useful if you want a smaller app.py).
    Output columns will match the selected template + Import Notes only.
    """
    _apply_streamlit_secrets_to_env()

    config = auto.Config()
    config.data_source = data_source
    config.contact_owner_email = contact_owner_email
    config.raw_data_path = raw_csv_path
    config.master_data_path = master_xlsx_path
    config.output_dir = output_dir

    if not use_zoho:
        config.zoho_enabled = False

    zoho_api = auto.ZohoAPI(config)
    operator_info = auto.load_master_data(config.master_data_path)

    # Build mapping schema from template columns (no template files needed)
    template_df = pd.DataFrame(columns=_template_cols(config.data_source))

    df_raw, phone_column = auto.load_raw_data(config)
    df_clean = auto.clean_phone_dataframe(df_raw, phone_column)
    df_clean = auto.remove_duplicate_phones(df_clean, phone_column)

    if config.data_source == "ZoomInfo":
        df_mapped = auto.map_zoominfo_to_template(df_clean, template_df)
    else:
        df_mapped = auto.map_salesgenie_to_template(df_clean, template_df)

    df_filled = auto.fill_operator_fields(
        df_mapped,
        operator_info,
        (config.contact_owner_email or ""),
        config.data_source,
    )

    df_final, df_deliveries = auto.match_with_zoho(df_filled, zoho_api)

    # Enforce output schema
    df_final = _enforce_schema(df_final, config.data_source)
    if not df_deliveries.empty:
        df_deliveries = _enforce_schema(df_deliveries, config.data_source)

    main_fp, deliveries_fp = auto.export_data(
        df_final,
        df_deliveries,
        operator_info.get("operator_name", "Unknown"),
        config.data_source,
        config.output_dir,
    )

    out: List[str] = []
    if main_fp:
        out.append(main_fp)
    if deliveries_fp:
        out.append(deliveries_fp)
    return out
