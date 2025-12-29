#!/usr/bin/env python3
"""
VanillaSoft Data Preparation - Streamlit Web App

A user-friendly web interface for preparing ZoomInfo/SalesGenie data
for VanillaSoft upload with Zoho CRM matching.

Usage:
    streamlit run app.py

Requirements:
    pip install streamlit pandas requests python-dotenv openpyxl gspread google-auth

Google Sheets Setup (for persistent operator storage):
    1. Create a Google Cloud project at https://console.cloud.google.com
    2. Enable the Google Sheets API
    3. Create a Service Account and download the JSON credentials
    4. Create a Google Sheet and share it with the service account email
    5. In Streamlit Cloud, add secrets in the dashboard (Settings > Secrets):
    
    [gcp_service_account]
    type = "service_account"
    project_id = "your-project-id"
    private_key_id = "..."
    private_key = "-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n"
    client_email = "your-service-account@your-project.iam.gserviceaccount.com"
    client_id = "..."
    auth_uri = "https://accounts.google.com/o/oauth2/auth"
    token_uri = "https://oauth2.googleapis.com/token"
    auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
    client_x509_cert_url = "..."
    
    [google_sheets]
    spreadsheet_id = "your-spreadsheet-id-from-url"
"""

import io
import json
import os
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st

# Google Sheets integration
try:
    import gspread
    from google.oauth2.service_account import Credentials
    GSHEETS_AVAILABLE = True
except ImportError:
    GSHEETS_AVAILABLE = False


# =============================================================================
# GOOGLE SHEETS OPERATOR STORAGE
# =============================================================================

class OperatorStorage:
    """
    Persistent storage for operator data using Google Sheets.
    
    Setup Instructions:
    1. Create a Google Cloud project at https://console.cloud.google.com
    2. Enable the Google Sheets API
    3. Create a Service Account and download the JSON credentials
    4. Create a Google Sheet and share it with the service account email
    5. Add credentials to Streamlit secrets (secrets.toml or Streamlit Cloud)
    
    Expected secrets.toml format:
    [gcp_service_account]
    type = "service_account"
    project_id = "your-project-id"
    private_key_id = "..."
    private_key = "-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n"
    client_email = "your-service-account@your-project.iam.gserviceaccount.com"
    client_id = "..."
    auth_uri = "https://accounts.google.com/o/oauth2/auth"
    token_uri = "https://oauth2.googleapis.com/token"
    auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
    client_x509_cert_url = "..."
    
    [google_sheets]
    spreadsheet_id = "your-spreadsheet-id-from-url"
    """
    
    SCOPES = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    # Column headers for the operators sheet
    HEADERS = [
        'operator_id',
        'operator_name', 
        'vending_business_name',
        'operator_phone',
        'operator_email',
        'operator_zip',
        'operator_website',
        'team',
        'created_at',
        'updated_at'
    ]
    
    def __init__(self):
        self.client = None
        self.sheet = None
        self.worksheet = None
        self.is_configured = False
        self._initialize()
    
    def _initialize(self):
        """Initialize Google Sheets connection."""
        if not GSHEETS_AVAILABLE:
            return
        
        try:
            # Check if credentials are configured in Streamlit secrets
            if 'gcp_service_account' not in st.secrets:
                return
            if 'google_sheets' not in st.secrets:
                return
            
            # Create credentials from secrets
            creds = Credentials.from_service_account_info(
                dict(st.secrets['gcp_service_account']),
                scopes=self.SCOPES
            )
            
            # Connect to Google Sheets
            self.client = gspread.authorize(creds)
            
            # Open the spreadsheet
            spreadsheet_id = st.secrets['google_sheets']['spreadsheet_id']
            self.sheet = self.client.open_by_key(spreadsheet_id)
            
            # Get or create the operators worksheet
            try:
                self.worksheet = self.sheet.worksheet('Operators')
            except gspread.WorksheetNotFound:
                self.worksheet = self.sheet.add_worksheet(
                    title='Operators',
                    rows=100,
                    cols=len(self.HEADERS)
                )
                # Add headers
                self.worksheet.update('A1', [self.HEADERS])
            
            self.is_configured = True
            
        except Exception as e:
            st.warning(f"⚠️ Google Sheets not configured: {str(e)}")
            self.is_configured = False
    
    def get_all_operators(self) -> List[Dict]:
        """Retrieve all saved operators from Google Sheets."""
        if not self.is_configured:
            return []
        
        try:
            # Get all records
            records = self.worksheet.get_all_records()
            return records
        except Exception as e:
            st.error(f"Error loading operators: {str(e)}")
            return []
    
    def save_operator(self, operator: Dict) -> bool:
        """Save a new operator or update existing one."""
        if not self.is_configured:
            return False
        
        try:
            now = datetime.now().isoformat()
            
            # Check if operator already exists (by name)
            existing = self.get_all_operators()
            existing_row = None
            
            for idx, op in enumerate(existing):
                if op.get('operator_name') == operator.get('operator_name'):
                    existing_row = idx + 2  # +2 for header row and 0-indexing
                    break
            
            # Prepare row data
            row_data = [
                operator.get('operator_id', f"op_{datetime.now().strftime('%Y%m%d%H%M%S')}"),
                operator.get('operator_name', ''),
                operator.get('vending_business_name', 'N/A'),
                operator.get('operator_phone', 'N/A'),
                operator.get('operator_email', 'N/A'),
                operator.get('operator_zip', 'N/A'),
                operator.get('operator_website', 'N/A'),
                operator.get('team', 'N/A'),
                operator.get('created_at', now),
                now  # updated_at
            ]
            
            if existing_row:
                # Update existing row
                self.worksheet.update(f'A{existing_row}', [row_data])
            else:
                # Append new row
                self.worksheet.append_row(row_data)
            
            return True
            
        except Exception as e:
            st.error(f"Error saving operator: {str(e)}")
            return False
    
    def delete_operator(self, operator_name: str) -> bool:
        """Delete an operator by name."""
        if not self.is_configured:
            return False
        
        try:
            # Find the operator's row
            cell = self.worksheet.find(operator_name, in_column=2)
            if cell:
                self.worksheet.delete_rows(cell.row)
                return True
            return False
            
        except Exception as e:
            st.error(f"Error deleting operator: {str(e)}")
            return False


@st.cache_resource
def get_operator_storage():
    """Get cached operator storage instance."""
    return OperatorStorage()


# =============================================================================
# CALL CENTER AGENTS (Round-Robin Assignment)
# =============================================================================
# These emails will be assigned to records in round-robin fashion
# Replace with actual agent emails

CALL_CENTER_AGENTS: List[str] = [
  "courtney@hlmii.com",
  "caitlyn@hlmii.com",
  "caroline.hlmii.com",
  "dodie@hlmii.com",
  "chelsie@hlmii.com",
  "hannah@hlmii.com",
  "jessica@hlmii.com",
]


def assign_contact_owners(df: pd.DataFrame, agents: List[str]) -> pd.DataFrame:
    """
    Assign contact owners to records in round-robin fashion.
    
    Args:
        df: DataFrame to assign contact owners to
        agents: List of agent email addresses
    
    Returns:
        DataFrame with 'Contact Owner' column populated
    """
    df = df.copy()
    
    if not agents:
        df['Contact Owner'] = ''
        return df
    
    # Round-robin assignment
    num_agents = len(agents)
    df['Contact Owner'] = [agents[i % num_agents] for i in range(len(df))]
    
    return df


# =============================================================================
# OUTPUT SCHEMA (HARDCODED FROM OFFICIAL TEMPLATES)
# =============================================================================

SALES_GENIE_TEMPLATE_COLUMNS = ['List Source', 'Company Name', 'Business', 'Number of Employees', 'Web site', 'First Name', 'Last Name', 'Title', 'Address', 'City', 'State', 'ZIP code', 'Primary SIC', 'Primary Line of Business', 'Square Footage', 'Contact Owner', 'Lead Source', 'Vending Business Name', 'Operator Name', 'Operator Phone #', 'Operator Email Address', 'Operator Zip Code', 'Operator Website Address', 'Best Appts Time', 'Unavailable for appointments', 'Team', 'Call Priority']
ZOOMINFO_TEMPLATE_COLUMNS = ['List Source', 'Last Name', 'First Name', 'Title', 'Home', 'Email', 'Mobile', 'Company', 'Web site', 'Business', 'Number of Employees', 'Primary SIC', 'Primary Line of Business', 'Address', 'City', 'State', 'ZIP code', 'Square Footage', 'Contact Owner', 'Lead Source', 'Vending Business Name', 'Operator Name', 'Operator Phone #', 'Operator Email Address', 'Operator Zip Code', 'Operator Website Address', 'Best Appts Time', 'Unavailable for appointments', 'Team', 'Call Priority']
EXTRA_OUTPUT_COLUMN = "Import Notes"

def get_template_columns(data_source: str):
    return ZOOMINFO_TEMPLATE_COLUMNS if data_source == "ZoomInfo" else SALES_GENIE_TEMPLATE_COLUMNS

def build_template_df(data_source: str) -> pd.DataFrame:
    return pd.DataFrame(columns=get_template_columns(data_source))

def enforce_output_schema(df: pd.DataFrame, data_source: str) -> pd.DataFrame:
    desired = list(get_template_columns(data_source)) + [EXTRA_OUTPUT_COLUMN]
    df = df.copy()

    for c in desired:
        if c not in df.columns:
            df[c] = ""

    return df[desired].copy()

from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# PAGE CONFIGURATION
# =============================================================================

st.set_page_config(
    page_title="VanillaSoft Data Prep",
    page_icon="🚀",
    layout="centered",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .success-box {
        padding: 1rem;
        background-color: #d4edda;
        border-radius: 0.5rem;
        border-left: 4px solid #28a745;
    }
    .warning-box {
        padding: 1rem;
        background-color: #fff3cd;
        border-radius: 0.5rem;
        border-left: 4px solid #ffc107;
    }
    .info-box {
        padding: 1rem;
        background-color: #d1ecf1;
        border-radius: 0.5rem;
        border-left: 4px solid #17a2b8;
    }
    .stDownloadButton > button {
        width: 100%;
    }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# CONFIGURATION
# =============================================================================

class Config:
    """Configuration class for Zoho CRM settings"""
    
    def __init__(self):
        # Zoho CRM API Configuration (from environment)
        self.zoho_accounts_url = os.getenv("ZOHO_ACCOUNTS_URL", "https://accounts.zoho.com").rstrip("/")
        self.zoho_api_base = os.getenv("ZOHO_API_BASE", "https://www.zohoapis.com/crm/v8").rstrip("/")
        self.zoho_client_id = os.getenv("ZOHO_CLIENT_ID", "").strip()
        self.zoho_client_secret = os.getenv("ZOHO_CLIENT_SECRET", "").strip()
        self.zoho_refresh_token = os.getenv("ZOHO_REFRESH_TOKEN", "").strip()
        
        # Zoho CRM field names
        self.locatings_address_field = "Street_Address"
        self.locatings_zip_field = "Zip_Code"
        self.deliveries_address_field = "Address"
        self.deliveries_city_field = "City"
        
        # Validate Zoho credentials
        self.zoho_enabled = bool(
            self.zoho_client_id and 
            self.zoho_client_secret and 
            self.zoho_refresh_token
        )


# =============================================================================
# ZOHO CRM API CLASS
# =============================================================================

class ZohoAPI:
    """Handle all Zoho CRM API interactions with optimized batch queries"""
    
    def __init__(self, config: Config):
        self.config = config
        self.access_token = None
        self._org_id = None
    
    def mint_access_token(self) -> Optional[str]:
        """Generate a short-lived Zoho access token using the refresh token."""
        if not self.config.zoho_enabled:
            return None
        
        try:
            response = requests.post(
                f"{self.config.zoho_accounts_url}/oauth/v2/token",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self.config.zoho_refresh_token,
                    "client_id": self.config.zoho_client_id,
                    "client_secret": self.config.zoho_client_secret,
                },
                timeout=30
            )
            response.raise_for_status()
            
            json_response = response.json()
            if "access_token" not in json_response:
                return None
            
            self.access_token = json_response["access_token"]
            return self.access_token
        
        except Exception:
            return None
    
    def get_org_id(self) -> Optional[str]:
        """Fetch and cache the organization ID for correct URL construction."""
        if self._org_id:
            return self._org_id
        
        if not self.access_token:
            self.mint_access_token()
        
        if not self.access_token:
            return None
        
        try:
            headers = {"Authorization": f"Zoho-oauthtoken {self.access_token}"}
            response = requests.get(
                f"{self.config.zoho_api_base}/org",
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                org_data = response.json().get("org", [])
                if org_data and len(org_data) > 0:
                    self._org_id = org_data[0].get("id")
                    return self._org_id
        except Exception:
            pass
        
        return None
    
    def execute_coql(self, query: str) -> Dict:
        """Execute a COQL query against Zoho CRM."""
        if not self.access_token:
            return {"data": []}
        
        headers = {"Authorization": f"Zoho-oauthtoken {self.access_token}"}
        
        try:
            response = requests.post(
                f"{self.config.zoho_api_base}/coql",
                headers=headers,
                json={"select_query": query},
                timeout=60
            )
            
            if response.status_code == 204:
                return {"data": []}
            
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.RequestException:
            return {"data": []}
    
    def batch_query_locatings(self, zip_codes: List[str]) -> Dict[str, List[Dict]]:
        """Query multiple ZIP codes in one API call using IN operator."""
        if not zip_codes or not self.access_token:
            return {}
        
        results = {}
        batch_size = 50
        
        for i in range(0, len(zip_codes), batch_size):
            batch = zip_codes[i:i + batch_size]
            in_values = ", ".join([f"'{str(z).replace(chr(39), chr(39)+chr(39))}'" for z in batch])
            
            query = f"""
                select id, {self.config.locatings_address_field}, {self.config.locatings_zip_field}
                from Locatings
                where {self.config.locatings_zip_field} in ({in_values})
                limit 2000
            """.strip()
            
            response = self.execute_coql(query)
            
            for record in response.get("data", []):
                zip_code = str(record.get(self.config.locatings_zip_field, ""))
                if zip_code:
                    if zip_code not in results:
                        results[zip_code] = []
                    results[zip_code].append(record)
        
        return results

    def batch_query_deliveries(self, cities: List[str], zip_codes: List[str] = None) -> Dict[str, List[Dict]]:
        """
        Query Deliveries module. The City field contains full location string like
        "Long Beach, CA 90803" so we need to use LIKE for partial matching.
        
        Since LIKE doesn't work well with batch IN queries, we query individually
        but cache results to avoid duplicate queries.
        """
        if not self.access_token:
            return {}
        
        results = {}
        queried_terms = set()
        
        # Combine cities and zip codes for querying
        search_terms = []
        if cities:
            search_terms.extend([(c, 'city') for c in cities if c])
        if zip_codes:
            search_terms.extend([(z, 'zip') for z in zip_codes if z])
        
        for term, term_type in search_terms:
            if term in queried_terms:
                continue
            queried_terms.add(term)
            
            # Escape single quotes in search term
            escaped_term = str(term).replace("'", "''")
            
            # Use LIKE to search within the City field (which contains "City, State ZIP")
            query = f"""
                select id, {self.config.deliveries_address_field}, {self.config.deliveries_city_field}
                from Deliveries
                where {self.config.deliveries_city_field} like '%{escaped_term}%'
                limit 200
            """.strip()
            
            response = self.execute_coql(query)
            
            for record in response.get("data", []):
                city_field = str(record.get(self.config.deliveries_city_field, ""))
                if city_field:
                    # Store under the original search term
                    if term not in results:
                        results[term] = []
                    results[term].append(record)
        
        return results
    
    def build_record_url(self, module_name: str, record_id: str) -> str:
        """Build a Zoho CRM record URL."""
        org_id = self.get_org_id()
        module_api_name = "CustomModule5" if module_name == "Locatings" else module_name
        
        if org_id:
            return f"https://crm.zoho.com/crm/org{org_id}/tab/{module_api_name}/{record_id}"
        else:
            return f"https://crm.zoho.com/crm/tab/{module_api_name}/{record_id}"


# =============================================================================
# DATA PROCESSING FUNCTIONS
# =============================================================================

def remove_phone_extension(phone: str) -> str:
    """Remove extension from phone number."""
    if pd.isna(phone) or not isinstance(phone, str):
        return phone
    
    pattern = r'\s*[xX]\d+|\s*[eE][xX][tT]\.?\s*\d+|\s*[eE][xX][tT][eE][nN][sS][iI][oO][nN]\s*\d+'
    cleaned = re.split(pattern, phone)[0].strip()
    return cleaned


def clean_phone_dataframe(df: pd.DataFrame, phone_column: str) -> Tuple[pd.DataFrame, int]:
    """Clean phone numbers in a dataframe by removing extensions."""
    df = df.copy()
    
    if phone_column not in df.columns:
        return df, 0
    
    has_extension = df[phone_column].astype(str).str.contains(
        r'[xX]\d+|[eE][xX][tT]', 
        na=False, 
        regex=True
    )
    extension_count = has_extension.sum()
    
    if extension_count > 0:
        df[phone_column] = df[phone_column].apply(remove_phone_extension)
    
    return df, extension_count


def remove_duplicate_phones(df: pd.DataFrame, phone_column: str) -> Tuple[pd.DataFrame, int]:
    """Remove duplicate phone numbers, keeping only the first occurrence.
    
    Records with empty/null phone numbers are NOT treated as duplicates
    and are all kept.
    """
    if phone_column not in df.columns:
        return df, 0
    
    initial_count = len(df)
    
    # Separate records with and without phone numbers
    has_phone = df[phone_column].notna() & (df[phone_column].astype(str).str.strip() != '')
    
    df_with_phone = df[has_phone].copy()
    df_without_phone = df[~has_phone].copy()
    
    # Only dedupe records that have phone numbers
    df_with_phone_deduped = df_with_phone.drop_duplicates(subset=[phone_column], keep='first')
    
    # Combine: deduped records with phones + all records without phones
    df_clean = pd.concat([df_with_phone_deduped, df_without_phone], ignore_index=True)
    
    duplicates_removed = initial_count - len(df_clean)
    
    return df_clean, duplicates_removed


def load_master_data_multi(master_file, num_columns: int = 5) -> List[Dict[str, str]]:
    """
    Load operator metadata from the last N columns of Master Data Excel file.
    
    Data is read from FIXED ROW POSITIONS:
    - Row 2: Vending Business Name (may be empty)
    - Row 3: Operator Name
    - Row 4: Phone
    - Row 5: Email
    - Row 6: ZIP Code
    - Row 7: Website
    - Row 10: Team
    
    Args:
        master_file: Uploaded Excel file
        num_columns: Number of columns to read from the right (default: 5)
    
    Returns:
        List of operator info dictionaries
    """
    master_df = pd.read_excel(master_file)
    
    total_cols = master_df.shape[1]
    start_col = max(0, total_cols - num_columns)
    
    operators = []
    
    # Fixed row positions for operator data
    ROW_BUSINESS_NAME = 2
    ROW_OPERATOR_NAME = 3
    ROW_PHONE = 4
    ROW_EMAIL = 5
    ROW_ZIP = 6
    ROW_WEBSITE = 7
    ROW_TEAM = 10
    
    def safe_get(df, row, col):
        """Safely get a cell value, returning 'N/A' if empty, NaN, or already 'N/A'."""
        try:
            if row < len(df):
                val = df.iloc[row, col]
                if pd.notna(val):
                    # Clean up non-breaking spaces and whitespace
                    cleaned = str(val).replace('\xa0', ' ').strip()
                    # Return N/A if empty or already N/A
                    if cleaned == '' or cleaned.upper() == 'N/A':
                        return 'N/A'
                    return cleaned
        except (IndexError, KeyError):
            pass
        return 'N/A'
    
    for col_idx in range(start_col, total_cols):
        # Read from fixed row positions
        operator_name = safe_get(master_df, ROW_OPERATOR_NAME, col_idx)
        
        # Skip columns that don't have a valid operator name
        if operator_name == 'N/A':
            continue
        
        operator_info = {
            'column_index': col_idx,
            'vending_business_name': safe_get(master_df, ROW_BUSINESS_NAME, col_idx),
            'operator_name': operator_name,
            'operator_phone': safe_get(master_df, ROW_PHONE, col_idx),
            'operator_email': safe_get(master_df, ROW_EMAIL, col_idx),
            'operator_zip': safe_get(master_df, ROW_ZIP, col_idx),
            'operator_website': safe_get(master_df, ROW_WEBSITE, col_idx),
            'team': safe_get(master_df, ROW_TEAM, col_idx)
        }
        operators.append(operator_info)
    
    return operators


def fill_operator_fields(df: pd.DataFrame, operator_info: Dict[str, str], 
                        data_source: str) -> pd.DataFrame:
    """Fill operator-related fields and metadata in the dataframe."""
    df = df.copy()
    
    df['Vending Business Name'] = operator_info.get('vending_business_name', '')
    df['Operator Name'] = operator_info.get('operator_name', '')
    df['Operator Phone #'] = operator_info.get('operator_phone', '')
    df['Operator Email Address'] = operator_info.get('operator_email', '')
    df['Operator Zip Code'] = operator_info.get('operator_zip', '')
    df['Operator Website Address'] = operator_info.get('operator_website', '')
    
    if 'Team' in df.columns and operator_info.get('team'):
        df['Team'] = operator_info['team']
    
    today = datetime.now().strftime('%b %d %Y')
    df['List Source'] = f"{data_source} {today}"
    
    return df


def match_address(search_address: str, records: List[Dict], address_field: str) -> Optional[Dict]:
    """Find matching address in a list of records."""
    if not search_address or not records:
        return None
    
    search_addr = str(search_address).lower().strip()
    
    for record in records:
        record_addr = str(record.get(address_field, "")).lower().strip()
        
        if search_addr == record_addr:
            return record
        if search_addr in record_addr or record_addr in search_addr:
            return record
    
    return None


def match_with_zoho(df: pd.DataFrame, zoho_api: ZohoAPI, progress_callback=None) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
    """Match records with Zoho CRM using batch queries."""
    stats = {
        'deliveries_found': 0,
        'locatings_found': 0,
        'not_found': 0,
        'zoho_enabled': zoho_api.config.zoho_enabled
    }
    
    if not zoho_api.config.zoho_enabled:
        return df, pd.DataFrame(), stats
    
    if not zoho_api.mint_access_token():
        stats['zoho_enabled'] = False
        return df, pd.DataFrame(), stats
    
    df = df.copy()
    
    zip_codes = df['ZIP code'].dropna().astype(str).unique().tolist()
    cities = df['City'].dropna().astype(str).unique().tolist()
    
    if progress_callback:
        progress_callback(0.3, "Querying Zoho CRM Deliveries...")
    
    # Query Deliveries by both city and ZIP (since City field contains "City, State ZIP")
    deliveries_by_term = zoho_api.batch_query_deliveries(cities, zip_codes)
    
    if progress_callback:
        progress_callback(0.5, "Querying Zoho CRM Locatings...")
    
    locatings_by_zip = zoho_api.batch_query_locatings(zip_codes)
    
    if progress_callback:
        progress_callback(0.7, "Matching records...")
    
    found_in_deliveries = []
    found_in_locatings = []
    
    for idx, row in df.iterrows():
        address = str(row.get('Address', ''))
        zip_code = str(row.get('ZIP code', ''))
        city = str(row.get('City', ''))
        
        if not address or (not zip_code and not city):
            continue
        
        # Check Deliveries first - try matching by city OR zip code
        delivery_match = None
        
        # Try city match
        if city and city in deliveries_by_term:
            delivery_match = match_address(
                address, 
                deliveries_by_term[city], 
                zoho_api.config.deliveries_address_field
            )
        
        # If no city match, try ZIP match
        if not delivery_match and zip_code and zip_code in deliveries_by_term:
            delivery_match = match_address(
                address, 
                deliveries_by_term[zip_code], 
                zoho_api.config.deliveries_address_field
            )
        
        if delivery_match:
            found_in_deliveries.append(idx)
            continue
        
        # Check Locatings
        if zip_code and zip_code in locatings_by_zip:
            match = match_address(
                address, 
                locatings_by_zip[zip_code], 
                zoho_api.config.locatings_address_field
            )
            if match:
                record_id = match.get('id')
                url = zoho_api.build_record_url('Locatings', record_id)
                found_in_locatings.append((idx, url))
    
    stats['deliveries_found'] = len(found_in_deliveries)
    stats['locatings_found'] = len(found_in_locatings)
    stats['not_found'] = len(df) - len(found_in_deliveries) - len(found_in_locatings)
    
    if found_in_deliveries:
        df_deliveries = df.loc[found_in_deliveries].copy()
        df = df.drop(found_in_deliveries)
    else:
        df_deliveries = pd.DataFrame()
    
    if found_in_locatings:
        if 'Import Notes' not in df.columns:
            df['Import Notes'] = ''
        for idx, url in found_in_locatings:
            if idx in df.index:
                df.at[idx, 'Import Notes'] = url
    
    return df, df_deliveries, stats


def convert_df_to_csv(df: pd.DataFrame) -> bytes:
    """Convert dataframe to CSV bytes for download."""
    return df.to_csv(index=False).encode('utf-8')


def map_zoominfo_to_template(df_raw: pd.DataFrame, template_df: pd.DataFrame) -> pd.DataFrame:
    """Map ZoomInfo raw data columns to VanillaSoft template format."""
    df_output = pd.DataFrame(columns=template_df.columns)
    
    column_mapping = {
        'Last Name': 'Last Name',
        'First Name': 'First Name',
        'Job Title': 'Title',
        'Direct Phone Number': 'Business',
        'Email Address': 'Email',
        'Mobile phone': 'Mobile',
        'Company Name': 'Company',
        'Website': 'Web site',
        'Company HQ Phone': 'Home',
        'Employees': 'Number of Employees',
        'SIC Code 1': 'Primary SIC',
        'Primary Industry': 'Primary Line of Business',
        'Company Street Address': 'Address',
        'Company City': 'City',
        'Company State': 'State',
        'Company Zip Code': 'ZIP code'
    }
    
    for source_col, target_col in column_mapping.items():
        if source_col in df_raw.columns and target_col in df_output.columns:
            df_output[target_col] = df_raw[source_col]
    
    return df_output


def map_salesgenie_to_template(df_raw: pd.DataFrame, template_df: pd.DataFrame) -> pd.DataFrame:
    """Map SalesGenie raw data columns to VanillaSoft template format."""
    df_output = pd.DataFrame(columns=template_df.columns)
    
    column_mapping = {
        'Company Name': 'Company Name',
        'Phone Number Combined': 'Business',
        'Location Employee Size Range': 'Number of Employees',
        'Company Website': 'Web site',
        'Executive First Name': 'First Name',
        'Executive Last Name': 'Last Name',
        'Executive Title': 'Title',
        'Location Address': 'Address',
        'Location City': 'City',
        'Location State': 'State',
        'Location ZIP Code': 'ZIP code',
        'Primary SIC Code': 'Primary SIC',
        'Primary SIC Code Description': 'Primary Line of Business',
        'Square Footage': 'Square Footage'
    }
    
    for source_col, target_col in column_mapping.items():
        if source_col in df_raw.columns and target_col in df_output.columns:
            df_output[target_col] = df_raw[source_col]
    
    return df_output


# =============================================================================
# STREAMLIT APP
# =============================================================================

def main():
    # Initialize session state
    if 'processing_complete' not in st.session_state:
        st.session_state.processing_complete = False
    if 'results' not in st.session_state:
        st.session_state.results = None
    if 'operators_loaded' not in st.session_state:
        st.session_state.operators_loaded = []
    if 'selected_operator' not in st.session_state:
        st.session_state.selected_operator = None
    
    # Header
    st.markdown('<p class="main-header">🚀 VanillaSoft Data Preparation</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Prepare ZoomInfo/SalesGenie data for VanillaSoft upload</p>', unsafe_allow_html=True)
    
    # Initialize config
    config = Config()
    
    # Zoho status indicator
    if config.zoho_enabled:
        st.success("✅ Zoho CRM integration enabled")
    else:
        st.warning("⚠️ Zoho CRM integration disabled (credentials not configured)")
    
    # Show call center agents info
    with st.expander("👥 Call Center Agents (Contact Owners)", expanded=False):
        st.caption("Records will be distributed evenly among these agents:")
        for i, agent in enumerate(CALL_CENTER_AGENTS, 1):
            st.write(f"{i}. {agent}")
    
    st.divider()
    
    # ==========================================================================
    # FILE UPLOAD SECTION
    # ==========================================================================
    
    st.subheader("📁 Step 1: Upload Raw Data")
    
    raw_file = st.file_uploader(
        "Raw Data (CSV)",
        type=['csv'],
        help="Upload your raw ZoomInfo or SalesGenie CSV export"
    )
    
    # ==========================================================================
    # AUTO-DETECT DATA SOURCE
    # ==========================================================================
    
    data_source = None
    
    if raw_file is not None:
        # Read just the header to detect source
        raw_file.seek(0)  # Reset file pointer
        df_header = pd.read_csv(raw_file, nrows=0)
        raw_file.seek(0)  # Reset again for later use
        
        # Check for ZoomInfo-specific column
        if "Job Title Hierarchy Level" in df_header.columns:
            data_source = "ZoomInfo"
        else:
            data_source = "SalesGenie"
        
        st.success(f"✅ Detected data source: **{data_source}**")
    
    st.divider()
    
    # ==========================================================================
    # OPERATOR DATA ENTRY MODE
    # ==========================================================================
    
    st.subheader("👤 Step 2: Operator Information")
    
    # Initialize operator storage
    operator_storage = get_operator_storage()
    
    # Build mode options based on what's available
    mode_options = ["Upload Master Excel File", "Manual Entry", "Load from File"]
    if operator_storage.is_configured:
        mode_options.insert(0, "☁️ Saved Operators")  # Cloud option first if available
    
    operator_mode = st.radio(
        "How would you like to provide operator data?",
        options=mode_options,
        horizontal=True,
        help="Choose how to input operator information"
    )
    
    operator_info = None
    
    # ---------------------------------------------------------------------
    # MODE: Saved Operators (Google Sheets Cloud Storage)
    # ---------------------------------------------------------------------
    if operator_mode == "☁️ Saved Operators":
        saved_operators = operator_storage.get_all_operators()
        
        if saved_operators:
            # Create display names for dropdown
            def format_saved_operator(op):
                name = op.get('operator_name', 'Unknown')
                business = op.get('vending_business_name', '')
                if business and business != 'N/A':
                    return f"{name} ({business})"
                return name
            
            operator_names = [format_saved_operator(op) for op in saved_operators]
            
            selected_idx = st.selectbox(
                "Select Saved Operator",
                options=range(len(saved_operators)),
                index=len(saved_operators) - 1,  # Default to most recent
                format_func=lambda x: operator_names[x],
                help="Choose from your saved operators"
            )
            
            selected_saved = saved_operators[selected_idx]
            
            st.caption("📝 Edit operator details if needed:")
            
            col1, col2 = st.columns(2)
            
            with col1:
                saved_business_name = st.text_input(
                    "Vending Business Name",
                    value=selected_saved.get('vending_business_name', 'N/A'),
                    key=f"saved_business_{selected_idx}"
                )
                saved_operator_name = st.text_input(
                    "Operator Name",
                    value=selected_saved.get('operator_name', ''),
                    key=f"saved_name_{selected_idx}"
                )
                saved_phone = st.text_input(
                    "Operator Phone",
                    value=selected_saved.get('operator_phone', 'N/A'),
                    key=f"saved_phone_{selected_idx}"
                )
                saved_team = st.text_input(
                    "Team",
                    value=selected_saved.get('team', 'N/A'),
                    key=f"saved_team_{selected_idx}"
                )
            
            with col2:
                saved_email = st.text_input(
                    "Operator Email",
                    value=selected_saved.get('operator_email', 'N/A'),
                    key=f"saved_email_{selected_idx}"
                )
                saved_zip = st.text_input(
                    "Operator ZIP Code",
                    value=selected_saved.get('operator_zip', 'N/A'),
                    key=f"saved_zip_{selected_idx}"
                )
                saved_website = st.text_input(
                    "Operator Website",
                    value=selected_saved.get('operator_website', 'N/A'),
                    key=f"saved_website_{selected_idx}"
                )
            
            operator_info = {
                'vending_business_name': saved_business_name,
                'operator_name': saved_operator_name,
                'operator_phone': saved_phone,
                'operator_email': saved_email,
                'operator_zip': saved_zip,
                'operator_website': saved_website,
                'team': saved_team
            }
            
            # Management buttons
            st.divider()
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("💾 Update Saved", use_container_width=True, help="Save changes to this operator"):
                    if operator_storage.save_operator(operator_info):
                        st.success("✅ Operator updated!")
                        st.rerun()
            
            with col2:
                if st.button("🗑️ Delete", use_container_width=True, type="secondary", help="Remove this operator"):
                    if operator_storage.delete_operator(selected_saved.get('operator_name')):
                        st.success("✅ Operator deleted!")
                        st.rerun()
            
            with col3:
                # Download as JSON backup
                operator_json = json.dumps(operator_info, indent=2)
                st.download_button(
                    label="📥 Export JSON",
                    data=operator_json,
                    file_name=f"{saved_operator_name.replace(' ', '_')}_operator.json",
                    mime="application/json",
                    use_container_width=True
                )
        else:
            st.info("📭 No saved operators yet. Use 'Manual Entry' to create one and save it to the cloud.")
    
    # ---------------------------------------------------------------------
    # MODE 1: Upload Master Excel File
    # ---------------------------------------------------------------------
    if operator_mode == "Upload Master Excel File":
        master_file = st.file_uploader(
            "Master Data (Excel)",
            type=['xlsx', 'xls'],
            help="Upload the Master Data Excel file with operator info"
        )
        
        if master_file is not None:
            # Load operators from last 5 columns
            operators = load_master_data_multi(master_file, num_columns=5)
            st.session_state.operators_loaded = operators
            
            if operators:
                # Create display names for dropdown
                def format_operator_name(op):
                    name = op['operator_name']
                    business = op['vending_business_name']
                    if business and business != 'N/A':
                        return f"{name} ({business})"
                    return name
                
                operator_names = [format_operator_name(op) for op in operators]
                
                selected_idx = st.selectbox(
                    "Select Operator",
                    options=range(len(operators)),
                    index=len(operators) - 1,  # Default to last column
                    format_func=lambda x: operator_names[x],
                    help="Choose which operator's information to use"
                )
                
                selected_op = operators[selected_idx]
                
                st.caption("📝 Edit operator details below if needed:")
                
                # Editable fields in two columns
                col1, col2 = st.columns(2)
                
                with col1:
                    edited_business_name = st.text_input(
                        "Vending Business Name",
                        value=selected_op['vending_business_name'],
                        key=f"excel_business_name_{selected_idx}"
                    )
                    edited_operator_name = st.text_input(
                        "Operator Name",
                        value=selected_op['operator_name'],
                        key=f"excel_operator_name_{selected_idx}"
                    )
                    edited_phone = st.text_input(
                        "Operator Phone",
                        value=selected_op['operator_phone'],
                        key=f"excel_phone_{selected_idx}"
                    )
                    edited_team = st.text_input(
                        "Team",
                        value=selected_op.get('team', 'N/A'),
                        key=f"excel_team_{selected_idx}"
                    )
                
                with col2:
                    edited_email = st.text_input(
                        "Operator Email",
                        value=selected_op['operator_email'],
                        key=f"excel_email_{selected_idx}"
                    )
                    edited_zip = st.text_input(
                        "Operator ZIP Code",
                        value=selected_op['operator_zip'],
                        key=f"excel_zip_{selected_idx}"
                    )
                    edited_website = st.text_input(
                        "Operator Website",
                        value=selected_op['operator_website'],
                        key=f"excel_website_{selected_idx}"
                    )
                
                # Build operator_info from edited values
                operator_info = {
                    'vending_business_name': edited_business_name,
                    'operator_name': edited_operator_name,
                    'operator_phone': edited_phone,
                    'operator_email': edited_email,
                    'operator_zip': edited_zip,
                    'operator_website': edited_website,
                    'team': edited_team
                }
                
                # Option to save this operator for later
                with st.expander("💾 Save this operator for future use", expanded=False):
                    col1, col2 = st.columns(2)
                    
                    # Cloud save (if configured)
                    with col1:
                        if operator_storage.is_configured:
                            if st.button("☁️ Save to Cloud", use_container_width=True, key="excel_cloud_save"):
                                if operator_storage.save_operator(operator_info):
                                    st.success("✅ Saved to cloud!")
                        else:
                            st.caption("☁️ Cloud storage not configured")
                    
                    # JSON download
                    with col2:
                        operator_json = json.dumps(operator_info, indent=2)
                        safe_name = edited_operator_name.replace(' ', '_').replace('&', 'and')
                        save_filename = f"{safe_name}_operator.json"
                        
                        st.download_button(
                            label="📥 Download JSON",
                            data=operator_json,
                            file_name=save_filename,
                            mime="application/json",
                            use_container_width=True
                        )
            else:
                st.warning("⚠️ No valid operator data found in the last 5 columns of the Master Data file.")
    
    # ---------------------------------------------------------------------
    # MODE 2: Manual Entry
    # ---------------------------------------------------------------------
    elif operator_mode == "Manual Entry":
        st.caption("📝 Enter operator details manually:")
        
        col1, col2 = st.columns(2)
        
        with col1:
            manual_business_name = st.text_input(
                "Vending Business Name",
                value="",
                key="manual_business_name",
                placeholder="e.g., ABC Vending LLC"
            )
            manual_operator_name = st.text_input(
                "Operator Name",
                value="",
                key="manual_operator_name",
                placeholder="e.g., John Smith"
            )
            manual_phone = st.text_input(
                "Operator Phone",
                value="",
                key="manual_phone",
                placeholder="e.g., 555-123-4567"
            )
            manual_team = st.text_input(
                "Team",
                value="",
                key="manual_team",
                placeholder="e.g., John Smith (TX)"
            )
        
        with col2:
            manual_email = st.text_input(
                "Operator Email",
                value="",
                key="manual_email",
                placeholder="e.g., john@abcvending.com"
            )
            manual_zip = st.text_input(
                "Operator ZIP Code",
                value="",
                key="manual_zip",
                placeholder="e.g., 75001"
            )
            manual_website = st.text_input(
                "Operator Website",
                value="",
                key="manual_website",
                placeholder="e.g., abcvending.com"
            )
        
        # Validate required field (operator name)
        if manual_operator_name.strip():
            operator_info = {
                'vending_business_name': manual_business_name.strip() or 'N/A',
                'operator_name': manual_operator_name.strip(),
                'operator_phone': manual_phone.strip() or 'N/A',
                'operator_email': manual_email.strip() or 'N/A',
                'operator_zip': manual_zip.strip() or 'N/A',
                'operator_website': manual_website.strip() or 'N/A',
                'team': manual_team.strip() or 'N/A'
            }
            
            # Option to save this operator for later
            with st.expander("💾 Save this operator for future use", expanded=False):
                col1, col2 = st.columns(2)
                
                # Cloud save (if configured)
                with col1:
                    if operator_storage.is_configured:
                        if st.button("☁️ Save to Cloud", use_container_width=True, key="manual_cloud_save"):
                            if operator_storage.save_operator(operator_info):
                                st.success("✅ Saved to cloud!")
                    else:
                        st.caption("☁️ Cloud storage not configured")
                
                # JSON download
                with col2:
                    operator_json = json.dumps(operator_info, indent=2)
                    safe_name = manual_operator_name.strip().replace(' ', '_').replace('&', 'and')
                    save_filename = f"{safe_name}_operator.json"
                    
                    st.download_button(
                        label="📥 Download JSON",
                        data=operator_json,
                        file_name=save_filename,
                        mime="application/json",
                        use_container_width=True
                    )
        else:
            st.info("👆 Please enter at least the Operator Name to continue.")
    
    # ---------------------------------------------------------------------
    # MODE 3: Load from File
    # ---------------------------------------------------------------------
    elif operator_mode == "Load from File":
        st.caption("📂 Load a previously saved operator JSON file:")
        
        saved_operator_file = st.file_uploader(
            "Operator JSON File",
            type=['json'],
            help="Upload a previously saved operator JSON file"
        )
        
        if saved_operator_file is not None:
            try:
                loaded_operator = json.load(saved_operator_file)
                
                st.success(f"✅ Loaded operator: **{loaded_operator.get('operator_name', 'Unknown')}**")
                
                st.caption("📝 Edit operator details if needed:")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    loaded_business_name = st.text_input(
                        "Vending Business Name",
                        value=loaded_operator.get('vending_business_name', 'N/A'),
                        key="loaded_business_name"
                    )
                    loaded_operator_name = st.text_input(
                        "Operator Name",
                        value=loaded_operator.get('operator_name', ''),
                        key="loaded_operator_name"
                    )
                    loaded_phone = st.text_input(
                        "Operator Phone",
                        value=loaded_operator.get('operator_phone', 'N/A'),
                        key="loaded_phone"
                    )
                    loaded_team = st.text_input(
                        "Team",
                        value=loaded_operator.get('team', 'N/A'),
                        key="loaded_team"
                    )
                
                with col2:
                    loaded_email = st.text_input(
                        "Operator Email",
                        value=loaded_operator.get('operator_email', 'N/A'),
                        key="loaded_email"
                    )
                    loaded_zip = st.text_input(
                        "Operator ZIP Code",
                        value=loaded_operator.get('operator_zip', 'N/A'),
                        key="loaded_zip"
                    )
                    loaded_website = st.text_input(
                        "Operator Website",
                        value=loaded_operator.get('operator_website', 'N/A'),
                        key="loaded_website"
                    )
                
                operator_info = {
                    'vending_business_name': loaded_business_name,
                    'operator_name': loaded_operator_name,
                    'operator_phone': loaded_phone,
                    'operator_email': loaded_email,
                    'operator_zip': loaded_zip,
                    'operator_website': loaded_website,
                    'team': loaded_team
                }
                
                # Option to save edited operator
                with st.expander("💾 Save edited operator", expanded=False):
                    col1, col2 = st.columns(2)
                    
                    # Cloud save (if configured)
                    with col1:
                        if operator_storage.is_configured:
                            if st.button("☁️ Save to Cloud", use_container_width=True, key="loaded_cloud_save"):
                                if operator_storage.save_operator(operator_info):
                                    st.success("✅ Saved to cloud!")
                        else:
                            st.caption("☁️ Cloud storage not configured")
                    
                    # JSON download
                    with col2:
                        operator_json = json.dumps(operator_info, indent=2)
                        safe_name = loaded_operator_name.replace(' ', '_').replace('&', 'and')
                        save_filename = f"{safe_name}_operator.json"
                        
                        st.download_button(
                            label="📥 Download JSON",
                            data=operator_json,
                            file_name=save_filename,
                            mime="application/json",
                            use_container_width=True
                        )
                
            except json.JSONDecodeError:
                st.error("❌ Invalid JSON file. Please upload a valid operator file.")
            except Exception as e:
                st.error(f"❌ Error loading file: {str(e)}")
    
    st.divider()
    
    # ==========================================================================
    # VALIDATION
    # ==========================================================================
    
    all_inputs_valid = (
        raw_file is not None and 
        operator_info is not None and
        data_source is not None
    )
    
    if not all_inputs_valid:
        st.info("👆 Please upload raw data and provide operator information to continue.")
        
        missing = []
        if raw_file is None:
            missing.append("Raw CSV file")
        if operator_info is None:
            if operator_mode == "☁️ Saved Operators":
                missing.append("Select a saved operator")
            elif operator_mode == "Upload Master Excel File":
                missing.append("Master Data Excel file with valid operator")
            elif operator_mode == "Manual Entry":
                missing.append("Operator Name (required)")
            elif operator_mode == "Load from File":
                missing.append("Operator JSON file")
        
        if missing:
            st.caption("Missing: " + ", ".join(missing))
        
        st.session_state.processing_complete = False
        st.session_state.results = None
        return
    
    # ==========================================================================
    # PROCESS BUTTON
    # ==========================================================================
    
    st.subheader("🚀 Step 3: Process Data")
    
    process_button = st.button("▶️ Process Data", type="primary", use_container_width=True)
    
    if process_button:
        st.session_state.processing_complete = False
        st.session_state.results = None
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        try:
            status_text.text("Loading files...")
            progress_bar.progress(0.1)
            
            # Build template schema
            template_df = build_template_df(data_source)
            
            # Load raw data (reset file pointer since we read it earlier for detection)
            raw_file.seek(0)
            df_raw = pd.read_csv(raw_file)
            initial_count = len(df_raw)
            
            phone_column = 'Direct Phone Number' if data_source == 'ZoomInfo' else 'Phone Number Combined'
            
            progress_bar.progress(0.2)
            status_text.text("Cleaning phone numbers...")
            
            df_raw, extensions_removed = clean_phone_dataframe(df_raw, phone_column)
            df_raw, duplicates_removed = remove_duplicate_phones(df_raw, phone_column)
            
            progress_bar.progress(0.3)
            status_text.text("Mapping data to template...")
            
            if data_source == 'ZoomInfo':
                df_mapped = map_zoominfo_to_template(df_raw, template_df)
            else:
                df_mapped = map_salesgenie_to_template(df_raw, template_df)
            
            # Fill operator fields (without contact owner - that's done separately)
            df_mapped = fill_operator_fields(df_mapped, operator_info, data_source)
            
            # Assign contact owners round-robin
            status_text.text("Assigning contact owners...")
            df_mapped = assign_contact_owners(df_mapped, CALL_CENTER_AGENTS)
            
            progress_bar.progress(0.4)
            status_text.text("Matching with Zoho CRM...")
            
            zoho_api = ZohoAPI(config)
            
            def update_progress(value, text):
                progress_bar.progress(0.4 + value * 0.5)
                status_text.text(text)
            
            df_final, df_deliveries, zoho_stats = match_with_zoho(
                df_mapped, 
                zoho_api, 
                progress_callback=update_progress
            )
            
            # Enforce output schema
            df_final = enforce_output_schema(df_final, data_source)
            if not df_deliveries.empty:
                df_deliveries = enforce_output_schema(df_deliveries, data_source)
            
            progress_bar.progress(1.0)
            status_text.text("✅ Processing complete!")
            
            clean_operator_name = re.sub(r'[^a-zA-Z0-9_-]', '_', operator_info.get('operator_name', 'Unknown'))
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            st.session_state.processing_complete = True
            st.session_state.results = {
                'df_final': df_final,
                'df_deliveries': df_deliveries,
                'initial_count': initial_count,
                'extensions_removed': extensions_removed,
                'duplicates_removed': duplicates_removed,
                'zoho_stats': zoho_stats,
                'operator_info': operator_info,
                'data_source': data_source,
                'main_filename': f"{clean_operator_name}_{data_source}_{timestamp}.csv",
                'deliveries_filename': f"{clean_operator_name}_{data_source}_Deliveries_{timestamp}.csv"
            }
            
            st.balloons()
            
        except Exception as e:
            st.error(f"❌ An error occurred: {str(e)}")
            st.exception(e)
            st.session_state.processing_complete = False
            st.session_state.results = None
    
    # ==========================================================================
    # RESULTS SECTION
    # ==========================================================================
    
    if st.session_state.processing_complete and st.session_state.results:
        results = st.session_state.results
        
        st.divider()
        
        st.markdown("""
        <div style="padding: 1.5rem; background: linear-gradient(135deg, #28a745 0%, #20c997 100%); 
                    border-radius: 10px; text-align: center; margin-bottom: 1rem;">
            <h2 style="color: white; margin: 0;">✅ Processing Complete!</h2>
            <p style="color: white; margin: 0.5rem 0 0 0; opacity: 0.9;">Your files are ready to download</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.subheader("📊 Results Summary")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Initial Records", results['initial_count'])
        
        with col2:
            st.metric("Extensions Removed", results['extensions_removed'])
        
        with col3:
            st.metric("Duplicates Removed", results['duplicates_removed'])
        
        with col4:
            st.metric("Final Records", len(results['df_final']), 
                     delta=f"-{results['initial_count'] - len(results['df_final'])}" if results['initial_count'] > len(results['df_final']) else None)
        
        # Agent distribution summary
        if 'Contact Owner' in results['df_final'].columns:
            st.info(f"**👥 Contact Owner Distribution:** Records distributed among {len(CALL_CENTER_AGENTS)} agents (round-robin)")
        
        if results['zoho_stats']['zoho_enabled']:
            st.info(f"""
            **🔍 Zoho CRM Matching Results:**
            - ✅ Found in Deliveries: **{results['zoho_stats']['deliveries_found']}** records (separated to Deliveries file)
            - ✅ Found in Locatings: **{results['zoho_stats']['locatings_found']}** records (URLs added to Import Notes)
            - ⚪ Not found in CRM: **{results['zoho_stats']['not_found']}** records
            """)
        
        st.divider()
        
        st.subheader("⬇️ Download Your Files")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"""
            <div style="padding: 1rem; background-color: #e7f3ff; border-radius: 8px; 
                        border: 2px solid #0066cc; margin-bottom: 0.5rem;">
                <h4 style="margin: 0; color: #0066cc;">📄 Main File</h4>
                <p style="margin: 0.25rem 0; color: #666;">{len(results['df_final'])} records</p>
            </div>
            """, unsafe_allow_html=True)
            st.download_button(
                label="📥 Download Main CSV",
                data=convert_df_to_csv(results['df_final']),
                file_name=results['main_filename'],
                mime='text/csv',
                use_container_width=True,
                type="primary"
            )
        
        with col2:
            if not results['df_deliveries'].empty:
                st.markdown(f"""
                <div style="padding: 1rem; background-color: #fff3e6; border-radius: 8px; 
                            border: 2px solid #ff9900; margin-bottom: 0.5rem;">
                    <h4 style="margin: 0; color: #ff9900;">📄 Deliveries File</h4>
                    <p style="margin: 0.25rem 0; color: #666;">{len(results['df_deliveries'])} records</p>
                </div>
                """, unsafe_allow_html=True)
                st.download_button(
                    label="📥 Download Deliveries CSV",
                    data=convert_df_to_csv(results['df_deliveries']),
                    file_name=results['deliveries_filename'],
                    mime='text/csv',
                    use_container_width=True,
                    type="secondary"
                )
            else:
                st.markdown("""
                <div style="padding: 1rem; background-color: #f0f0f0; border-radius: 8px; 
                            border: 2px solid #ccc; margin-bottom: 0.5rem;">
                    <h4 style="margin: 0; color: #666;">📄 Deliveries File</h4>
                    <p style="margin: 0.25rem 0; color: #999;">No deliveries matches found</p>
                </div>
                """, unsafe_allow_html=True)
                st.button("No Deliveries to Download", disabled=True, use_container_width=True)
        
        st.divider()
        
        st.subheader("📋 Operator Information Used")
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**Business Name:** {results['operator_info'].get('vending_business_name', 'N/A')}")
            st.write(f"**Operator:** {results['operator_info'].get('operator_name', 'N/A')}")
            st.write(f"**Phone:** {results['operator_info'].get('operator_phone', 'N/A')}")
        with col2:
            st.write(f"**Email:** {results['operator_info'].get('operator_email', 'N/A')}")
            st.write(f"**ZIP:** {results['operator_info'].get('operator_zip', 'N/A')}")
            st.write(f"**Website:** {results['operator_info'].get('operator_website', 'N/A')}")
        
        st.divider()
        st.subheader("👀 Data Preview")
        
        if not results['df_deliveries'].empty:
            tab1, tab2 = st.tabs(["Main Data", "Deliveries Data"])
            
            with tab1:
                st.dataframe(results['df_final'].head(20), use_container_width=True)
                st.caption(f"Showing first 20 of {len(results['df_final'])} records")
            
            with tab2:
                st.dataframe(results['df_deliveries'].head(20), use_container_width=True)
                st.caption(f"Showing first 20 of {len(results['df_deliveries'])} records")
        else:
            st.dataframe(results['df_final'].head(20), use_container_width=True)
            st.caption(f"Showing first 20 of {len(results['df_final'])} records")
        
        st.divider()
        if st.button("🔄 Process Another File", use_container_width=True):
            st.session_state.processing_complete = False
            st.session_state.results = None
            st.rerun()


if __name__ == "__main__":
    main()