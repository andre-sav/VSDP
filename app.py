#!/usr/bin/env python3
"""
VanillaSoft Data Preparation - Streamlit Web App

A user-friendly web interface for preparing ZoomInfo/SalesGenie data
for VanillaSoft upload with Zoho CRM matching.

Usage:
    streamlit run vanillasoft_streamlit_app.py

Requirements:
    pip install streamlit pandas requests python-dotenv openpyxl
"""

import io
import os
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st


# =============================================================================
# OUTPUT SCHEMA (HARDCODED FROM OFFICIAL TEMPLATES)
# =============================================================================
# Output CSV must match the selected data source template's columns (order preserved),
# plus ONE additional column: "Import Notes" (Zoho Locatings URL if match found; otherwise blank).

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

    # ensure desired columns exist
    for c in desired:
        if c not in df.columns:
            df[c] = ""

    # drop extras + reorder
    return df[desired].copy()

from dotenv import load_dotenv

# Load environment variables
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

# Custom CSS for better styling
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
        # Template paths (stored in assets folder)
        self.assets_folder = "assets"
        self.zoominfo_template_path = os.path.join(self.assets_folder, "ZoomInfo_Default_List.csv")
        self.salesgenie_template_path = os.path.join(self.assets_folder, "Sales_Genie_Default_List.csv")
        
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
    
    def get_template_path(self, data_source: str) -> str:
        """Get the template path for the given data source."""
        if data_source == "ZoomInfo":
            return self.zoominfo_template_path
        else:
            return self.salesgenie_template_path


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

    def batch_query_deliveries(self, cities: List[str]) -> Dict[str, List[Dict]]:
        """Query multiple cities in one API call using IN operator."""
        if not cities or not self.access_token:
            return {}
        
        results = {}
        batch_size = 50
        
        for i in range(0, len(cities), batch_size):
            batch = cities[i:i + batch_size]
            in_values = ", ".join([f"'{str(c).replace(chr(39), chr(39)+chr(39))}'" for c in batch])
            
            query = f"""
                select id, {self.config.deliveries_address_field}, {self.config.deliveries_city_field}
                from Deliveries
                where {self.config.deliveries_city_field} in ({in_values})
                limit 2000
            """.strip()
            
            response = self.execute_coql(query)
            
            for record in response.get("data", []):
                city = str(record.get(self.config.deliveries_city_field, ""))
                if city:
                    if city not in results:
                        results[city] = []
                    results[city].append(record)
        
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
    """Remove duplicate phone numbers, keeping only the first occurrence."""
    if phone_column not in df.columns:
        return df, 0
    
    initial_count = len(df)
    df_clean = df.drop_duplicates(subset=[phone_column], keep='first')
    duplicates_removed = initial_count - len(df_clean)
    
    return df_clean, duplicates_removed


def load_master_data(master_file) -> Dict[str, str]:
    """Load operator metadata from Master Data Excel file."""
    master_df = pd.read_excel(master_file)
    
    last_column = master_df.iloc[:, -1]
    non_null_values = last_column[last_column.notna()].tolist()
    
    if len(non_null_values) >= 6:
        operator_info = {
            'vending_business_name': str(non_null_values[0]),
            'operator_name': str(non_null_values[1]),
            'operator_phone': str(non_null_values[2]),
            'operator_email': str(non_null_values[3]),
            'operator_zip': str(non_null_values[4]),
            'operator_website': str(non_null_values[5]),
            'team': str(non_null_values[6]) if len(non_null_values) > 6 else ''
        }
    else:
        operator_info = {
            'vending_business_name': '',
            'operator_name': '',
            'operator_phone': '',
            'operator_email': '',
            'operator_zip': '',
            'operator_website': '',
            'team': ''
        }
    
    return operator_info


def load_template(filepath: str) -> pd.DataFrame:
    """Load the template CSV file from the given path."""
    return pd.read_csv(filepath)


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


def fill_operator_fields(df: pd.DataFrame, operator_info: Dict[str, str], 
                        contact_owner: str, data_source: str) -> pd.DataFrame:
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
    
    df['Contact Owner'] = contact_owner
    
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
    
    # Collect unique ZIP codes and cities
    zip_codes = df['ZIP code'].dropna().astype(str).unique().tolist()
    cities = df['City'].dropna().astype(str).unique().tolist()
    
    if progress_callback:
        progress_callback(0.3, "Querying Zoho CRM Deliveries...")
    
    deliveries_by_city = zoho_api.batch_query_deliveries(cities)
    
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
        
        # Check Deliveries first
        if city and city in deliveries_by_city:
            match = match_address(
                address, 
                deliveries_by_city[city], 
                zoho_api.config.deliveries_address_field
            )
            if match:
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
    
    # Update stats
    stats['deliveries_found'] = len(found_in_deliveries)
    stats['locatings_found'] = len(found_in_locatings)
    stats['not_found'] = len(df) - len(found_in_deliveries) - len(found_in_locatings)
    
    # Separate deliveries records
    if found_in_deliveries:
        df_deliveries = df.loc[found_in_deliveries].copy()
        df = df.drop(found_in_deliveries)
    else:
        df_deliveries = pd.DataFrame()
    
    # Add Import Notes for Locatings matches
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


# =============================================================================
# STREAMLIT APP
# =============================================================================

def main():
    # Initialize session state for results
    if 'processing_complete' not in st.session_state:
        st.session_state.processing_complete = False
    if 'results' not in st.session_state:
        st.session_state.results = None
    
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
    
    st.divider()
    
    # ==========================================================================
    # INPUT SECTION
    # ==========================================================================
    
    st.subheader("📝 Step 1: Enter Details")
    
    col1, col2 = st.columns(2)
    
    with col1:
        data_source = st.selectbox(
            "Data Source",
            options=["SalesGenie", "ZoomInfo"],
            help="Select the source of your raw data"
        )
    
    with col2:
        contact_owner_email = st.text_input(
            "Contact Owner Email",
            placeholder="name@company.com",
            help="Email address of the contact owner"
        )
    
    st.divider()
    
    # ==========================================================================
    # FILE UPLOAD SECTION
    # ==========================================================================
    
    st.subheader("📁 Step 2: Upload Files")
    
    col1, col2 = st.columns(2)
    
    with col1:
        raw_file = st.file_uploader(
            f"Raw {data_source} Data (CSV)",
            type=['csv'],
            help=f"Upload your raw {data_source} CSV export"
        )
    
    with col2:
        master_file = st.file_uploader(
            "Master Data (Excel)",
            type=['xlsx', 'xls'],
            help="Upload the Master Data Excel file with operator info"
        )
    
    st.divider()
    
    # ==========================================================================
    # VALIDATION
    # ==========================================================================
    
    # Check if all inputs are provided
    all_inputs_valid = (
        contact_owner_email and 
        '@' in contact_owner_email and
        raw_file is not None and 
        master_file is not None
    )
    
    if not all_inputs_valid:
        st.info("👆 Please fill in all fields and upload all required files to continue.")
        
        # Show what's missing
        missing = []
        if not contact_owner_email or '@' not in contact_owner_email:
            missing.append("Valid contact owner email")
        if raw_file is None:
            missing.append(f"Raw {data_source} CSV file")
        if master_file is None:
            missing.append("Master Data Excel file")
        
        if missing:
            st.caption("Missing: " + ", ".join(missing))
        
        # Clear previous results if inputs changed
        st.session_state.processing_complete = False
        st.session_state.results = None
        return
    
    # ==========================================================================
    # PROCESS BUTTON
    # ==========================================================================
    
    st.subheader("🚀 Step 3: Process Data")
    
    process_button = st.button("▶️ Process Data", type="primary", use_container_width=True)
    
    if process_button:
        # Clear previous results
        st.session_state.processing_complete = False
        st.session_state.results = None
        
        # Progress bar
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        try:
            # Step 1: Load files
            status_text.text("Loading files...")
            progress_bar.progress(0.1)
            
            # Load master data
            operator_info = load_master_data(master_file)
            
            # Build template schema (hardcoded; no template files needed)
            template_df = build_template_df(data_source)

# Load raw data
            df_raw = pd.read_csv(raw_file)
            initial_count = len(df_raw)
            
            # Determine phone column
            phone_column = 'Direct Phone Number' if data_source == 'ZoomInfo' else 'Phone Number Combined'
            
            progress_bar.progress(0.2)
            status_text.text("Cleaning phone numbers...")
            
            # Step 2: Clean phone numbers
            df_raw, extensions_removed = clean_phone_dataframe(df_raw, phone_column)
            
            # Step 3: Remove duplicates
            df_raw, duplicates_removed = remove_duplicate_phones(df_raw, phone_column)
            
            progress_bar.progress(0.3)
            status_text.text("Mapping data to template...")
            
            # Step 4: Map to template
            if data_source == 'ZoomInfo':
                df_mapped = map_zoominfo_to_template(df_raw, template_df)
            else:
                df_mapped = map_salesgenie_to_template(df_raw, template_df)
            
            # Step 5: Fill operator fields
            df_mapped = fill_operator_fields(
                df_mapped, 
                operator_info, 
                contact_owner_email, 
                data_source
            )
            
            progress_bar.progress(0.4)
            status_text.text("Matching with Zoho CRM...")
            
            # Step 6: Match with Zoho CRM
            zoho_api = ZohoAPI(config)
            
            def update_progress(value, text):
                progress_bar.progress(0.4 + value * 0.5)
                status_text.text(text)
            
            df_final, df_deliveries, zoho_stats = match_with_zoho(
                df_mapped, 
                zoho_api, 
                progress_callback=update_progress
            )

            # Enforce output schema: template columns + 'Import Notes' only
            df_final = enforce_output_schema(df_final, data_source)
            if not df_deliveries.empty:
                df_deliveries = enforce_output_schema(df_deliveries, data_source)

            
            progress_bar.progress(1.0)
            status_text.text("✅ Processing complete!")
            
            # Generate filenames
            clean_operator_name = re.sub(r'[^a-zA-Z0-9_-]', '_', operator_info.get('operator_name', 'Unknown'))
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Store results in session state
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
            
            # Show balloons
            st.balloons()
            
        except Exception as e:
            st.error(f"❌ An error occurred: {str(e)}")
            st.exception(e)
            st.session_state.processing_complete = False
            st.session_state.results = None
    
    # ==========================================================================
    # RESULTS SECTION (Always visible when complete)
    # ==========================================================================
    
    if st.session_state.processing_complete and st.session_state.results:
        results = st.session_state.results
        
        st.divider()
        
        # Big success banner
        st.markdown("""
        <div style="padding: 1.5rem; background: linear-gradient(135deg, #28a745 0%, #20c997 100%); 
                    border-radius: 10px; text-align: center; margin-bottom: 1rem;">
            <h2 style="color: white; margin: 0;">✅ Processing Complete!</h2>
            <p style="color: white; margin: 0.5rem 0 0 0; opacity: 0.9;">Your files are ready to download</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Summary metrics in a highlighted box
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
        
        # Zoho matching results
        if results['zoho_stats']['zoho_enabled']:
            st.info(f"""
            **🔍 Zoho CRM Matching Results:**
            - ✅ Found in Deliveries: **{results['zoho_stats']['deliveries_found']}** records (separated to Deliveries file)
            - ✅ Found in Locatings: **{results['zoho_stats']['locatings_found']}** records (URLs added to Import Notes)
            - ⚪ Not found in CRM: **{results['zoho_stats']['not_found']}** records
            """)
        
        st.divider()
        
        # =================================================================
        # DOWNLOAD SECTION - Prominently displayed
        # =================================================================
        
        st.subheader("⬇️ Download Your Files")
        
        # Main download buttons - large and prominent
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
        
        # Operator info - visible by default
        st.subheader("📋 Operator Information")
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"**Business Name:** {results['operator_info'].get('vending_business_name', 'N/A')}")
            st.write(f"**Operator:** {results['operator_info'].get('operator_name', 'N/A')}")
            st.write(f"**Phone:** {results['operator_info'].get('operator_phone', 'N/A')}")
        with col2:
            st.write(f"**Email:** {results['operator_info'].get('operator_email', 'N/A')}")
            st.write(f"**ZIP:** {results['operator_info'].get('operator_zip', 'N/A')}")
            st.write(f"**Website:** {results['operator_info'].get('operator_website', 'N/A')}")
        
        # Data previews - in tabs for easy access
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
        
        # Process another file button
        st.divider()
        if st.button("🔄 Process Another File", use_container_width=True):
            st.session_state.processing_complete = False
            st.session_state.results = None
            st.rerun()


if __name__ == "__main__":
    main()
