#!/usr/bin/env python3
"""
VanillaSoft Data Preparation Automation Script

This script automates the process of preparing contact data from ZoomInfo or SalesGenie
for upload to VanillaSoft, including Zoho CRM matching.

Features:
- Interactive prompts for user configuration
- Phone number cleaning (extension removal, deduplication)
- Column mapping to VanillaSoft template format
- Optimized Zoho CRM integration with batch queries
- Automated export with proper naming

Usage:
    python vanillasoft_automation.py

Requirements:
    pip install pandas requests python-dotenv openpyxl

Environment Variables (optional, for Zoho CRM integration):
    ZOHO_CLIENT_ID
    ZOHO_CLIENT_SECRET
    ZOHO_REFRESH_TOKEN
    ZOHO_API_BASE (default: https://www.zohoapis.com/crm/v8)
    ZOHO_ACCOUNTS_URL (default: https://accounts.zoho.com)
"""

import os
import re
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


# =============================================================================
# CONFIGURATION CLASS
# =============================================================================

class Config:
    """Configuration class for all settings"""
    
    def __init__(self):
        # These will be set by user prompts
        self.data_source = None  # 'ZoomInfo' or 'SalesGenie'
        self.contact_owner_email = None
        self.raw_data_path = None
        self.master_data_path = None
        
        # Template paths (fixed locations)
        self.zoominfo_template_path = 'Input/ZoomInfo_Default_List.csv'
        self.salesgenie_template_path = 'Input/Sales_Genie_Default_List.csv'
        self.output_dir = 'Output'
        
        # Zoho CRM API Configuration (from environment)
        self.zoho_accounts_url = os.getenv("ZOHO_ACCOUNTS_URL", "https://accounts.zoho.com").rstrip("/")
        self.zoho_api_base = os.getenv("ZOHO_API_BASE", "https://www.zohoapis.com/crm/v8").rstrip("/")
        self.zoho_client_id = os.getenv("ZOHO_CLIENT_ID", "").strip()
        self.zoho_client_secret = os.getenv("ZOHO_CLIENT_SECRET", "").strip()
        self.zoho_refresh_token = os.getenv("ZOHO_REFRESH_TOKEN", "").strip()
        
        # Zoho CRM field names (based on your CRM setup)
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
    
    def print_config(self):
        """Print configuration summary"""
        print("=" * 80)
        print("CONFIGURATION")
        print("=" * 80)
        print(f"Data Source: {self.data_source}")
        print(f"Contact Owner: {self.contact_owner_email}")
        print(f"Raw Data Path: {self.raw_data_path}")
        print(f"Master Data Path: {self.master_data_path}")
        print(f"Zoho CRM: {'Enabled' if self.zoho_enabled else 'Disabled (credentials missing)'}")
        print(f"Output Directory: {self.output_dir}")
        print("=" * 80)
        print()


# =============================================================================
# USER INPUT PROMPTS
# =============================================================================

def get_user_inputs(config: Config) -> Config:
    """
    Prompt user for required inputs.
    
    Args:
        config: Config object to populate
    
    Returns:
        Updated Config object
    """
    print("\n" + "=" * 80)
    print("VANILLASOFT DATA PREPARATION AUTOMATION")
    print("=" * 80)
    print("\nPlease provide the following information:\n")
    
    # 1. Data Source Selection
    while True:
        print("Select data source:")
        print("  1. ZoomInfo")
        print("  2. SalesGenie")
        choice = input("\nEnter choice (1 or 2): ").strip()
        
        if choice == '1':
            config.data_source = 'ZoomInfo'
            break
        elif choice == '2':
            config.data_source = 'SalesGenie'
            break
        else:
            print("❌ Invalid choice. Please enter 1 or 2.\n")
    
    print(f"✓ Data source: {config.data_source}\n")
    
    # 2. Contact Owner Email
    while True:
        email = input("Enter contact owner email: ").strip()
        if email and '@' in email:
            config.contact_owner_email = email
            break
        else:
            print("❌ Please enter a valid email address.\n")
    
    print(f"✓ Contact owner: {config.contact_owner_email}\n")
    
    # 3. Raw Data Path
    while True:
        raw_path = input(f"Enter path to raw {config.data_source} CSV file: ").strip()
        if os.path.exists(raw_path):
            config.raw_data_path = raw_path
            break
        else:
            print(f"❌ File not found: {raw_path}")
            print("   Please enter a valid file path.\n")
    
    print(f"✓ Raw data path: {config.raw_data_path}\n")
    
    # 4. Master Data Path
    while True:
        master_path = input("Enter path to Master Data Excel file: ").strip()
        if os.path.exists(master_path):
            config.master_data_path = master_path
            break
        else:
            print(f"❌ File not found: {master_path}")
            print("   Please enter a valid file path.\n")
    
    print(f"✓ Master data path: {config.master_data_path}\n")
    
    return config


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
        """
        Generate a short-lived Zoho access token using the refresh token.
        
        Returns:
            Access token string, or None if credentials are missing or request fails
        """
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
                raise RuntimeError(f"Unexpected token response: {json_response}")
            
            self.access_token = json_response["access_token"]
            print(f"✓ Zoho access token obtained: {self.access_token[:24]}...")
            return self.access_token
        
        except Exception as e:
            print(f"❌ Error obtaining Zoho access token: {e}")
            return None
    
    def get_org_id(self) -> Optional[str]:
        """
        Fetch and cache the organization ID for correct URL construction.
        
        Returns:
            Organization ID string, or None if request fails
        """
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
                    print(f"✓ Organization ID: {self._org_id}")
                    return self._org_id
        except Exception as e:
            print(f"⚠️ Error fetching org ID: {e}")
        
        return None
    
    def execute_coql(self, query: str) -> Dict:
        """
        Execute a COQL query against Zoho CRM.
        
        Args:
            query: COQL query string
        
        Returns:
            Dictionary with query results (includes 'data' key with list of records)
        """
        if not self.access_token:
            print("⚠️  No access token available")
            return {"data": []}
        
        headers = {"Authorization": f"Zoho-oauthtoken {self.access_token}"}
        
        try:
            response = requests.post(
                f"{self.config.zoho_api_base}/coql",
                headers=headers,
                json={"select_query": query},
                timeout=60
            )
            
            # HTTP 204 means no content/no results
            if response.status_code == 204:
                return {"data": []}
            
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.RequestException as e:
            print(f"⚠️  COQL query error: {e}")
            return {"data": []}
    
    def batch_query_locatings(self, zip_codes: List[str]) -> Dict[str, List[Dict]]:
        """
        OPTIMIZED: Query multiple ZIP codes in one API call using IN operator.
        
        Args:
            zip_codes: List of ZIP codes to query
        
        Returns:
            Dict mapping ZIP codes to list of matching records
        """
        if not zip_codes or not self.access_token:
            return {}
        
        results = {}
        batch_size = 50  # COQL max for IN operator
        
        for i in range(0, len(zip_codes), batch_size):
            batch = zip_codes[i:i + batch_size]
            
            # Escape single quotes and build IN clause
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
        """
        OPTIMIZED: Query multiple cities in one API call using IN operator.
        
        Args:
            cities: List of cities to query
        
        Returns:
            Dict mapping cities to list of matching records
        """
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
        """
        Build a Zoho CRM record URL.
        
        Correct format: https://crm.zoho.com/crm/org{ORG_ID}/tab/{MODULE}/{RECORD_ID}
        
        Args:
            module_name: Module name (e.g., 'Locatings')
            record_id: Zoho record ID
        
        Returns:
            Full URL to the Zoho CRM record
        """
        org_id = self.get_org_id()
        # Note: Locatings is CustomModule5 in Zoho CRM
        module_api_name = "CustomModule5" if module_name == "Locatings" else module_name
        
        if org_id:
            return f"https://crm.zoho.com/crm/org{org_id}/tab/{module_api_name}/{record_id}"
        else:
            return f"https://crm.zoho.com/crm/tab/{module_api_name}/{record_id}"


# =============================================================================
# DATA CLEANING FUNCTIONS
# =============================================================================

def remove_phone_extension(phone: str) -> str:
    """
    Remove extension from phone number.
    
    Args:
        phone: Phone number string (may contain extension)
    
    Returns:
        Phone number without extension
    
    Examples:
        '(615) 301-5348 x123' -> '(615) 301-5348'
        '615-301-5348 ext 456' -> '615-301-5348'
    """
    if pd.isna(phone) or not isinstance(phone, str):
        return phone
    
    # Pattern matches: x123, ext 123, extension 123, etc.
    pattern = r'\s*[xX]\d+|\s*[eE][xX][tT]\.?\s*\d+|\s*[eE][xX][tT][eE][nN][sS][iI][oO][nN]\s*\d+'
    
    # Remove everything from the extension marker onwards
    cleaned = re.split(pattern, phone)[0].strip()
    return cleaned


def clean_phone_dataframe(df: pd.DataFrame, phone_column: str) -> pd.DataFrame:
    """
    Clean phone numbers in a dataframe by removing extensions.
    
    Args:
        df: DataFrame containing phone numbers
        phone_column: Name of the column containing phone numbers
    
    Returns:
        DataFrame with cleaned phone numbers
    """
    df = df.copy()
    
    if phone_column not in df.columns:
        print(f"⚠️  Column '{phone_column}' not found in dataframe")
        return df
    
    # Count how many have extensions
    has_extension = df[phone_column].astype(str).str.contains(
        r'[xX]\d+|[eE][xX][tT]', 
        na=False, 
        regex=True
    )
    extension_count = has_extension.sum()
    
    if extension_count > 0:
        print(f"  Found {extension_count} phone numbers with extensions")
        df[phone_column] = df[phone_column].apply(remove_phone_extension)
        print(f"  ✓ Extensions removed")
    else:
        print(f"  No extensions found in {phone_column}")
    
    return df


def remove_duplicate_phones(df: pd.DataFrame, phone_column: str) -> pd.DataFrame:
    """
    Remove duplicate phone numbers, keeping only the first occurrence.
    
    Args:
        df: DataFrame containing phone numbers
        phone_column: Name of the column containing phone numbers
    
    Returns:
        DataFrame with duplicates removed
    """
    if phone_column not in df.columns:
        print(f"⚠️  Column '{phone_column}' not found in dataframe")
        return df
    
    initial_count = len(df)
    
    # Remove duplicates based on phone number, keeping first occurrence
    df_clean = df.drop_duplicates(subset=[phone_column], keep='first')
    
    duplicates_removed = initial_count - len(df_clean)
    
    if duplicates_removed > 0:
        print(f"  Removed {duplicates_removed} duplicate phone numbers")
        print(f"  Original: {initial_count} rows → After deduplication: {len(df_clean)} rows")
    else:
        print(f"  No duplicate phone numbers found")
    
    return df_clean


# =============================================================================
# DATA LOADING FUNCTIONS
# =============================================================================

def load_master_data(filepath: str) -> Dict[str, str]:
    """
    Load operator metadata from Master Data Excel file.
    Extracts information from the last column.
    
    Args:
        filepath: Path to Master Data Excel file
    
    Returns:
        Dictionary containing operator information
    """
    print("\n" + "=" * 80)
    print("LOADING MASTER DATA")
    print("=" * 80)
    
    # Read the Excel file
    master_df = pd.read_excel(filepath)
    print(f"✓ Loaded {master_df.shape[0]} rows × {master_df.shape[1]} columns")
    
    # Extract operator metadata from the last column
    last_column = master_df.iloc[:, -1]
    non_null_values = last_column[last_column.notna()].tolist()
    
    print(f"  Found {len(non_null_values)} non-null values in last column")
    
    # Extract operator information
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
        
        print("\n✓ Operator Information Extracted:")
        print(f"  Business Name: {operator_info['vending_business_name']}")
        print(f"  Operator: {operator_info['operator_name']}")
        print(f"  Phone: {operator_info['operator_phone']}")
        print(f"  Email: {operator_info['operator_email']}")
        print(f"  Zip: {operator_info['operator_zip']}")
        print(f"  Website: {operator_info['operator_website']}")
        if operator_info['team']:
            print(f"  Team: {operator_info['team']}")
    else:
        print("⚠️  WARNING: Not enough data in last column to extract operator info")
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
    """
    Load the template CSV file to get the correct column structure.
    
    Args:
        filepath: Path to template CSV
    
    Returns:
        DataFrame with template column structure
    """
    template_df = pd.read_csv(filepath)
    print(f"✓ Template loaded: {len(template_df.columns)} columns")
    return template_df


def load_raw_data(config: Config) -> Tuple[pd.DataFrame, str]:
    """
    Load raw data from ZoomInfo or SalesGenie based on configuration.
    
    Args:
        config: Configuration object
    
    Returns:
        Tuple of (raw_dataframe, phone_column_name)
    """
    print("\n" + "=" * 80)
    print(f"LOADING RAW DATA - {config.data_source}")
    print("=" * 80)
    
    df_raw = pd.read_csv(config.raw_data_path)
    
    if config.data_source == 'ZoomInfo':
        phone_column = 'Direct Phone Number'
    else:  # SalesGenie
        phone_column = 'Phone Number Combined'
    
    print(f"✓ Loaded {len(df_raw)} records")
    print(f"  Columns: {len(df_raw.columns)}")
    print(f"  Phone column: {phone_column}")
    
    return df_raw, phone_column


# =============================================================================
# DATA MAPPING FUNCTIONS
# =============================================================================

def map_zoominfo_to_template(df_raw: pd.DataFrame, template_df: pd.DataFrame) -> pd.DataFrame:
    """
    Map ZoomInfo raw data columns to VanillaSoft template format.
    
    Args:
        df_raw: Raw ZoomInfo data
        template_df: Empty template with correct column structure
    
    Returns:
        DataFrame with data mapped to template format
    """
    print("\n📋 Mapping ZoomInfo data to template format...")
    
    # Create empty dataframe with template columns
    df_output = pd.DataFrame(columns=template_df.columns)
    
    # Map columns from ZoomInfo to template
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
    
    # Apply mapping
    for source_col, target_col in column_mapping.items():
        if source_col in df_raw.columns and target_col in df_output.columns:
            df_output[target_col] = df_raw[source_col]
    
    print(f"✓ Mapped {len(df_raw)} rows from ZoomInfo format to template")
    return df_output


def map_salesgenie_to_template(df_raw: pd.DataFrame, template_df: pd.DataFrame) -> pd.DataFrame:
    """
    Map SalesGenie raw data columns to VanillaSoft template format.
    
    Args:
        df_raw: Raw SalesGenie data
        template_df: Empty template with correct column structure
    
    Returns:
        DataFrame with data mapped to template format
    """
    print("\n📋 Mapping SalesGenie data to template format...")
    
    # Create empty dataframe with template columns
    df_output = pd.DataFrame(columns=template_df.columns)
    
    # Map columns from SalesGenie to template
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
    
    # Apply mapping
    for source_col, target_col in column_mapping.items():
        if source_col in df_raw.columns and target_col in df_output.columns:
            df_output[target_col] = df_raw[source_col]
    
    print(f"✓ Mapped {len(df_raw)} rows from SalesGenie format to template")
    return df_output


def fill_operator_fields(df: pd.DataFrame, operator_info: Dict[str, str], 
                        contact_owner: str, data_source: str) -> pd.DataFrame:
    """
    Fill operator-related fields and metadata in the dataframe.
    
    Args:
        df: DataFrame to fill
        operator_info: Dictionary with operator information
        contact_owner: Email of contact owner
        data_source: Data source name (ZoomInfo or SalesGenie)
    
    Returns:
        DataFrame with operator fields filled
    """
    print("\n📝 Filling operator fields and metadata...")
    
    df = df.copy()
    
    # Fill operator fields
    df['Vending Business Name'] = operator_info.get('vending_business_name', '')
    df['Operator Name'] = operator_info.get('operator_name', '')
    df['Operator Phone #'] = operator_info.get('operator_phone', '')
    df['Operator Email Address'] = operator_info.get('operator_email', '')
    df['Operator Zip Code'] = operator_info.get('operator_zip', '')
    df['Operator Website Address'] = operator_info.get('operator_website', '')
    
    # Fill team if present in template
    if 'Team' in df.columns and operator_info.get('team'):
        df['Team'] = operator_info['team']
    
    # Fill Contact Owner
    df['Contact Owner'] = contact_owner
    
    # Fill List Source with data source + today's date
    today = datetime.now().strftime('%b %d %Y')  # e.g., "Dec 16 2025"
    df['List Source'] = f"{data_source} {today}"
    
    print(f"✓ Operator fields filled for {len(df)} records")
    print(f"  List Source: {df['List Source'].iloc[0] if len(df) > 0 else 'N/A'}")
    
    return df


# =============================================================================
# ZOHO CRM MATCHING
# =============================================================================

def match_address(search_address: str, records: List[Dict], address_field: str) -> Optional[Dict]:
    """
    Find matching address in a list of records.
    
    Args:
        search_address: Address to find
        records: List of Zoho records
        address_field: Field name containing address
    
    Returns:
        Matching record or None
    """
    if not search_address or not records:
        return None
    
    search_addr = str(search_address).lower().strip()
    
    for record in records:
        record_addr = str(record.get(address_field, "")).lower().strip()
        
        # Exact or partial match
        if search_addr == record_addr:
            return record
        if search_addr in record_addr or record_addr in search_addr:
            return record
    
    return None


def match_with_zoho(df: pd.DataFrame, zoho_api: ZohoAPI) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    OPTIMIZED: Match records with Zoho CRM using batch queries.
    
    Instead of querying one record at a time (N API calls), this:
    1. Collects all unique ZIP codes and cities
    2. Batch queries Zoho (2-4 API calls total)
    3. Matches records locally in memory
    
    Args:
        df: DataFrame with contact data
        zoho_api: ZohoAPI instance
    
    Returns:
        Tuple of (main_dataframe, deliveries_dataframe)
    """
    print("\n" + "=" * 80)
    print("ZOHO CRM MATCHING (OPTIMIZED)")
    print("=" * 80)
    
    if not zoho_api.config.zoho_enabled:
        print("⚠️  Zoho CRM integration disabled (no credentials)")
        return df, pd.DataFrame()
    
    # Mint access token
    if not zoho_api.mint_access_token():
        print("⚠️  Failed to obtain Zoho access token, skipping CRM matching")
        return df, pd.DataFrame()
    
    df = df.copy()
    
    # Step 1: Collect unique ZIP codes and cities
    zip_codes = df['ZIP code'].dropna().astype(str).unique().tolist()
    cities = df['City'].dropna().astype(str).unique().tolist()
    
    print(f"\n📊 Records to match: {len(df)}")
    print(f"   Unique ZIP codes: {len(zip_codes)}")
    print(f"   Unique cities: {len(cities)}")
    
    # Step 2: Batch query Zoho CRM
    print("\n🔍 Batch querying Zoho CRM...")
    
    print("   Querying Deliveries by city...")
    deliveries_by_city = zoho_api.batch_query_deliveries(cities)
    deliveries_count = sum(len(v) for v in deliveries_by_city.values())
    print(f"   ✓ Found {deliveries_count} records in Deliveries")
    
    print("   Querying Locatings by ZIP...")
    locatings_by_zip = zoho_api.batch_query_locatings(zip_codes)
    locatings_count = sum(len(v) for v in locatings_by_zip.values())
    print(f"   ✓ Found {locatings_count} records in Locatings")
    
    # Step 3: Match records locally
    print("\n🔄 Matching records...")
    
    found_in_deliveries = []
    found_in_locatings = []
    
    for idx, row in df.iterrows():
        address = str(row.get('Address', ''))
        zip_code = str(row.get('ZIP code', ''))
        city = str(row.get('City', ''))
        
        # Skip if missing required fields
        if not address or (not zip_code and not city):
            continue
        
        # Check Deliveries first (higher priority)
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
    
    # Step 4: Apply results
    print(f"\n📊 Matching Summary:")
    print(f"   Found in Deliveries: {len(found_in_deliveries)} records")
    print(f"   Found in Locatings: {len(found_in_locatings)} records")
    print(f"   Not found: {len(df) - len(found_in_deliveries) - len(found_in_locatings)} records")
    
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
    
    print("\n✅ Matching complete!")
    return df, df_deliveries


# =============================================================================
# EXPORT FUNCTIONS
# =============================================================================

def export_data(df_main: pd.DataFrame, df_deliveries: pd.DataFrame, 
                operator_name: str, data_source: str, output_dir: str) -> Tuple[str, str]:
    """
    Export processed data to CSV files.
    
    Filename format: {operator_name}_{data_source}_{date}.csv
    
    Args:
        df_main: Main dataframe to export
        df_deliveries: Deliveries dataframe (if any)
        operator_name: Operator name for filename
        data_source: Data source name for filename
        output_dir: Output directory path
    
    Returns:
        Tuple of (main_filepath, deliveries_filepath)
    """
    print("\n" + "=" * 80)
    print("EXPORTING DATA")
    print("=" * 80)
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Clean operator name for filename (remove special characters)
    clean_operator_name = re.sub(r'[^a-zA-Z0-9_-]', '_', operator_name)
    
    # Generate timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Export main file
    main_filename = f"{clean_operator_name}_{data_source}_{timestamp}.csv"
    main_filepath = os.path.join(output_dir, main_filename)
    
    df_main.to_csv(main_filepath, index=False)
    print(f"✓ Main file exported: {main_filename}")
    print(f"  Records: {len(df_main)}")
    print(f"  Path: {main_filepath}")
    
    # Export deliveries file if there are any
    deliveries_filepath = None
    if not df_deliveries.empty:
        deliveries_filename = f"{clean_operator_name}_{data_source}_Deliveries_{timestamp}.csv"
        deliveries_filepath = os.path.join(output_dir, deliveries_filename)
        
        df_deliveries.to_csv(deliveries_filepath, index=False)
        print(f"\n✓ Deliveries file exported: {deliveries_filename}")
        print(f"  Records: {len(df_deliveries)}")
        print(f"  Path: {deliveries_filepath}")
    
    return main_filepath, deliveries_filepath


# =============================================================================
# MAIN FUNCTION
# =============================================================================

def main():
    """Main execution function"""
    print("\n" + "=" * 80)
    print("VANILLASOFT DATA PREPARATION AUTOMATION")
    print("=" * 80)
    
    # 1. Initialize configuration
    config = Config()
    
    # 2. Get user inputs
    config = get_user_inputs(config)
    
    # 3. Print configuration summary
    config.print_config()
    
    # 4. Initialize Zoho API
    zoho_api = ZohoAPI(config)
    
    # 5. Load master data (operator information)
    operator_info = load_master_data(config.master_data_path)
    
    # 6. Load template
    if config.data_source == 'ZoomInfo':
        template_df = load_template(config.zoominfo_template_path)
    else:
        template_df = load_template(config.salesgenie_template_path)
    
    # 7. Load raw data
    df_raw, phone_column = load_raw_data(config)
    
    # 8. Clean phone numbers
    print("\n" + "=" * 80)
    print("DATA CLEANING")
    print("=" * 80)
    print("\n🧹 Cleaning phone numbers...")
    df_raw = clean_phone_dataframe(df_raw, phone_column)
    
    # 9. Remove duplicate phone numbers
    print("\n🧹 Removing duplicate phone numbers...")
    df_raw = remove_duplicate_phones(df_raw, phone_column)
    
    # 10. Map to template format
    print("\n" + "=" * 80)
    print("DATA MAPPING")
    print("=" * 80)
    
    if config.data_source == 'ZoomInfo':
        df_mapped = map_zoominfo_to_template(df_raw, template_df)
    else:
        df_mapped = map_salesgenie_to_template(df_raw, template_df)
    
    # 11. Fill operator fields and metadata
    df_mapped = fill_operator_fields(
        df_mapped, 
        operator_info, 
        config.contact_owner_email, 
        config.data_source
    )
    
    # 12. Match with Zoho CRM
    df_final, df_deliveries = match_with_zoho(df_mapped, zoho_api)
    
    # 13. Export data
    main_file, deliveries_file = export_data(
        df_final,
        df_deliveries,
        operator_info.get('operator_name', 'Unknown'),
        config.data_source,
        config.output_dir
    )
    
    # 14. Summary
    print("\n" + "=" * 80)
    print("AUTOMATION COMPLETE")
    print("=" * 80)
    print(f"\n✅ Processing complete!")
    print(f"   Main output: {len(df_final)} records")
    if not df_deliveries.empty:
        print(f"   Deliveries output: {len(df_deliveries)} records")
    print(f"\n📁 Files saved to: {config.output_dir}")
    print()


if __name__ == "__main__":
    main()
