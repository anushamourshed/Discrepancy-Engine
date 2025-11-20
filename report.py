# ==============================================================================
# 1. IMPORTS AND STATIC DATA DEFINITIONS (FROM CELL 1)
# ==============================================================================

import streamlit as st
import pandas as pd
import numpy as np
import re
import io
import textwrap
from collections import defaultdict
import functools
from datetime import date
import os

# --- A. Existing Template Content (Used for standardizing column names) ---
STATIC_TEMPLATE_CONTENT = textwrap.dedent("""
Placement ID,Alternate PLID,Line Item ID / ad group id,Creative ID,Site Name,3rd Party Site ID,Report Placement,Report Line Item / ad group name,Report Creative / creative name / Master and Companion creative,Publisher impressions,Date,Sum of Total Impressions,Unique Concat ID
""")

# --- B. Existing Mapping Logic (Client Translator Dictionary) ---
STATIC_MAPPING_CONTENT = textwrap.dedent("""
Template_Column,Client_Column_Variant,Priority
Placement ID,Placement ID,1
Placement ID,PLID,2
Placement ID,placement_id,3
Placement ID,pl_id,4
Alternate PLID,Alternate PLID,1
Alternate PLID,Alt PLID,2
Alternate PLID,Secondary Placement ID,3
Line Item ID / ad group id,Line Item ID,1
Line Item ID / ad group id,Ad Group ID,2
Line Item ID / ad group id,adgroup_id,3
Creative ID,Creative ID,1
Creative ID,Ad Creative ID,2
Creative ID,Ad Server Creative ID,3                                        
Creative ID,Master and Companion creative ID,4
Creative ID,sa_cr,4
Report Creative / creative name / Master and Companion creative,Creative Name,1
Report Creative / creative name / Master and Companion creative,Creative,2
Report Creative / creative name / Master and Companion creative,Master and Companion creative,3 
Report Creative / creative name / Master and Companion creative,Master & Companion creative,4
Report Creative / creative name / Master and Companion creative,Master Creative Name,5
Report Creative / creative name / Master and Companion creative,Flight Name,6
Report Creative / creative name / Master and Companion creative,Creative Group Name,7
Report Creative / creative name / Master and Companion creative,Ad creative,8 
Report Creative / creative name / Master and Companion creative,Ad creative name,9
Report Placement,Report Placement,1
Report Placement,Placement,2
Report Placement,Placement Name,3
Site Name,Site Name,1
Site Name,Site,2
Site Name,Site (CM360),3
Site Name,Publisher Site,4
Site Name,Website,5
Publisher impressions,Publisher Impressions,1
Publisher impressions,Impressions,2
Publisher impressions,Delivered Impressions,3
Publisher impressions,Ad server impressions,4
Publisher impressions,Advertiser impressions,5
Publisher impressions,Ad impressions,6
Publisher impressions,Total impressions,7
Publisher impressions,Impressions Delivered,8                                        
Publisher impressions,Net Counted Ads,9
Publisher impressions,Gross Counted Ads,10
Publisher impressions,Counts,11
Date,Date,1
Date,Day,2
Date,Report Date,3
Date,Event Date,4
Date,Impression Date,5
Date,Reporting Date,6  
Date,Time Period Name,7
Date,Time Period,8
Date,Time Stamp,9
Report Line Item / ad group name,Report Line Item / ad group name,1
Report Line Item / ad group name,Report Line Item,2
Report Line Item / ad group name,Ad group name,3
Report Line Item / ad group name,Line Item,4
Report Line Item / ad group name,Line group name,5
Report Line Item / ad group name,Line item name,6
Report Line Item / ad group name,Ad group,6
3rd Party Site ID,3rd Party Site ID,1
3rd Party Site ID,Site ID,2
3rd Party Site ID,Site ID(CM360),3
3rd Party Site ID,Publisher ID,4
""")

# --- C. New Static Samba Mapping (Defined Structure) ---
SAMBA_MAPPING = {
    'sa_cr': 'Creative ID',
    'sa_pl': 'Placement ID',
    'sa_li': 'Line Item ID / ad group id',
    'day_str': 'Date',
    'sum__counts': 'Samba Impressions',
    'counts': 'Samba Impressions',     
    'Counts': 'Samba Impressions',
    'Count': 'Samba Impressions',
    'Count': 'Samba Impressions',
}


# ==============================================================================
# 2. CORE HELPER FUNCTIONS (FROM CELL 2 & 3)
# ==============================================================================

# --- A. CORE HELPERS ---

def normalize(s):
    """Lowercase, strip, and remove all non-alphanumeric chars (robust)."""
    return re.sub(r'[^a-z0-9]', '', str(s).strip().lower())

@st.cache_data
def get_mapping_dataframes():
    """Loads and cleans the static mapping dataframe."""
    mapping_buffer = io.StringIO(STATIC_MAPPING_CONTENT)
    mapping_df = pd.read_csv(mapping_buffer) 
    
    mapping_df.columns = mapping_df.columns.map(lambda c: str(c).strip())

    try:
        mapping_cols = mapping_df.columns
        mapping_df = mapping_df.rename(columns={
            mapping_cols[0]: 'Template_Column',
            mapping_cols[1]: 'Client_Column_Variant',
            mapping_cols[2]: 'Priority'
        })
    except IndexError:
        raise ValueError("Internal mapping structure error.")
    
    mapping_df = mapping_df.sort_values('Priority', na_position='last')
    
    return mapping_df

def find_best_header_row(df_raw, mapping_df):
    """
    Analyzes the first 100 rows to find the row that contains at least 4 standardized column names.
    Returns the index of the best header row (0-based).
    """
    BEST_MATCH_THRESHOLD = 4 
    best_row_index = 0
    max_matches = 0
    
    all_normalized_variants = set(mapping_df['Client_Column_Variant'].map(normalize).tolist())
    
    for i in range(min(100, len(df_raw))): 
        potential_headers = df_raw.iloc[i].astype(str).tolist()
        match_count = 0
        
        for header in potential_headers:
            if normalize(header) in all_normalized_variants:
                match_count += 1
        
        if match_count >= BEST_MATCH_THRESHOLD:
            return i 
            
        if match_count > max_matches:
            max_matches = match_count
            best_row_index = i
            
    return best_row_index

def load_data(file_content, mapping_df, is_client_report=False):
    """
    Loads file content. If is_client_report=True, applies dynamic header finding 
    (to skip metadata) and robust footer trimming.
    """
    file_content.seek(0)
    
    is_excel = False
    if hasattr(file_content, 'name'):
        is_excel = file_content.name.lower().endswith(('.xlsx', '.xls'))
        
    reader = pd.read_excel if is_excel else pd.read_csv
    
    # --- STEP 1: Load Raw Data (Headerless for Scanning) ---
    try:
        file_content_copy = io.BytesIO(file_content.read())
        file_content_copy.seek(0)
        df_temp = reader(file_content_copy, header=None)
    except Exception as e:
        try:
            file_content_copy.seek(0)
            df_temp = pd.read_excel(file_content_copy, header=None)
            is_excel = True
        except Exception:
            try:
                file_content_copy.seek(0)
                df_temp = pd.read_csv(file_content_copy, header=None)
                is_excel = False
            except Exception as e_inner:
                raise ValueError(f"Could not read file content as Excel or CSV: {e_inner}")
    
    reader = pd.read_excel if is_excel else pd.read_csv
    
    # --- STEP 2: Standard/Clean Report Loading (Samba) ---
    if not is_client_report:
        file_content.seek(0)
        return reader(file_content, header=0)

    # --- STEP 3: Client Report Pre-Cleaning (Dynamic Header/Footer) ---
    
    # A. Find the correct header row index 
    header_row_index = find_best_header_row(df_temp, mapping_df)
    
    # B. Re-read the data using the correct header row index
    file_content.seek(0)
    df = reader(file_content, header=header_row_index)
    df.columns = df.columns.astype(str).str.strip() 
        
    # C. Footer Trimming
    FOOTER_KEYWORDS = ['total', 'sum', 'grand total', 'overall']
    
    if df.shape[1] > 0:
        footer_mask = df.iloc[:, 0].astype(str).str.strip().str.lower().isin(FOOTER_KEYWORDS)
        footer_indices = df[footer_mask].index
        if not footer_indices.empty:
            first_footer_index = footer_indices[0]
            df = df.loc[:first_footer_index - 1].copy()
            
    return df.copy()

# --- B. PROCESSING & CLEANING FUNCTIONS (FROM CELL 3) ---

def process_and_standardize(df_raw, mapping_df, PRIMARY_ID_HIERARCHY, REQUIRED_DATE_COL, 
                            REQUIRED_CLIENT_METRIC, REQUIRED_SAMBA_METRIC, REQUIRED_DESC_COL, 
                            samba_mapping=None, is_client=True):
    """
    Combines fuzzy mapping and explicit renaming to standardize column headers.
    """
    df = df_raw.copy()
    
    if samba_mapping:
        df = df.rename(columns=samba_mapping, errors='ignore') 
    
    client_columns_norm = defaultdict(list)
    for col in df.columns:
        client_columns_norm[normalize(col)].append(col)

    rename_dict = {} 
    
    for _, row in mapping_df.iterrows():
        template_col = row['Template_Column']
        client_variant = str(row['Client_Column_Variant'])
        var_norm = normalize(client_variant)
        if var_norm in client_columns_norm:
            original_col = client_columns_norm[var_norm][0]
            if original_col not in rename_dict:
                rename_dict[original_col] = template_col
                
    df = df.rename(columns={k: v for k, v in rename_dict.items() if k != v}, errors='ignore')
    
    standard_name_map = {normalize(name): name for name in PRIMARY_ID_HIERARCHY}
    corrections = {}
    for current_header in df.columns:
        current_norm = normalize(current_header)
        if current_norm in standard_name_map and current_header != standard_name_map[current_norm]:
            corrections[current_header] = standard_name_map[current_norm]
    df = df.rename(columns=corrections, errors='ignore')
    
    REQUIRED_NORM_DESC = normalize(REQUIRED_DESC_COL)
    for current_header in df.columns:
        if normalize(current_header) == REQUIRED_NORM_DESC and current_header != REQUIRED_DESC_COL:
            df = df.rename(columns={current_header: REQUIRED_DESC_COL})
            break
            
    df = df.loc[:, ~df.columns.duplicated(keep='first')]

    required_keys = PRIMARY_ID_HIERARCHY + [REQUIRED_DATE_COL]
    if is_client:
        required_keys += [REQUIRED_CLIENT_METRIC, REQUIRED_DESC_COL, 'Report Placement', 'Report Line Item / ad group name'] 
    else:
        required_keys += [REQUIRED_SAMBA_METRIC] 
        
    final_columns = [col for col in required_keys if col in df.columns]
    final_unique_cols = list(set(final_columns))

    return df[final_unique_cols].copy()

def clean_and_standardize_keys(df, id_cols, placeholders, metric_cols):
    """
    Converts metrics to numeric and IDs to clean strings, stripping .0
    """
    
    for col in metric_cols:
        if col in df.columns:
            cleaned_series = df[col].apply(lambda x: str(x).strip()).replace(placeholders, np.nan)
            df[col] = pd.to_numeric(cleaned_series, errors='coerce').fillna(0)
            df[col] = df[col].astype('Int64')

    for col in id_cols:
        if col in df.columns:
            string_series = df[col].apply(lambda x: str(x).strip())
            
            exact_match_placeholders = [f"^{re.escape(str(p))}$" for p in placeholders]
            placeholder_regex = '|'.join(exact_match_placeholders)
            
            # Use .apply() with re.sub to bypass potential .str accessor errors
            stripped_series = string_series.apply(lambda x: re.sub(r'\.0$', '', x))
            
            # Use pandas .str.replace for the case-insensitive regex replacement
            cleaned_series = stripped_series.str.replace(placeholder_regex, str(np.nan), case=False, regex=True)
            cleaned_series = cleaned_series.replace('nan', np.nan) 
            df[col] = cleaned_series.replace('<NA>', np.nan)
    
    return df

# --- C. DECISION & RECONCILIATION FUNCTIONS (FROM CELL 4 & 5) ---

def select_primary_key(df_client_filtered, df_samba_filtered, HIERARCHY, THRESHOLD, NAME_MAP):
    """
    Iterates through the hierarchy to find the best valid ID key.
    """
    primary_key = None
    descriptive_name_key = None
    missing_id_threshold = 1.0 - THRESHOLD

    for potential_id in HIERARCHY:
        if potential_id not in df_client_filtered.columns or potential_id not in df_samba_filtered.columns:
            continue
            
        client_validity = df_client_filtered[potential_id].count() / len(df_client_filtered)
        samba_validity = df_samba_filtered[potential_id].count() / len(df_samba_filtered)
        
        if client_validity >= THRESHOLD and samba_validity >= THRESHOLD:
            primary_key = potential_id
            descriptive_name_key = NAME_MAP.get(primary_key)
            break
    
    if not primary_key:
        error_message = (
            "❌ RECONCILIATION FAILED: A matching ID column could not be found.\n\n"
            "Here's why:\n"
            "1. The program checks for 'Placement ID', 'Creative ID', and 'Line Item ID' in both files.\n"
            "2. To work, at least ONE of these IDs must be present and mostly complete (95% valid) in BOTH your reports.\n\n"
            "Please check your files to ensure they share a common, high-quality ID column."
        )
        raise Exception(error_message)

    return primary_key, descriptive_name_key

def find_actual_descriptive_key(df_cols, standardized_key, mapping_df):
    """
    Finds the actual column header for the descriptive name that exists in the DataFrame.
    """
    normalized_standard = normalize(standardized_key)
    all_variants_df = mapping_df[mapping_df['Template_Column'].map(normalize) == normalized_standard]
    
    possible_names = set([standardized_key])
    possible_names.update(all_variants_df['Client_Column_Variant'].tolist())
    
    for name in possible_names:
        if name in df_cols:
            return name
    return None # Return None if no key is found

def reconcile_data(df_client_filtered, df_samba_filtered, primary_key, descriptive_name_key, mapping_df):
    """
    Runs the final aggregation, merge, and calculations.
    """
    
    IMPRESSION_COL_CLIENT = 'Publisher impressions'
    IMPRESSION_COL_SAMBA = 'Samba Impressions'

    # --- Enforce String Type on Primary Key ---
    if primary_key in df_client_filtered.columns:
        df_client_filtered[primary_key] = df_client_filtered[primary_key].astype(str).str.strip()
    if primary_key in df_samba_filtered.columns:
        df_samba_filtered[primary_key] = df_samba_filtered[primary_key].astype(str).str.strip()

    # --- Get the actual descriptive column name ---
    actual_descriptive_header = find_actual_descriptive_key(
        df_client_filtered.columns, 
        descriptive_name_key, 
        mapping_df
    )

    # --- Aggregation (Pivot Table Replacement) ---
    client_agg_dict = { IMPRESSION_COL_CLIENT: 'sum' }
    if actual_descriptive_header:
        client_agg_dict[actual_descriptive_header] = 'first'
    
    df_client_agg = df_client_filtered.groupby(primary_key, dropna=True, as_index=False).agg(client_agg_dict)
    df_samba_agg = df_samba_filtered.groupby(primary_key, dropna=True, as_index=False).agg({
        IMPRESSION_COL_SAMBA: 'sum'
    })

    # --- Reconciliation (VLOOKUP Replacement) ---
    df_final = df_client_agg.merge(
        df_samba_agg,
        on=primary_key,
        how='left'
    )

    # --- Rename and Format ---
    df_final = df_final.rename(columns={IMPRESSION_COL_CLIENT: 'Client Impressions'})
    
    if actual_descriptive_header:
        df_final = df_final.rename(columns={actual_descriptive_header: descriptive_name_key})
    else:
        # If no name was found, create the column and fill with "N/A"
        df_final[descriptive_name_key] = "N/A"

    # --- Calculation and Categorization ---
    df_final['Samba Impressions'] = pd.to_numeric(df_final['Samba Impressions'], errors='coerce')
    df_final['Disc'] = (df_final['Client Impressions'] / df_final['Samba Impressions']) - 1.0

    conditions = [
        (df_final['Samba Impressions'].isna()) | (df_final['Samba Impressions'] <= 0),
        ((df_final['Client Impressions'] < 1000) & (df_final['Samba Impressions'] < 1000)).fillna(False),
        df_final['Disc'].abs().le(0.10).fillna(False),
    ]
    choices = [ "N/A", "Low Impressions", "Tracking Well" ]
    df_final['Notes'] = np.select(conditions, choices, default="Monitor")

    # --- Final Formatting ---
    df_final['Disc'] = (df_final['Disc'] * 100).map('{:.2f}%'.format).replace('nan%', 'N/A')
    df_final['Samba Impressions'] = df_final['Samba Impressions'].astype('Int64').astype(str).replace('<NA>', 'N/A')

    FINAL_COLUMNS = [
        descriptive_name_key, 
        primary_key,
        'Client Impressions',
        'Samba Impressions',
        'Disc',
        'Notes'
    ]
    
    # Ensure all required columns exist, even if one was created as 'N/A'
    for col in FINAL_COLUMNS:
        if col not in df_final.columns:
            df_final[col] = "N/A" # Safety net

    return df_final[FINAL_COLUMNS].copy()


# ==============================================================================
# 3. STREAMLIT UI APPLICATION
# ==============================================================================

# --- Page Configuration ---
st.set_page_config(
    page_title="Discrepancy Report Generator",
    layout="centered"
)

# --- NEW: Add Logo ---
LOGO_FILE = 'sambalogo.png'
if os.path.isfile(LOGO_FILE):
    st.image(LOGO_FILE, width=200)
else:
    st.warning(f"Note: Logo file '{LOGO_FILE}' not found. Place it in the same directory as the app to display it.")


# --- Header ---
st.title("Discrepancy Report Generator")
st.markdown("Upload your Client and Samba reports to generate a discrepancy report automatically.")
st.markdown("---")

# --- File Uploaders ---
st.header("1. Upload Files")
client_file = st.file_uploader("Upload Client Report (e.g., CM360, Roku, Teads)", type=['xlsx', 'csv'])
samba_file = st.file_uploader("Upload Samba Report (e.g., Omni)", type=['xlsx', 'csv'])

# --- Date Range Input ---
st.header("2. Select Date Range")
col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input("Start Date", date.today() - pd.Timedelta(days=7))
with col2:
    end_date = st.date_input("End Date", date.today())

st.markdown("---")

# --- Process Button and Main Logic ---
if st.button("🚀 Generate Discrepancy Report", type="primary", use_container_width=True):

    if client_file and samba_file:
        with st.spinner("Processing files... This may take a moment."):
            try:
                # --- This block runs the entire notebook pipeline ---
                
                # 1. Load static map
                mapping_df = get_mapping_dataframes()
                
                # 2. Define constants (from Cell 2)
                PRIMARY_ID_HIERARCHY = ['Placement ID', 'Creative ID', 'Line Item ID / ad group id']
                MISSING_VALUE_PLACEHOLDERS = ['N/A', 'plid', '0', 0]
                VALIDITY_THRESHOLD = 0.95
                REQUIRED_ID_COLS = PRIMARY_ID_HIERARCHY
                REQUIRED_DATE_COL = 'Date'
                REQUIRED_CLIENT_METRIC = 'Publisher impressions'
                REQUIRED_SAMBA_METRIC = 'Samba Impressions'
                REQUIRED_DESC_COL = 'Report Creative / creative name / Master and Companion creative'
                NAME_MAP = {
                    'Placement ID': 'Report Placement',
                    'Creative ID': 'Report Creative / creative name / Master and Companion creative',
                    'Line Item ID / ad group id': 'Report Line Item / ad group name'
                }

                # 3. Load raw data (from Cell 2)
                client_buffer = io.BytesIO(client_file.read())
                client_buffer.name = client_file.name # Add name attribute for file type detection
                samba_buffer = io.BytesIO(samba_file.read())
                samba_buffer.name = samba_file.name
                
                df_client_raw = load_data(client_buffer, mapping_df, is_client_report=True)
                df_samba_raw = load_data(samba_buffer, mapping_df, is_client_report=False)
                
                # Warning 1: Empty File
                if len(df_client_raw) == 0:
                    raise Exception(f"WARNING: The file '{client_file.name}' is empty or contains no data rows.")
                if len(df_samba_raw) == 0:
                    raise Exception(f"WARNING: The file '{samba_file.name}' is empty.")

                # 4. Standardize (from Cell 3)
                df_client_std = process_and_standardize(
                    df_client_raw, mapping_df, PRIMARY_ID_HIERARCHY, REQUIRED_DATE_COL, 
                    REQUIRED_CLIENT_METRIC, REQUIRED_SAMBA_METRIC, REQUIRED_DESC_COL, 
                    is_client=True
                )
                df_samba_std = process_and_standardize(
                    df_samba_raw, mapping_df, PRIMARY_ID_HIERARCHY, REQUIRED_DATE_COL, 
                    REQUIRED_CLIENT_METRIC, REQUIRED_SAMBA_METRIC, REQUIRED_DESC_COL, 
                    samba_mapping=SAMBA_MAPPING, is_client=False
                )

                # Warning 2: Missing Essential Headers
                client_missing = []
                if REQUIRED_DATE_COL not in df_client_std.columns: client_missing.append("'Date'")
                if REQUIRED_CLIENT_METRIC not in df_client_std.columns: client_missing.append("'Publisher impressions'")
                if client_missing:
                    raise Exception(f"WARNING: Client file '{client_file.name}' is missing: {', '.join(client_missing)}.")
                
                samba_missing = []
                if REQUIRED_DATE_COL not in df_samba_std.columns: samba_missing.append("'Date'")
                if REQUIRED_SAMBA_METRIC not in df_samba_std.columns: samba_missing.append("'Samba Impressions'")
                if samba_missing:
                    raise Exception(f"WARNING: Samba file '{samba_file.name}' is missing: {', '.join(samba_missing)}.")

                # 5. Filter by Date (from Cell 3)
                df_client_std[REQUIRED_DATE_COL] = pd.to_datetime(df_client_std[REQUIRED_DATE_COL], errors='coerce').dt.date
                df_samba_std[REQUIRED_DATE_COL] = pd.to_datetime(df_samba_std[REQUIRED_DATE_COL], errors='coerce').dt.date
                
                client_mask = (df_client_std[REQUIRED_DATE_COL] >= start_date) & (df_client_std[REQUIRED_DATE_COL] <= end_date)
                samba_mask = (df_samba_std[REQUIRED_DATE_COL] >= start_date) & (df_samba_std[REQUIRED_DATE_COL] <= end_date)
                
                df_client_filtered = df_client_std[client_mask].copy()
                df_samba_filtered = df_samba_std[samba_mask].copy()
                
                # Warning 3: Data Not in Range
                if len(df_client_filtered) == 0 or len(df_samba_filtered) == 0:
                    msg = "WARNING: Data not in range. Reconciliation cannot proceed.\n"
                    if len(df_client_filtered) == 0: msg += "The Client file contains no data for the selected period."
                    else: msg += "The Samba file contains no data for the selected period."
                    raise Exception(msg)

                # 6. Clean Keys (from Cell 3)
                df_client_filtered = clean_and_standardize_keys(
                    df_client_filtered, REQUIRED_ID_COLS, MISSING_VALUE_PLACEHOLDERS, [REQUIRED_CLIENT_METRIC]
                )
                df_samba_filtered = clean_and_standardize_keys(
                    df_samba_filtered, REQUIRED_ID_COLS, MISSING_VALUE_PLACEHOLDERS, [REQUIRED_SAMBA_METRIC]
                )

                # 7. Select Key (from Cell 4)
                primary_key, descriptive_name_key = select_primary_key(
                    df_client_filtered, df_samba_filtered, PRIMARY_ID_HIERARCHY, VALIDITY_THRESHOLD, NAME_MAP
                )
                
                # 8. Reconcile (from Cell 5)
                df_discrepancy_report = reconcile_data(
                    df_client_filtered, df_samba_filtered, primary_key, descriptive_name_key, mapping_df
                )
                
                # 9. Prepare for Download (from Cell 7)
                output_buffer = io.BytesIO()
                df_discrepancy_report.to_excel(output_buffer, index=False)
                output_buffer.seek(0)
                
                base_name, _ = os.path.splitext(client_file.name)
                output_filename = f"{base_name} Discrepancy Report.xlsx"
                
                st.success("✅ Reconciliation Complete!")
                st.dataframe(df_discrepancy_report) # Display the final report in the app
                
                st.download_button(
                    label=f"⬇️ Download: {output_filename}",
                    data=output_buffer,
                    file_name=output_filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )

            except Exception as e:
                # This catches ALL our custom warnings and stops the process
                st.error(e)
    else:
        st.warning("Please upload both the Client and Samba files.")