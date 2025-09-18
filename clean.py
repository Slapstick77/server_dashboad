import pandas as pd
import os
import csv
import re
import logging

# Configure logging
logging.basicConfig(
    filename='data_processing.log',
    filemode='a',  # Append to existing log
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger()

def prompt_file_path():
    """Prompt the user to input a valid file path."""
    while True:
        file_path = input("Please enter the path to your input CSV file (e.g., file1.csv): ").strip('"').strip("'")
        if os.path.isfile(file_path):
            return file_path
        else:
            print(f"File not found: {file_path}. Please try again.\n")

def detect_delimiter(file_path):
    """Detect the delimiter used in the CSV file."""
    with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
        sample = csvfile.read(1024)
        sniffer = csv.Sniffer()
        try:
            dialect = sniffer.sniff(sample)
            delimiter = dialect.delimiter
            print(f"Detected delimiter: '{delimiter}'\n")
            logger.info(f"Detected delimiter: '{delimiter}'")
            return delimiter
        except csv.Error:
            print("Could not detect delimiter. Defaulting to comma (',').\n")
            logger.warning("Could not detect delimiter. Defaulting to comma (',').")
            return ','

def normalize_column_names(columns):
    """
    Normalize column names by stripping whitespace, converting to lowercase,
    and replacing spaces/special characters with underscores.
    """
    normalized = []
    for col in columns:
        col = col.strip().lower()
        col = re.sub(r'\s+', '_', col)       # Replace spaces with underscores
        col = re.sub(r'[^\w]', '', col)      # Remove non-word characters
        normalized.append(col)
    return normalized

def get_required_columns(df):
    """
    Retrieve the required mapping columns from the DataFrame.
    Returns the original column names if found, else None.
    """
    required_columns = ['comnumber1', 'jobname', 'contractnumber']
    normalized_columns = normalize_column_names(df.columns)
    column_mapping = {col: original for col, original in zip(normalized_columns, df.columns)}
    missing_columns = [col for col in required_columns if col not in normalized_columns]

    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        print(f"One or more required mapping columns are missing in the input file: {missing_columns}\n")
        print("Available columns in the input file:")
        for col in df.columns:
            print(f" - '{col}'")
        return None, None, None

    comnumber1_col = column_mapping['comnumber1']
    jobname_col = column_mapping['jobname']
    contractnumber_col = column_mapping['contractnumber']
    return comnumber1_col, jobname_col, contractnumber_col

def parse_description(description):
    """
    Parse the 'Description' field to extract Indoor, Outdoor, Code, Height, sqft.
    Returns a dictionary with the extracted values.
    """
    result = {
        'indoor': 0,         # 1 if Indoor (i), else 0
        'outdoor': 0,        # 1 if Outdoor (o), else 0
        'code': 0,           # Numerical code following i) or o), default to 0
        'height': 0.0,       # Height dimension
        'sqft': 0.0          # Calculated as Height * Width
    }

    if pd.isna(description):
        logger.debug("Description is NaN.")
        return result

    try:
        # Normalize text to lowercase and remove extra spaces
        description_clean = re.sub(r'\s+', ' ', description.strip().lower())
        logger.debug(f"Processing description: '{description_clean}'")

        # Extract Indoor ('i') or Outdoor ('o') and Code (only number)
        io_code_match = re.match(r'^([io])\)?\s*(\d+)', description_clean)
        if io_code_match:
            io_type = io_code_match.group(1)
            code = io_code_match.group(2)
            if io_type == 'i':
                result['indoor'] = 1
            else:
                result['outdoor'] = 1
            result['code'] = int(code)
            logger.debug(f"Extracted IO: {io_type.upper()}, Code: {code}")
        else:
            logger.warning(f"IO Code pattern not matched: '{description_clean}'")

        # Extract dimensions in HxWxL format with flexible separators
        dim_match = re.search(r'(\d+(?:\.\d+)?)\s*[\'"]?\s*[xX]\s*(\d+(?:\.\d+)?)\s*[\'"]?\s*[xX]\s*(\d+(?:\.\d+)?)', description_clean)
        if dim_match:
            height = float(dim_match.group(1))
            width = float(dim_match.group(2))
            # Length is ignored as per requirement
            result['height'] = height
            result['sqft'] = height * width  # Calculate sqft
            logger.debug(f"Extracted Dimensions - Height: {height}, Width: {width}, Sqft: {result['sqft']}")
        else:
            logger.warning(f"Dimensions pattern not matched: '{description_clean}'")

    except Exception as e:
        logger.error(f"Error parsing description '{description}': {e}")

    logger.debug(f"Result: {result}")
    return result

def convert_file1_to_cleaned(file1_path, output_csv_path):
    """Convert file1 to cleaned format and save as CSV."""
    try:
        delimiter = detect_delimiter(file1_path)
    except Exception as e:
        logger.error(f"Error detecting delimiter: {e}")
        delimiter = ','
        print("Defaulting to comma delimiter.\n")

    try:
        df = pd.read_csv(file1_path, delimiter=delimiter, dtype=str, encoding='utf-8', low_memory=False)
        logger.info(f"Input file successfully read with {len(df)} rows.\n")
    except Exception as e:
        logger.error(f"Error reading the input file: {e}\n")
        return

    # Normalize column names
    df.columns = normalize_column_names(df.columns)
    logger.info(f"Normalized columns: {df.columns.tolist()}")

    # Drop specific columns if they exist
    columns_to_drop = ['alum', 'housing', 'abase', 'hw', 'cs']
    existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
    if existing_columns_to_drop:
        df.drop(columns=existing_columns_to_drop, inplace=True)
        logger.info(f"Dropped columns: {existing_columns_to_drop}\n")
    else:
        logger.info("No specific columns to drop.\n")

    # Retrieve required columns
    comnumber1_col, jobname_col, contractnumber_col = get_required_columns(df)
    if not all([comnumber1_col, jobname_col, contractnumber_col]):
        logger.error("Missing required columns. Terminating process.")
        print("Please ensure that your input file contains the required columns with exact names.\n")
        return

    # **Adjusted Filtering Step: Ensuring no valid rows are excluded unintentionally**
    # Strip leading/trailing spaces in comnumber1
    df[comnumber1_col] = df[comnumber1_col].str.strip()

    # Apply filtering: Retain rows where comnumber1 starts with '1' or '2'
    total_rows = len(df)
    logger.info(f"Total rows before filtering: {total_rows}")

    df_filtered = df[df[comnumber1_col].str.startswith(('1', '2'), na=False)]
    filtered_rows = len(df_filtered)
    excluded_rows = total_rows - filtered_rows
    logger.info(f"Filtered out {excluded_rows} rows based on 'comnumber1' criteria. Remaining rows: {filtered_rows}")

    # Optionally, save excluded rows for analysis
    if excluded_rows > 0:
        excluded_df = df[~df[comnumber1_col].str.startswith(('1', '2'), na=False)]
        excluded_df.to_csv('excluded_rows.csv', index=False)
        logger.info(f"Excluded rows have been saved to 'excluded_rows.csv' for further analysis.")

    # Use the filtered DataFrame for further processing
    df = df_filtered
    # IMPORTANT: reset index so all subsequent assignments align row-by-row
    df = df.reset_index(drop=True)

    # Define the exact columns and order as per your requirements
    cleaned_columns = [
        'comnumber1',   # Ensure 'comnumber1' is the first column
        'jobname',      # Include original job name (column B)
        'contractnumber',  # Include original contract number (column C)
        'emb_new', 'flow_new', 'med_new', 'ol_new',
        'detailingstdhrs', 'progstdhrs', 'fabstdhrs', 'fabacthrs',
        'weldingstdhrs', 'weldingacthrs', 'baseformpaintstdhrs',
        'baseformpaintacthrs', 'fanassyteststdhrs', 'fanassytestacthrs',
        'insulwallfabstdhrs', 'insulwallfabacthrs', 'assystdhrs',
        'assyacthrs', 'doorfabstdhrs', 'doorfabacthrs',
        'electricalstdhrs', 'electricalacthrs', 'pipestdhrs',
        'pipeacthrs', 'paintstdhrs', 'paintacthrs',
        'cratingstdhrs', 'cratingacthrs',  # **Added CratingActHrs here**
        'mmp', 'sppp', 'lau', 'vfd', 'alum', 'airflow', 'leaktest', 'deflection',
        'indoor', 'outdoor', 'code', 'height', 'sqft',
        'flowline',  # New column to indicate if flowline data was moved
        'shipmonth'
    ]

    # Initialize the cleaned DataFrame with the specified columns
    # Start by including 'comnumber1'
    cleaned_df = pd.DataFrame(index=df.index)
    cleaned_df['comnumber1'] = df[comnumber1_col]
    # Preserve job name and contract number as provided in the source file
    cleaned_df['jobname'] = df[jobname_col]
    cleaned_df['contractnumber'] = df[contractnumber_col]

    # Add the rest of the cleaned columns initialized to 0 or appropriate default
    other_columns = [col for col in cleaned_columns if col not in ('comnumber1', 'jobname', 'contractnumber')]
    cleaned_df = cleaned_df.reindex(columns=['comnumber1', 'jobname', 'contractnumber'] + other_columns)
    # Initialize only numeric columns with 0, leave jobname/contractnumber as strings
    for init_col in other_columns:
        cleaned_df[init_col] = 0

    # Handle 'area' column to populate 'emb_new', 'flow_new', 'med_new', 'ol_new'
    if 'area' in df.columns:
        area_series = df['area'].str.strip().str.lower()
        area_categories = {
            'emb - new': 'emb_new',
            'flow - new': 'flow_new',
            'med - new': 'med_new',
            'ol - new': 'ol_new'
        }
        for key, col_name in area_categories.items():
            # Adjust the key to match normalization if necessary
            normalized_key = key.replace('_', '-').lower()
            cleaned_df[col_name] = (area_series == normalized_key).astype(int)
        logger.info("Processed 'area' column to populate 'emb_new', 'flow_new', 'med_new', 'ol_new'.")
    else:
        logger.warning("Column 'area' not found in the input file. Setting 'emb_new', 'flow_new', 'med_new', 'ol_new' to 0.\n")
        for col in ['emb_new', 'flow_new', 'med_new', 'ol_new']:
            cleaned_df[col] = 0

    # Assign keyword columns by setting binary flags
    if 'description' in df.columns:
        # Apply parse_description function to the 'description' column
        parsed_data = df['description'].apply(parse_description)
        parsed_df = pd.DataFrame(parsed_data.tolist())
        cleaned_df[['indoor', 'outdoor', 'code', 'height', 'sqft']] = parsed_df[['indoor', 'outdoor', 'code', 'height', 'sqft']]

        # Assign new keyword flags based on description
        cleaned_df['mmp'] = df['description'].str.contains(r'\bmmp\b', case=False, na=False).astype(int)
        cleaned_df['sppp'] = df['description'].str.contains(r'\bsppp\b', case=False, na=False).astype(int)
        cleaned_df['lau'] = df['description'].str.contains(r'\blau\b', case=False, na=False).astype(int)
        cleaned_df['vfd'] = df['description'].str.contains(r'\bvfd\b', case=False, na=False).astype(int)
        cleaned_df['alum'] = df['description'].str.contains(r'\balum\b', case=False, na=False).astype(int)
        cleaned_df['airflow'] = df['description'].str.contains(r'\bairflow\b|\blda\b', case=False, na=False).astype(int)
        cleaned_df['leaktest'] = df['description'].str.contains(r'\bleak\b|\bld\b|\blda\b', case=False, na=False).astype(int)
        cleaned_df['deflection'] = df['description'].str.contains(r'\bdeflection\b|\bld\b', case=False, na=False).astype(int)

        logger.info("Assigned keyword flags based on 'description' column.")
    else:
        logger.warning("Column 'description' not found in the input file. Setting additional keyword columns to default values.\n")
        additional_columns = ['mmp', 'sppp', 'lau', 'vfd', 'alum', 'airflow', 'leaktest', 'deflection']
        for col in additional_columns:
            cleaned_df[col] = 0

    # Define a mapping from file1 columns to cleaned file columns
    column_mapping = {
        'detailingstdhrs': 'detailingstdhrs',
        'progstdhrs': 'progstdhrs',
        'fabstdhrs': 'fabstdhrs',
        'fabacthrs': 'fabacthrs',
        'weldingstdhrs': 'weldingstdhrs',
        'weldingacthrs': 'weldingacthrs',
        'baseformpaintstdhrs': 'baseformpaintstdhrs',
        'baseformpaintacthrs': 'baseformpaintacthrs',
        'fanassyteststdhrs': 'fanassyteststdhrs',
        'fanassytestacthrs': 'fanassytestacthrs',
        'insulwallfabstdhrs': 'insulwallfabstdhrs',
        'insulwallfabacthrs': 'insulwallfabacthrs',
        'assystdhrs': 'assystdhrs',
        'assyacthrs': 'assyacthrs',
        'doorfabstdhrs': 'doorfabstdhrs',
        'doorfabacthrs': 'doorfabacthrs',
        'electricalstdhrs': 'electricalstdhrs',
        'electricalacthrs': 'electricalacthrs',
        'pipestdhrs': 'pipestdhrs',
        'pipeacthrs': 'pipeacthrs',
        'paintstdhrs': 'paintstdhrs',
        'paintacthrs': 'paintacthrs',
        'teststdhrs': 'teststdhrs',
        'testacthrs': 'testacthrs',
        'cratingstdhrs': 'cratingstdhrs',
        'cratingacthrs': 'cratingacthrs',  # Ensure cratingacthrs is mapped
        'flowlineacthrs': 'flowlineacthrs'  # Added flowlineacthrs to mapping
    }

    # Populate hour-related columns
    for file1_col, cleaned_col in column_mapping.items():
        if file1_col in df.columns:
            # Attempt to convert to numeric, fill NaN with 0
            cleaned_df[cleaned_col] = pd.to_numeric(df[file1_col], errors='coerce').fillna(0)
        else:
            # If the column doesn't exist in file1, set default value
            cleaned_df[cleaned_col] = 0

    logger.info("Mapped and populated hour-related columns.")

    # Handle 'ShipMonth' by extracting the month number from 'ShipDate'
    if 'shipdate' in df.columns:
        df['shipdate'] = pd.to_datetime(df['shipdate'], errors='coerce')
        cleaned_df['shipmonth'] = df['shipdate'].dt.month.fillna(0).astype(int)
        logger.info("Extracted 'shipmonth' from 'shipdate'.")
    else:
        logger.warning("Column 'shipdate' not found in the input file. Setting 'shipmonth' to 0.\n")
        cleaned_df['shipmonth'] = 0

    # Handle AssyActHrs and FlowLineActHrs
    if 'assyacthrs' in cleaned_df.columns and 'flowlineacthrs' in cleaned_df.columns:
        # Create a mask where 'assyacthrs' == 0 and 'flowlineacthrs' > 0
        mask = (cleaned_df['assyacthrs'] == 0) & (cleaned_df['flowlineacthrs'] > 0)

        # Replace 'assyacthrs' with 'flowlineacthrs' where mask is True
        cleaned_df.loc[mask, 'assyacthrs'] = cleaned_df.loc[mask, 'flowlineacthrs']

        # Mark 'flowline' as 1 where data was moved
        cleaned_df.loc[mask, 'flowline'] = 1

        logger.info("Handled 'assyacthrs' and 'flowlineacthrs' as per the specified logic.\n")
    else:
        logger.warning("One or both of the columns 'assyacthrs' and 'flowlineacthrs' are missing. Skipping AssyActHrs and FlowLineActHrs handling.\n")
        # Ensure 'flowline' column exists and set to 0
        if 'flowline' not in cleaned_df.columns:
            cleaned_df['flowline'] = 0

    # Drop 'flowlineacthrs' as it's no longer needed
    if 'flowlineacthrs' in cleaned_df.columns:
        cleaned_df.drop(columns=['flowlineacthrs'], inplace=True)
        logger.info("Dropped column 'flowlineacthrs' after processing.\n")

    # Remove 'test_lda' and 'test_ld' if they exist
    for col in ['test_lda', 'test_ld']:
        if col in cleaned_df.columns:
            cleaned_df.drop(columns=[col], inplace=True)
            logger.info(f"Dropped column '{col}' as per request.\n")

    # **Ensure 'flowline' column shows 0 when not 1**
    if 'flowline' in cleaned_df.columns:
        cleaned_df['flowline'] = cleaned_df['flowline'].fillna(0).astype(int)
    else:
        cleaned_df['flowline'] = 0

    # Ensure all required columns are present in the cleaned DataFrame
    for col in other_columns:
        if col not in cleaned_df.columns:
            # Assign default values based on column type
            if col == 'shipmonth':
                cleaned_df[col] = 0
            elif col in ['mmp', 'sppp', 'lau', 'vfd', 'alum', 'airflow', 'leaktest', 'deflection', 'indoor', 'outdoor', 'flowline', 'cratingacthrs']:
                cleaned_df[col] = 0
            elif col == 'code':
                cleaned_df[col] = 0
            elif col in ['height', 'sqft']:
                cleaned_df[col] = 0.0
            else:
                cleaned_df[col] = 0

    # Reorder columns to match the cleaned_columns list
    cleaned_df = cleaned_df[cleaned_columns]

    # ================= Additional summary-to-cleaned mappings =================
    # Both DataFrames are aligned by index — no merging required
    # Add requested columns using values from df (SCHSchedulingSummaryReport.csv)
    try:
        # Helper to map original header to our normalized df columns
        def _norm_key(k):
            k = str(k).strip().lower()
            k = re.sub(r'\s+', '_', k)
            k = re.sub(r'[^\w]', '', k)
            return k

        def _get_series(src_df: pd.DataFrame, key, alt=None) -> pd.Series:
            nk = _norm_key(key)
            if nk in src_df.columns:
                return src_df[nk]
            if alt is not None:
                ak = _norm_key(alt)
                if ak in src_df.columns:
                    return src_df[ak]
            # Fallback to empty series aligned to index
            return pd.Series([pd.NA]*len(src_df), index=src_df.index)

        def _to_num(s: pd.Series) -> pd.Series:
            # Convert strings like '75%', '1,234', '  42  ' to numeric; else 0
            if s is None:
                return pd.Series([0]*len(cleaned_df), index=cleaned_df.index, dtype='float64')
            s = s.astype(str).str.strip()
            s = s.replace(r'^\s*$', pd.NA, regex=True)
            s = s.str.replace('%','', regex=False)
            s = s.str.replace(',','', regex=False)
            s = s.str.replace(r'[^0-9\.-]','', regex=True)
            return pd.to_numeric(s, errors='coerce').fillna(0)

        # Excel column letter to zero-based index
        def _excel_col_to_index(col_letters):
            col_letters = str(col_letters).strip().upper()
            if not re.fullmatch(r"[A-Z]+", col_letters):
                raise ValueError(f"Invalid Excel column label: {col_letters}")
            n = 0
            for ch in col_letters:
                n = n * 26 + (ord(ch) - ord('A') + 1)
            return n - 1

        # Simple name-based mappings
        cleaned_df["Build Date"] = _get_series(df, "BuildDate")
        cleaned_df["Ship Date"] = _get_series(df, "ShipDate")

        cleaned_df["Fab Efficiency"] = _to_num(_get_series(df, "FabEfficiency"))
        cleaned_df["Fab Completion"] = _to_num(_get_series(df, "Fab"))

        cleaned_df["Welding Efficiency"] = _to_num(_get_series(df, "WeldingEfficiency"))
        cleaned_df["Welding Completion"] = _to_num(_get_series(df, "Welding"))

        cleaned_df["BaseFormPaint Efficiency"] = _to_num(_get_series(df, "BaseFormPaintEfficiency"))
        cleaned_df["BaseFormPaint Completion"] = _to_num(_get_series(df, "BaseFormPaint"))

        cleaned_df["FanAssyTest Efficiency"] = _to_num(_get_series(df, "FanAssyTestEfficiency"))
        cleaned_df["FanAssyTest Completion"] = _to_num(_get_series(df, "FanAssyTest"))

        cleaned_df["InsulWallFab Efficiency"] = _to_num(_get_series(df, "InsulWallFabEfficiency"))
        # Some files use InsuWallFab vs InsulWallFab
        cleaned_df["InsulWallFab Completion"] = _to_num(_get_series(df, "InsulWallFab", alt="InsuWallFab"))

        cleaned_df["DoorFab Efficiency"] = _to_num(_get_series(df, "DoorFabEfficiency"))
        cleaned_df["DoorFab Completion"] = _to_num(_get_series(df, "DoorFab"))

        cleaned_df["Electrical Efficiency"] = _to_num(_get_series(df, "ElectricalEfficiency"))
        cleaned_df["Electrical Completion"] = _to_num(_get_series(df, "Electrical"))

        cleaned_df["Pipe Efficiency"] = _to_num(_get_series(df, "PipeEfficiency"))
        cleaned_df["Pipe Completion"] = _to_num(_get_series(df, "Pipe"))

        cleaned_df["Paint Efficiency"] = _to_num(_get_series(df, "PaintEfficiency"))
        cleaned_df["Paint Completion"] = _to_num(_get_series(df, "Paint"))

        cleaned_df["Crating Efficiency"] = _to_num(_get_series(df, "CratingEfficiency"))
        cleaned_df["Crating Completion"] = _to_num(_get_series(df, "Crating"))

        # Assembly metrics via Excel letters BL/BM/CA/CB by position
        try:
            bl_i = _excel_col_to_index('BL')
            ca_i = _excel_col_to_index('CA')
            bm_i = _excel_col_to_index('BM')
            cb_i = _excel_col_to_index('CB')

            s_bl = _to_num(df.iloc[:, bl_i]) if bl_i < df.shape[1] else pd.Series([0]*len(df), index=df.index)
            s_ca = _to_num(df.iloc[:, ca_i]) if ca_i < df.shape[1] else pd.Series([0]*len(df), index=df.index)
            s_bm = _to_num(df.iloc[:, bm_i]) if bm_i < df.shape[1] else pd.Series([0]*len(df), index=df.index)
            s_cb = _to_num(df.iloc[:, cb_i]) if cb_i < df.shape[1] else pd.Series([0]*len(df), index=df.index)

            cleaned_df["Assembly Efficiency"] = pd.concat([s_bl, s_ca], axis=1).max(axis=1)
            cleaned_df["Assembly Completion"] = pd.concat([s_bm, s_cb], axis=1).max(axis=1)
        except Exception as _e:
            logger.warning(f"Assembly metrics could not be derived from Excel letters: {_e}")
            # Graceful fallback to zeros
            cleaned_df["Assembly Efficiency"] = 0
            cleaned_df["Assembly Completion"] = 0

        logger.info("Added SCHSchedulingSummary mappings to cleaned_df.")
    except Exception as e:
        logger.error(f"Failed to add SCHSchedulingSummary mappings: {e}")
    # ================= End additional mappings =================

    # Save the cleaned DataFrame to a CSV file
    try:
        cleaned_df.to_csv(output_csv_path, index=False, encoding='utf-8')
        logger.info(f"Cleaned data successfully written to '{output_csv_path}'.")
        print(f"\nConversion complete. Output saved to '{output_csv_path}'.\n")
    except Exception as e:
        logger.error(f"Error writing to CSV file: {e}\n")
        print(f"Error writing to CSV file: {e}\n")

def main():
    """Main function to execute the conversion."""
    print("=== File1 to Cleaned File Converter ===\n")
    file1_path = prompt_file_path()
    output_csv_path = 'cleaned_file.csv'  # Output as CSV

    # Count total rows before processing
    try:
        with open(file1_path, 'r', encoding='utf-8') as f:
            total_rows_input = sum(1 for _ in f) - 1  # Subtract header
        print(f"Total rows in input file: {total_rows_input}\n")
        logger.info(f"Total rows in input file: {total_rows_input}")
    except Exception as e:
        logger.error(f"Error counting rows in input file: {e}")
        print(f"Error counting rows in input file: {e}\n")
        return

    convert_file1_to_cleaned(file1_path, output_csv_path)

    # After processing, read the output and count rows
    try:
        cleaned_df = pd.read_csv(output_csv_path, encoding='utf-8')
        total_rows_output = len(cleaned_df)
        print(f"Total rows in output cleaned data: {total_rows_output}")
        logger.info(f"Total rows in output cleaned data: {total_rows_output}")
    except Exception as e:
        logger.error(f"Error reading the output file for validation: {e}")
        print(f"Error reading the output file for validation: {e}")

if __name__ == "__main__":
    main()
