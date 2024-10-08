import os
import shutil
import json
import ast
import math
import hashlib
import chardet
import re
import warnings
from datetime import datetime
from dateutil import parser as dt_parser
import pandas as pd
import numpy as np

#---------------------------------------------------------------------------------- 

# warnings to silence
warnings.simplefilter("ignore", UserWarning)
try:
    # Ignore panda DtypeWarnigs for low memory option (can't avoid in unknown schemas)
    warnings.filterwarnings('ignore', message="^Columns.*")
except:
    pass
try:
    # Ignore future warning on silent down casting (code assumes new method)
    pd.set_option('future.no_silent_downcasting', True)
except:
    pass
    
#---------------------------------------------------------------------------------- 

# Config class
class Config:
    """
    Configuration class for storing package constants and settings.

    This class provides a central location for managing configuration settings
    and constants used throughout the schema_validata package.

    Attributes:
        NA_VALUES (list): List of strings representing values to be treated as NaN.
        NA_PATTERNS (list): List of regex patterns for identifying values to be treated as NaN.
        DATE_FORMAT (str): Default date format used for parsing dates in the application.
        ...
    Example:
        config/DATA_DICT_PRIMARY_KEY = 'Name' # Changing a configuration attribute 
        print(config.NA_VALUES)  # Accessing a configuration attribute
    """
    
    # Data dictionary schema
    DATA_DICT_SCHEMA = {
        "field_name": "object",
        "required": "object",
        "data_type": "object",
        "allow_null": "object",
        "length": "int",
        "range_min": "float",
        "range_max": "float",
        "regex_pattern": "object",
        "unique_value": "object",
        "allowed_value_list": "object"
    }
    # Data dictionary schema primary key field
    DATA_DICT_PRIMARY_KEY = "field_name"

    # Overview error message string formats
    SCHEMA_ERROR_TEMPLATES = {
        'required_column'       : "Column by name '{col}' is required, but missing in dataset.",  
        'optional_column'       : "Column by name '{col}' is missing in the dataset, but is optional.",  
        'allow_null'            : "Column '{col}' data has {count} null values, null values are not allowed.",
        'data_type'             : "Column '{col}' data type: {observed} does not match the required data type: {expected} .", 
        'unique_value'          : "Column '{col}' values must be unique. Found  {count} duplicate values in dataset column .",
        'length'                : "Column '{col}' max string  of: {observed} exceeds the max allowed  of: {expected} .",
        'range_min'             : "Column '{col}' min value of: {observed} is less than the minimum allowed value of: {expected} .",
        'range_max'             : "Column '{col}' max value of: {observed} exceeds the maximum allowed value of: {expected} .",
        'allowed_value_list'    : "Column '{col}' contains values that are not allowed: {err_vals} .",
        'regex_pattern'         : "Column '{col}' contains values which do not match the allowed format/pattern ."
    }

    # Common US & ISO timestamp formats
    COMMON_TIMESTAMPS = [
                        # Common US Formats - Time 
                        "%I:%M:%S %p", # 12-Hour Time to seconds with AM/PM (Very Common)
                        "%I:%M:%S%p",  # 12-Hour Time to seconds with AM/PM (Common)
                        "%I:%M %p",    # 12-Hour Time to mins with AM/PM (Very Common)
                        "%I:%M%p",     # 12-Hour Time to mins with AM/PM (Common)
                        # Common International and ISO Standard Date Formats (ISO 8601)
                        "%H:%M:%S",    # 24-Hour Time (24-hour clock, US Military, Technical)
                        "%H:%M:%S %p"  # 24-Hour Time with AM/PM
                        ]

    # Common US & ISO date/datetime formats
    COMMON_DATETIMES = [
       
                        # US Formats - Date
                        "%m/%d/%Y",    # Month/Day/Year (Most Common)
                        "%d/%m/%Y",    # Day/Month/Year (Common)
                        "%b-%d-%Y",    # Month Abbreviation-Day-Year (e.g., Jan-01-2024)
                        "%B %d, %Y",   # Month Name-Day, Year (e.g., January 01, 2024)
                        "%Y-%m-%d",    # Year-Month-Day (ISO 8601 & Increasingly Common in US)
                        "%d-%m-%Y",    # Day-Month-Year (Less Common)

                        # US Date Time Formats 
                        "%m/%d/%Y %H:%M:%S", # Date and Time with Separators (Common)
                        "%Y-%m-%d %H:%M:%S", # Date and Time with Separators (Less Common)
                        "%d-%m-%Y %H:%M:%S", # Date and Time with Separators (Uncommon)

                        # ISO Standard Date Formats (ISO 8601)
                        #"%Y-%m-%d", # Year-Month-Day (Standard, Consistent) used above
                        "%Y-%m",   # Year-Month (Less Common)

                        # ISO Standard Date Time Formats (ISO 8601)
                        "%Y-%m-%dT%H:%M:%SZ", # Combined Date and Time with Zulu Time (Specific Use Cases)
                        "%Y-%m-%dT%H:%M:%S%z", # Combined Date and Time with Offset (Rare)
                        ]

    # Standard pandas null value reps with other common formats, values will be read in as nulls
    NA_VALUES = ['', ' ', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan','1.#IND', 
                         '1.#QNAN', '<NA>', 'N/A', 'NA', 'NULL', 'NaN', 'n/a','nan', 'null', 'Null', 'NULL',
                         np.nan, None, 'None'
                        ]   

    # Standard pattern reps for nulls, values will be converted to nulls
    NA_PATTERNS = [
		    r'(?i)^\s*NOT\s{0,1}(?:\s|_|-|/|\\|/){1}\s{0,1}AVAILABLE\s*$',
		    r'(?i)^\s*N\s{0,1}(?:\s|_|-|/|\\|/){1}\s{0,1}A\s*$',
		    r'(?i)^\s*(?:\s|_|-|/|\\|/){1}\s*$',
		    r'^\s+$'
		    ]

    class jsonEncoder(json.JSONEncoder):
        """Custom JSON encoder class that handles serialization of NumPy data types
        (int64, float64, and arrays) for compatibility with JSON.

        This class inherits from `json.JSONEncoder` and overrides the `default` method
        to provide custom logic for serializing specific object types.
        """

        def default(self, obj):
            """
            Overrides the default method of JSONEncoder to handle specific object types.

            Parameters
            ----------
            obj: 
                The object to be serialized.
            Returns
            -------
                A JSON-serializable representation of the object.
            """
            if isinstance(obj, np.integer):
                """Handle NumPy integer types (e.g., int64) by converting them to regular Python int."""
                return int(obj)
            elif isinstance(obj, np.floating):
                """Handle NumPy floating-point types (e.g., float64) by converting them to regular Python float."""
                return float(obj)
            elif isinstance(obj, np.ndarray):
                """Handle NumPy arrays by converting them to lists for JSON encoding."""
                return self.encode(obj.tolist())  # Recursively convert to list
            return super().default(obj)

#---------------------------------------------------------------------------------- 

def get_byte_units(size_bytes):
    """Converts bytes into the largest possible unit of measure.

    Parameters
    ----------
    size_bytes : int
        Numeric value representing bytes.

    Returns
    -------
    str
        String representing the value and the largest unit size.
        Example: '200 : GB'
    """
    if size_bytes == 0:
        return '0 : B'

    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1000)))
    p = math.pow(1000, i)
    s = round(size_bytes / p, 2)

    return f'{s} : {size_name[i]}'

# ----------------------------------------------------------------------------------

def get_md5_hash(file_path):
    """Generates an MD5 hash for the contents of a 
    files contents.

    Parameters
    ----------
    file_path : str
        Path to the file.

    Returns
    -------
    str
        MD5 hash string.
    """

    try:
        with open(file_path, "rb") as file:
            file_hash = hashlib.md5()
            while True:
                chunk = file.read(8192)
                if not chunk:
                    break
                file_hash.update(chunk)
        return file_hash.hexdigest()
    except FileNotFoundError:
        return f"File not found: {file_path}"
    except PermissionError:
        return f"Permission error reading file: {file_path}"
    except Exception as e:
        return f"An error occurred: {str(e)}"

# ----------------------------------------------------------------------------------

def get_spreadsheet_metadata(file_path):
    """Returns a dictionary with general metadata for a CSV or Excel file.

    Parameters
    ----------
    file_path : str
        Path to the CSV or Excel file.

    Returns
    -------
    dict
        Dictionary of file metadata.
    """
    try:
        # Extract filename and extension
        filename = os.path.basename(file_path)
        base_name, ext = os.path.splitext(filename)

        # Get date time file metadata
        statinfo = os.stat(file_path)
        create_date = datetime.fromtimestamp(statinfo.st_ctime).isoformat()
        modified_date = datetime.fromtimestamp(statinfo.st_mtime).isoformat()

        # Create dictionary to store the metadata
        file_meta = {}

        # Read the data into a pandas dataframe by sheet
        dfs = read_csv_or_excel_to_df(file_path, infer=True, multi_sheets=True)

        file_hash = get_md5_hash(file_path)
        for sheet_name, df in dfs.items():
            meta = {
                'file_path': file_path,
                'file_name': filename,
                'file_type': ext,
                'file_size_bytes': f'{statinfo.st_size:,}',
                'file_size_memory_unit': get_byte_units(int(statinfo.st_size)),
                'record_qty': f'{len(df):,}',
                'column_qty': f'{len(df.columns):,}',
                'file_md5_hash': file_hash,
                'created': create_date,
                'modified': modified_date
            }

            # Generate the schema dictionary
            file_meta[sheet_name] = meta

        return file_meta

    except FileNotFoundError:
        return f"File not found: {file_path}"
    except PermissionError:
        return f"Permission error reading file: {file_path}"
    except Exception as e:
        return f"An error occurred: {str(e)}"

# ----------------------------------------------------------------------------------

def is_numeric_type(value):
    """
    Checks if a value is a common numeric data type in 
    pandas, NumPy, or Python.

    Parameters:
    ----------
        value: The value to check.
    Returns:
    -------
        bool: True if the value is numeric, False otherwise.
    """
    # Check for standard numeric types (int, float, complex)
    if isinstance(value, (int, float, complex)):
        return True
    # Check for NumPy numeric dtypes using np.issubdtype
    elif np.issubdtype(type(value), np.number):
        return True
    else:
        return False

# ----------------------------------------------------------------------------------

def downcast_ints(value):
    """
    Downcast a numeric value to an integer if it is equal to 
    a float representation.
    
    Parameters
    ----------
    value: The numeric value to downcast.
    
    Returns
    -------
    The value as an integer if it is equal to its float 
    representation, otherwise the original value.
    """
    try:
        if isinstance(value, float) and int(value) == float(value):
            value = int(value)
    except ValueError:
        # Handle the case where the conversion to int fails
        pass
    return value

# ----------------------------------------------------------------------------------

def get_best_uid_column(df, 
                        preferred_column=None
                        ):
    """
    Identifies the column with the most unique values 
    (excluding nulls) in a DataFrame.

    Parameters:
    -----------
    df : pandas.DataFrame
        The input DataFrame.
    preferred_column : str, optional
        The preferred column if ties for uniqueness occur.

    Returns:
    --------
    str or None
        The column name with the most unique values, 
        or None if none qualify.

    Raises:
    -------
    ValueError
        If `df` is not a pandas DataFrame.
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError("Input must be a pandas DataFrame.")

    uniq_cnts = {}
    for col in df.columns:
        uid_dtypes = ['Integer', 'String']
        if infer_data_types(df[col]) in uid_dtypes:
            # Drop null values and calculate unique values
            unique_vals = df[col].dropna().nunique()
            # Filter only unique columns
            uniq_cnts[col] = int(unique_vals)

    if bool(uniq_cnts):
        # Find the key with the highest value using a loop and max
        max_value = max(uniq_cnts.values())
        uid_cols = [c for c, uc in uniq_cnts.items() 
                    if uc > 0 and uc == max_value]
    else:
        return preferred_column

    # Prioritize preferred_column in case of ties
    if len(uid_cols) > 0:
        if bool(preferred_column):
            uid_cols = [c for c in uid_cols 
                        if uniq_cnts[c] > uniq_cnts[preferred_column]]
            if len(uid_cols) == 0:
                return preferred_column
            else:
                return uid_cols[0]
        # Return the first column if tied
        else:
            return uid_cols[0]

# ----------------------------------------------------------------------------------

def eval_nested_string_literals(data):
    """
    Iterates through a nested dictionary or JSON object, attempting to evaluate
    string representations of data types (e.g., lists, dict, tuples) into their 
    actual Python counterparts. Modifies the structure in-place, replacing string
    representations with evaluated values.

    Parameters:
    -----------
    data : dict or str
        The nested dictionary to iterate through, or a JSON string to be parsed.

    Returns:
    --------
    dict
        The modified dictionary with string representations replaced by evaluated
        values.

    Raises:
    -------
    ValueError
        If the provided data is not a valid JSON string.
    """
    if isinstance(data, str):
        try:
            data = json.loads(data)  # Parse JSON string if needed
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON string: {e}")

    for key, value in data.items():
        if isinstance(value, dict):
            eval_nested_string_literals(value)  # Recursive call for nested dictionaries
        else:
            try:
                value = value.strip('"\'')
            except AttributeError:
                pass

            try:
                evaluated_value = ast.literal_eval(value)
            except (SyntaxError, ValueError):
                evaluated_value = value  # Ignore evaluation errors

            if value != evaluated_value:  # Replace only if evaluation is successful
                data[key] = evaluated_value  # Replace the value with the object    

    return data

# ----------------------------------------------------------------------------------

def remove_pd_df_newlines(df, 
                          replace_char=''
                          ):
    """
    Removes newline characters ('\n') from all string 
    columns in a pandas DataFrame with the given replace 
    character.

    Parameters:
    -----------
    df : pandas.DataFrame
        The DataFrame to process.
    replace_char : str, optional
        String value to replace newline character with.
        Defaults to single space (' ').

    Returns:
    --------
    pandas.DataFrame
        The DataFrame with newlines removed from string columns.
    """
    df = df.replace('\n', replace_char, regex=True)
    return df

# ----------------------------------------------------------------------------------

def column_is_timestamp(df, 
                        column_name, 
                        time_format
                        ):
    """
    Checks if all non-null values in a DataFrame 
    column can be parsed as time-only given a 
    specific format.

    Parameters:
    -----------
    df : pd.DataFrame
        The DataFrame containing the column.
    column_name : str
        The name of the column to check.
    time_format : str
        The expected string time format.

    Returns:
    --------
    bool
        True if all non-null values in the column can 
        be parsed as time, False otherwise.
    """
    column_as_str = df[column_name].astype(str)
    column_as_str = column_as_str.replace(r'^\s+$', pd.NA, regex=True)  
    column_as_str = column_as_str.dropna()
    
    if len(column_as_str) == 0:
        return False
        
    def try_parse(time_str):
        try:
            if not isinstance(time_str, str):
                return False
            datetime.strptime(time_str, time_format).time()
            return True
        except ValueError:
            return False

    return column_as_str.apply(try_parse).all()

# ----------------------------------------------------------------------------------

def infer_datetime_column(df, 
                          column_name
                          ):
    """
    Attempts to convert a pandas column to datetime 
    type, handling various formats. Integer columns 
    will not be attempted.

    Parameters:
    -----------
    df : pandas.DataFrame
        The pandas DataFrame containing the column.
    column_name : str
        The name of the column to convert.

    Returns:
    --------
    pandas.DataFrame column or pandas.Series
        The column/series converted to a datetime type if successful,
        otherwise the unaltered column is returned.
    """

    column_copy = df[column_name].copy()

    string_column = df[column_name].astype(str)
    string_column = string_column.replace(r'^\s+$', pd.NA, regex=True)  
    string_column = string_column.dropna()
        
    if len(string_column) == 0:
        return column_copy
    
    # Check for strings
    if pd.api.types.is_string_dtype(string_column):
        
        # Try converting to int before checking other types
        # int may incorrectly be converted to dates
        try:
            converted_numeric = pd.to_numeric(string_column)
            if pd.api.types.is_integer_dtype(converted_numeric):
                return column_copy
        except:

            # Check if all non-null values can be parsed as
            # standalone timestamps. If so, leave it be
            is_timestamp = all(column_is_timestamp(
                                df, 
                                column_name, 
                                ts_format) 
                                for ts_format in Config.COMMON_TIMESTAMPS)
            if is_timestamp:
                return column_copy

            # Try specific formatting attempts
            for date_format in Config.COMMON_DATETIMES:
                try:
                    string_column = pd.to_datetime(string_column, 
                                                   format=date_format)
                    return string_column
                    print(column_name, date_format)
                except:
                    pass

            try:
                if pd.api.types.is_string_dtype(string_column):
                    string_column = column_copy.apply(dt_parser.parse)
                    return string_column
                    print(column_name, date_format)
            except:
                pass

    # No successful parsing
    return column_copy

# ----------------------------------------------------------------------------------

def detect_file_encoding(file_path):
    """Detects the character encoding of a text-based file using chardet library.

    This function is useful for determining the appropriate encoding when reading
    files that may not explicitly declare their encoding. It analyzes a sample
    of the file's content to identify the most likely character encoding scheme
    used.

    Parameters:
    ----------
    file_path (str):
        The path to the target file.
    Returns:
    ----------
    str:
        The detected character encoding of the file. If chardet cannot
        determine the encoding with sufficient confidence (less than 50%),
        the function returns the pandas default encoding=None or ('utf-8') 
        as a default fallback.
    Raises:
    ----------
    OSError:
        If the specified file cannot be opened for reading.
    """

    try:
        # Open the file in binary mode to read raw bytes
        with open(file_path, 'rb') as f:
            rawdata = f.read()
    except OSError as e:
        raise OSError(f"Error opening file: {file_path}. {e}")

    # Use chardet to analyze the byte data and detect encoding
    result = chardet.detect(rawdata)

    # Check confidence level of the detection
    if result['confidence'] > 0.5:
        encoding = result['encoding']
    else:
        # Confidence level below 50%, return a safe default encoding (utf-8)
        encoding = None
        print(f"Encoding confidence for '{file_path}' is low (< 50%). Using pandas default.")

    return encoding

# ----------------------------------------------------------------------------------

def read_spreadsheets(file_path, 
                      sheet_name=None, 
                      dtype=None, 
                      rm_newlines=True,
                      replace_char="", 
                      na_values=None
                      ):
    """
    Reads and processes raw data from Excel (.xlsx, .xls) or CSV (.csv) files 
    into a pandas DataFrame accounting for newline/return characters and datatypes. 
    Parameters:
    ----------
    file_path (str): 
        The path to the data file.
    sheet_name (str, optional): 
        The name of the sheet to read from an Excel file 
        (default: None, reads the first sheet).
    dtype (dict, optional): 
        A dictionary mapping column names to desired data types 
        (default: None, inferred from data).
    rm_newlines (bool, optional): 
        If True, removes newline characters from the data 
        (default: True).
    replace_char (str, optional): 
        The character to replace newline characters with 
        (default: empty string "").
    na_values : scalar, str, list-like, or dict, optional
        Additional strings to recognize as NA/NaN. If dict passed, specific
        per-column NA values.  By default the following values are interpreted as
        NaN: '', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan',
        '1.#IND', '1.#QNAN', '<NA>', 'N/A', 'NA', 'NULL', 'NaN', 'n/a',
        'nan', 'null'.

    Returns:
    -------
    pandas.DataFrame: 
        The DataFrame containing the data from the file.

    Raises:
    -------
    ValueError: 
        If the file extension is not supported (.xlsx, .xls, or .csv).
    """
    if not na_values:
        na_values = Config.NA_VALUES

    filename = os.path.basename(file_path)
    base_name, ext = os.path.splitext(filename)
    
    

    if ext in [".xlsx", ".xls"]:
        xls = pd.ExcelFile(file_path)  
        df = pd.read_excel(file_path, 
                           sheet_name=sheet_name, 
                           dtype=dtype, 
                           na_values=na_values)
    elif ext == ".csv":
        encoding=detect_file_encoding(file_path)
        df = pd.read_csv(file_path, 
                         dtype=dtype, 
                         na_values=na_values,
                         encoding=encoding)
    else:
        raise ValueError(f"Unsupported file extension: {ext}")

    if rm_newlines:
        df = remove_pd_df_newlines(df, replace_char=replace_char)

    # Use str.strip() to remove leading and trailing spaces from column names
    df.columns = df.columns.str.strip()

    return df

# ----------------------------------------------------------------------------------

def xlsx_tabs_to_pd_dataframes(file_path, 
                               infer=True,
                               rm_newlines=True, 
                               replace_char="",
                               na_values=Config.NA_VALUES,
                               na_patterns=Config.NA_PATTERNS
                               ):
    """
    Read all sheets/tabs from an excel file into a dictionary of 
    pandas DataFrames.

    Parameters:
    ----------
    file_path (str):
        Path to the Excel file.
    infer: (bool, optional): 
        If True, use read_df_with_optimal_dtypes to infer datatypes.
        If False, use the pandas default. 
        (default: True).
    rm_newlines (bool, optional): 
        If True, removes newline characters from the data 
        (default: True).
    replace_char (str, optional): 
        The character to replace newline characters with 
        (default: empty string "").
    na_values: (Optional) 
        List of values to consider nulls in addition to standard nulls. 
        (default: None)
    na_patterns: (Optional) 
        List of regular expressions to identify strings representing missing values. 
        (default: None)     
    Returns:
    -------
    dict
        A dictionary containing a DataFrame for each sheet in 
        the Excel file or the CSV file itself.
    """
    
    # Use a dictionary to store DataFrames
    dfs = {}

    xls = pd.ExcelFile(file_path)

    # Determine the key for the dictionary based on the file extension
    filename = os.path.basename(file_path)
    base_name, ext = os.path.splitext(filename)

    # Iterate through each worksheet and read its data into a DataFrame
    for sheet_name in xls.sheet_names:
        # Choose the appropriate function based on the 'infer' parameter
        if infer:
            df = read_df_with_optimal_dtypes(file_path, 
                                             sheet_name=sheet_name,
                                             rm_newlines=rm_newlines,
                                             replace_char=replace_char,
                                             na_values=na_values,
                                             na_patterns=na_patterns)
        else:
            df = read_spreadsheets(file_path,
                                   sheet_name=sheet_name,
                                   dtype=None, 
                                   rm_newlines=rm_newlines,
                                   replace_char=replace_char,
                                   na_values=na_values,
                                   na_patterns=na_patterns)

        # Set key for CSV files to ensure consistent dictionary keys
        key = base_name if ext == '.csv' else sheet_name
        dfs[key] = df

    return dfs

# ----------------------------------------------------------------------------------

def data_dict_to_json(data_dict_file, 
                      out_dir=None, 
                      out_name=None, 
                      na_values=Config.NA_VALUES, 
                      na_patterns=Config.NA_PATTERNS
                      ):
                      
    """Converts an XLSX data dictionary to a formatted JSON string.

    Reads an XLSX file containing data dictionary information, processes 
    it according to a schema, and generates a JSON representation.

    Parameters:
    ----------
    data_dict_file (str): 
        Path to the XLSX data dictionary file.
    out_dir (str, optional): 
        Path to the output directory for the JSON file. 
        Defaults to None.
    out_name (str, optional): 
        Desired name for the output JSON file (without extension). 
        Defaults to None.
    na_values: (Optional) 
        List of values to consider nulls in addition to standard nulls. 
        (default: None)
    na_patterns: (Optional) 
        List of regular expressions to identify strings representing missing values. 
        (default: None)
    Returns:
    -------
        json_string (str):
        Formatted JSON string representing the processed data dictionary.
    """
    try:
        # Read the xlxs data dictionary file, convert each tab into a dataframe, 
        # Return a dictionary {tabName: dataframe}
        dfs = xlsx_tabs_to_pd_dataframes(data_dict_file,
                                            rm_newlines=True, 
                                            replace_char='',
                                            infer=True,
                                            na_values=na_values,
                                            na_patterns=na_patterns)

        # Iterate through the dataframes to create a new subset dictionary
        data_dict = {}
        for sheet_name, df in dfs.items():
            # Check if each sheet/tab matches the data dictionary columns/schema and is not empty
            if set(Config.DATA_DICT_SCHEMA.keys()).issubset(set(df.columns)) and len(df) != 0 :
                # Ensure data types
                df_with_types = df.astype(Config.DATA_DICT_SCHEMA, errors='ignore') 
                # Ignore rows without a field/column name
                df_with_types = df_with_types.dropna(subset=[Config.DATA_DICT_PRIMARY_KEY], 
                                                     inplace=False)
                
                # Convert the dataframes into dictionaries for easier lookup
                df_with_types = df_with_types.set_index(Config.DATA_DICT_PRIMARY_KEY) 
                sheet_schema = json.loads(df_with_types.to_json(orient='index')) 
                sheet_schema = {k: {**v, Config.DATA_DICT_PRIMARY_KEY: k} 
                                for k, v in sheet_schema.items()}
                data_dict[sheet_name] = sheet_schema

        # Convert any nested string literal lists, dicts, tuples into Python objects
        data_dict = eval_nested_string_literals(data_dict)

        if out_dir and out_name:
            # Convert the dictionary to a formatted JSON string
            json_string = json.dumps(data_dict, indent=4, sort_keys=True, cls=Config.jsonEncoder)
            output_path = os.path.join(out_dir, f'{out_name}.json')
            # Save the JSON text to a file
            with open(output_path, "w") as f:
                f.write(json_string)
            print(f'Data saved to: {output_path}')

    except FileNotFoundError:
        print(f"Error: File '{data_dict_file}' not found.")
    except Exception as e:  # Catch any type of exception
        print(f"An error occurred: {e}")  # Print the error message

    return data_dict

# ----------------------------------------------------------------------------------

def read_csv_or_excel_to_df(file_path,
                            infer=True,
                            multi_sheets=True, 
                            rm_newlines=True,
                            replace_char="",
                            na_values=Config.NA_VALUES,
                            na_patterns=Config.NA_PATTERNS
                            ):
    """
    Reads a CSV or Excel file and returns data as a dictionary 
    of DataFrames, where keys are sheet names and values are 
    DataFrames containing data from each sheet.

    Parameters
    ----------
    file_path (str): 
        Path to the CSV, XLSX, or XLS file.
    infer: (bool, optional): 
        If True, use read_df_with_optimal_dtypes to infer datatypes.
        If False, us the pandas default. 
        (default: True).      
    multi_sheets : bool, optional (default=False)
        If True, allows reading multiple sheets from Excel files.
        If False, raises an error if the Excel file has multiple sheets.
    rm_newlines (bool, optional): 
        If True, removes newline characters from the data 
        (default: True).
    replace_char (str, optional): 
        The character to replace newline characters with 
        (default: empty string "").
    na_values: (Optional) 
        List of values to consider nulls in addition to standard nulls. 
        (default: None)
    na_patterns: (Optional) 
        List of regular expressions to identify strings representing missing values. 
        (default: None)
    Returns
    -------
    dict
        Returns a dictionary of DataFrames, 
        where keys are sheet names and values are DataFrames 
        containing data from each sheet.

    Raises
    ------
    ValueError
        If the file has multiple sheets and multi_sheets=False, 
        or if the file format is unsupported.
    """

    def read_excel_file():
        try:
            return xlsx_tabs_to_pd_dataframes(file_path, 
                                              rm_newlines=rm_newlines, 
                                              replace_char=replace_char,
                                              infer=infer,
                                              na_values=na_values,
                                              na_patterns=na_patterns)
        except ImportError:
            raise ValueError(f"Failed to import: {file_path}")

    filename = os.path.basename(file_path)
    base_name, ext = os.path.splitext(filename)

    if ext in [".xlsx", ".xls"]:
        dfs = read_excel_file()

        if not multi_sheets and len(dfs) > 1:
            sheet_names = ", ".join(dfs.keys())
            raise ValueError(f"""File contains multiple sheets: 
                {sheet_names}. Allow multi_sheets is set to False!""")
        else:
            return dfs
    elif ext == ".csv":
        if infer:
            # Read with string dtypes for accurate inference
            return {base_name: 
                    read_df_with_optimal_dtypes(file_path,
                                                rm_newlines=rm_newlines,
                                                replace_char=replace_char,
                                                na_values=na_values,
                                                na_patterns=na_patterns)}
        else:
            return {base_name: read_spreadsheets(file_path, 
                                                 sheet_name=None, 
                                                 dtype=None, 
                                                 rm_newlines=rm_newlines, 
                                                 replace_char=replace_char,
                                                 na_values=na_values,
                                                 na_patterns=na_patterns)}
    else:
        raise ValueError(f"Unsupported file type: {ext}")

#---------------------------------------------------------------------------------- 

def identify_leading_zeros(df_col):
    """
    Identify columns with potential leading zeros.

    Parameters:
    ----------
    df_col : pandas.Series
        Column of a DataFrame.

    Returns:
    -------
    bool
        True if potential leading zeros are found, False otherwise.
    """
    return any(str(val).startswith("0") for val in df_col.dropna())

#---------------------------------------------------------------------------------- 

def check_all_int(df_col):
    """
    Check if all non-null values in a column can be inferred 
    as integers or floats.

    Parameters:
    ----------
    df_col : pandas.Series
        Column of a DataFrame.

    Returns:
    -------
    type
        Data type to use for the column.
    """
    try:
        _s = df_col.dropna()
        try:
            _s = pd.to_numeric(_s)
        except:
            pass
        if pd.api.types.is_bool_dtype(_s):
            return bool      
        elif pd.api.types.is_numeric_dtype(_s):
            all_ints = (_s - _s.astype(int) == 0).all()
            return 'Int64' if all_ints else 'Float64'
        else:
            return str
    except:
        return str

#---------------------------------------------------------------------------------- 

def read_spreadsheet_with_params(file_path, 
                                 sheet_name, 
                                 dtype, 
                                 na_values
                                 ):
    """
    Read spreadsheet with specified parameters.

    Parameters:
    ----------
    file_path : str
        File path to the spreadsheet.
    sheet_name : str or None
        The name of the sheet to read from an Excel file.
    dtype : type or dict
        Data type or dictionary of data types to use.
    na_values : scalar, str, list-like, or dict
        Additional strings to recognize as NA/NaN.

    Returns:
    -------
    pandas.DataFrame
        DataFrame containing data from the spreadsheet.
    """
    return read_spreadsheets(file_path, 
                             sheet_name=sheet_name, 
                             dtype=dtype, 
                             rm_newlines=True,
                             replace_char='',
                             na_values=na_values)

#---------------------------------------------------------------------------------- 

def read_df_with_optimal_dtypes(file_path,
                                sheet_name=None,
                                rm_newlines=True, 
                                replace_char='',
                                na_values=Config.NA_VALUES,
                                na_patterns=Config.NA_PATTERNS
                                ):
    """
    Infers optimal data types for a DataFrame or read, preserving 
    the the best datatype for each column including leading zeros
    boolean, strings, dates, ints, floats etc.

    Parameters
    ----------
    file_path (str):
        File path to the CSV, XLSX, or XLS file.    
    sheet_name (str, optional): 
      The name of the sheet to read from an Excel file 
      (default: None, reads the first sheet).    
    rm_newlines (bool, optional): 
      If True, removes newline characters from the data 
      (default: True).
    replace_char (str, optional): 
      The character to replace newline characters with 
      (default: empty string "").
    na_values: (Optional) 
        List of values to consider nulls in addition to standard nulls. 
        (default: None)
    na_patterns: (Optional) 
        List of regular expressions to identify strings representing missing values. 
        (default: None)
        
    Returns
    -------
        df (pandas.DataFrame): 
            A pandas DataFrame with inferred data types.
    """

    # Initialize empty data type dictionary
    dtypes = {}

    # Read the sheet without specifying initial data types   
    df = read_spreadsheet_with_params(file_path, sheet_name, str, na_values)
 
    # identify any null patterns as nulls and add the observed values to the na_values
    read_as_na = na_values.copy()

    for col in df.columns:
        null_p_vals = [v for v in df[col].unique().tolist()
                       if check_na_value(v, 
                                         na_values=na_values, 
                                         na_patterns=na_patterns)
                       and not pd.isna(v)] 

        if bool(null_p_vals):
            read_as_na.extend(list(set(null_p_vals)))

    read_as_na = list(set(read_as_na))

    # re-Read the sheet without specifying specific na values defined   
    df = read_spreadsheet_with_params(file_path, 
                                      sheet_name, 
                                      str, 
                                      read_as_na)

    # Identify potential leading zeros for each column
    for col in df.columns:
        non_null_values = df[col].dropna()
        
        if non_null_values.empty:
            dtypes[col] = object
            
        elif identify_leading_zeros(non_null_values):
            dtypes[col] = str  # Preserve leading zeros

        # Infer data types for other columns, ignoring nulls
        elif pd.api.types.is_bool_dtype(non_null_values):
            dtypes[col] = bool           
            
        elif pd.api.types.is_numeric_dtype(non_null_values):
            dtypes[col] = check_all_int(non_null_values)

        elif pd.api.types.is_string_dtype(non_null_values) or \
            pd.api.types.is_categorical_dtype(non_null_values):
            dtypes[col] = check_all_int(non_null_values)

        else:
            dtypes[col] = str             
                
    # read the data in again with the defined data types
    df = read_spreadsheet_with_params(file_path, 
                                      sheet_name, 
                                      dtypes, 
                                      read_as_na)
    
    # attempt to convert datetime strings to datetime data types
    with warnings.catch_warnings():
        # suppress RuntimeWarning during attempts to cast
        warnings.simplefilter("ignore", RuntimeWarning)  
        try:
            for col in df.columns:
                df[col] = infer_datetime_column(df, col)
        except:
            pass  # leave it be

    return df
    
#---------------------------------------------------------------------------------- 

def infer_data_types(pd_series):
    """
    Documents the most likely data type of a pandas Series based on 
    the non-null values in a pandas.Series / column.

    Parameters:
    ----------
        pd_series (pandas.Series): 
            The pandas series/column to analyze.

    Returns:
        str: 
            The name of the data type, including the values:
                 "Null-Unknown", "Boolean", "Integer", "Float", 
                 "Datetime", "String", or "Other".
    """
    # Empty strings, including whitespaces changed to pd.NA
    non_null_values = pd_series.replace(r'^\s+$', pd.NA, regex=True) 
    # ignore nulls in determining the datatype of the column
    non_null_values = non_null_values.dropna() 

    if non_null_values.empty:
        return "Null-Unknown"

    # Check for boolean
    elif pd.api.types.is_bool_dtype(non_null_values):
        return "Boolean"
    
    # Check for integers
    elif pd.api.types.is_integer_dtype(non_null_values):
        return "Integer"

    # Check for floats
    elif pd.api.types.is_float_dtype(non_null_values):
        return "Float"

    # Check for datetime
    elif pd.api.types.is_datetime64_any_dtype(non_null_values):
        return "Datetime"

    # Check for strings
    elif pd.api.types.is_string_dtype(non_null_values) or \
        pd.api.types.is_categorical_dtype(non_null_values):
        # Try converting to numeric before checking other types
        try:
            converted_numeric = pd.to_numeric(non_null_values)
            if pd.api.types.is_bool_dtype(converted_numeric):
                return "Boolean"
            if pd.api.types.is_integer_dtype(converted_numeric):
                return "Integer"
            else:
                return "Float"
        except:
            try:
                dt = pd.to_datetime(non_null_values.astype(str),
                                    infer_datetime_format=True)
                return "Datetime"
            except:
                return "String"

    # Other data types
    else:
        return "Other"

#---------------------------------------------------------------------------------- 

def check_na_value(value, 
                   na_values=Config.NA_VALUES, 
                   na_patterns=Config.NA_PATTERNS
                   ):
    """
    Checks if a value is considered a missing value based on predefined 
    patterns and custom values.

    Parameters:
    ----------
    value: 
        The value to be checked.
    na_values: (Optional) 
        List of values to consider nulls in addition to standard nulls. 
        (default: None)
    na_patterns: (Optional) 
        List of regular expressions to identify strings representing
        missing values. 
        (default: None)
    Returns:
    ----------
    bool: 
        True if the value is considered a missing/null value, 
        False otherwise.
    """
    # Check for common null-like values before further checks
    if pd.isna(value) or value is None:
        return True    
    elif isinstance(value, str):
        # Pre-compile regular expressions for efficiency 
        compiled_patterns = None
        if na_patterns:
            compiled_patterns = [re.compile(p) for p in na_patterns]
            if compiled_patterns and any(p.search(value) 
                                         for p in compiled_patterns):
                return True  # Return if any pattern matches
        if na_values: 
            if pd.isna(value):
                return pd.NA
            else:
                # Check for empty string after stripping whitespaces 
                # and presence in na_values
                return not value.strip() \
                           or (na_values and value in na_values) \
                           or pd.isna(value)
    else:
        # For non-strings, check only na_values
        return na_values and value in na_values

#---------------------------------------------------------------------------------- 

def series_hasNull(series, 
                   na_values=Config.NA_VALUES, 
                   na_patterns=Config.NA_PATTERNS
                   ):
    """
    Checks if a Pandas Series contains any null values or strings 
    matching predefined patterns.

    Parameters:
    ----------
    series : pd.Series
        The Pandas Series to check for null values or strings.
    na_values : list, optional
        List of values to consider as nulls 
        (default: Config.NA_VALUES).
    na_patterns : list, optional
        List of regular expressions to identify null strings 
        (default: Config.NA_PATTERNS).

    Returns:
    -------
    bool
        True if the Series contains any null values or strings 
        matching predefined patterns, 
        False otherwise.
    """

    # Use apply to vectorize the check_na_value function
    return series.apply(lambda x: check_na_value(x, na_values, na_patterns)).any()

#---------------------------------------------------------------------------------- 

def get_numeric_range(pd_series, 
                      attribute,
                      na_val=None
                      ):
    """
    Calculates the minimum or maximum value for a numeric Series, handling both 
    numerical and non-numerical cases.

    Parameters:
        pd_series (pd.Series): 
            The Pandas Series to process.
        attribute (str): 
            The desired statistical attribute, either 'min' or 'max'.
        na_val : (Any, optional): 
            The value to return if the Series is empty or non-numeric. 
            Defaults to 'N/A'.

    Returns:
        float, int, str: 
            The minimum or maximum value in the Series, or `na_val` if the Series is 
            empty or non-numeric. If the Series is numeric, returns the min or max 
            value as an integer if possible; otherwise, returns it as a float. If the 
            Series is empty or non-numeric, returns (na_val).
    """
    # Check for integers or float
    _s = pd_series.replace(r'^\s+$', pd.NA, regex=True)
    _s.fillna(pd.NA)  
    try:
        _s = pd.to_numeric(_s)
        _s.fillna(pd.NA) 
    except:
        pass

    _s = _s.dropna()

    if not pd.api.types.is_numeric_dtype(_s):
        return na_val  # Return `na_val` for non-numeric Series
    
    if attribute == 'min':
        return int(_s.min()) if int(_s.min()) == float(_s.min()) else float(_s.min())
    elif attribute == 'max':
        return int(_s.max()) if int(_s.max()) == float(_s.max()) else float(_s.max())

#---------------------------------------------------------------------------------- 

def build_data_dictionary(df, 
                          max_unique_vals=100,
                          false_val='False',
                          true_val='True',
                          na_val=None
                          ):
    """
    Creates a detailed data dictionary from a Pandas DataFrame, 
    capturing key attributes for each column.

    Parameters:
    ----------
    df (pandas.DataFrame): 
        The DataFrame to analyze.
    max_unique_vals (int, optional): 
        The maximum number of unique values to include in the 
        allowed value list for string columns. 
        Defaults to 100.
    false_val (str, optional): 
        The value to use for False boolean values. 
        Defaults to 'False'.
    true_val (str, optional): 
        The value to use for True boolean values. 
        Defaults to 'True'.
    na_val (str, optional): 
        The value to use for N/A or not applicable values. 
        Defaults to None.

    Returns:
    -------
    dict: A dictionary of dictionaries, each representing a column 
          with the following attributes:
        - field_name (str): Name of the column.
        - data_type (str): Data type of the column.
        - allow_null (bool): Indicates whether the column allows 
          null values.
        - null_count (int): Count of null values in the column.
        - duplicate_count (int): Count of duplicated values in 
          the column.
        - length (int or str): Maximum length of values for string 
          columns, or 'N/A' for other types.
        - range_min (float or int): Minimum value for numeric columns, 
          or 'N/A' for other types.
        - range_max (float or str): Maximum value for numeric columns, 
          or 'N/A' for other types.
        - regex_pattern (str): Regular expression pattern for the column, 
          or 'N/A' if not applicable.
        - unique_value (bool): Indicates whether the column has unique 
          values.
        - allowed_value_list (list or str): A sorted list of allowed 
          values for non-unique string columns with a manageable 
          number of unique values, or 'N/A' otherwise.
        - required (bool): Indicates whether the column is required.

    """
    data_dict = {}

    for col in df.columns:
        # get a null mask
        null_mask = df[col].isnull()

        # Identify non-null values
        non_null_mask = ~null_mask

        # default column info structure/values (null columns)
        column_info = {
            "field_name": col,
            "data_type": "Null-Unknown",
            "allow_null": true_val,
            "null_count": int(len(df)),
            "duplicate_count": 0,
            "length": na_val,
            "range_min": na_val,
            "range_max": na_val,
            "regex_pattern": na_val,  
            "unique_value": na_val,
            "allowed_value_list": na_val,
            "required": false_val
        }  

        # create column info structure/values (non-null columns)
        if not null_mask.all():
            _s = df[col][non_null_mask]
            dups = _s.duplicated(keep=False)
            has_nulls = series_hasNull(df[col])
            column_info = {
                "field_name": col,
                "data_type": infer_data_types(_s),
                "allow_null":  true_val if has_nulls else false_val,
                "null_count": int(null_mask.sum()),
                "duplicate_count": _s.duplicated(keep=False).sum(),
                "length": na_val,
                "range_min": get_numeric_range(_s, 'min', na_val),  
                "range_max": get_numeric_range(_s, 'max', na_val),
                "regex_pattern": na_val,
                "unique_value": true_val if dups.sum() == 0 else false_val,
                "allowed_value_list": na_val,
                "required": true_val
            }

            # document allowed values found           
            if pd.api.types.is_numeric_dtype(_s):
                try:
                    # try to cast the series as an int 
                    _s = _s.astype(int)   
                except:
                    pass

            if pd.api.types.is_string_dtype(_s) or \
                pd.api.types.is_categorical_dtype(_s) or \
                    pd.api.types.is_integer_dtype(_s):  
                if _s.nunique() <= max_unique_vals: 
                    if pd.api.types.is_integer_dtype(_s): 
                        column_info["allowed_value_list"] = sorted([int(x) 
                                                                    for x in _s.unique()])  
                    else:
                        column_info["allowed_value_list"] = sorted(_s.astype(str).unique())  

            # document max length of values        
            if column_info["length"] == na_val:
                try:
                    # cast the series as a string 
                    _s = _s.astype(str)
                    # get the max character length in the value
                    column_info["length"] = int(_s.str.len().max())
                except:
                    pass
  
        data_dict[col] = column_info
    return data_dict

#---------------------------------------------------------------------------------- 

def dataset_schema_to_json(file_path, 
                           out_dir=None, 
                           out_name=None,
                           na_values=Config.NA_VALUES, 
                           na_patterns=Config.NA_PATTERNS
                           ):
                           
    """Generates a data dictionary JSON string given a spreadsheet
    (CSV, XLSX, or XLS) file.

    Parameters:
    ----------
    file_path (str):
        Path to the spreadsheet (CSV, XLSX, or XLS) file.
    out_dir (str, optional):
        Path to the output directory for the JSON file.
        Defaults to None.
    out_name (str, optional):
        Desired name for the output JSON file (without extension).
        Defaults to None.
    na_values: (Optional)
        List of values to consider nulls in addition to standard nulls.
        (default: None)
    na_patterns: (Optional)
        List of regular expressions to identify strings representing missing values.
        (default: None)
    Returns:
    -------
    json_string (str):
        Formatted JSON string representing the processed data dictionary.
    """

    # Initialize the schema dictionary
    schema = {}

    # Read data from CSV or Excel file
    dataframes = read_csv_or_excel_to_df(file_path, infer=True, 
                                         multi_sheets=True,
                                         na_values=na_values, 
                                         na_patterns=na_patterns)

    # Attempt to cast data types for each dataframe
    for sheet_name, dataframe in dataframes.items():
        # Generate the schema dictionary
        schema[sheet_name] = build_data_dictionary(dataframe)
        
    # Convert any nested string literal lists, dicts, tuples into Python objects
    schema = eval_nested_string_literals(schema)

    if out_dir and out_name:
        # Convert the dictionary to a JSON object
        json_string = json.dumps(schema, indent=4, sort_keys=True, cls=Config.jsonEncoder)        
        # Ensure the correct file extension
        if not out_name.endswith('.json'):
            out_name = f'{out_name}.json'
        # Ensure the correct naming convention
        if not out_name.endswith('_data_dictionary.json'):
            out_name = f'{os.path.splitext(out_name)[0]}_data_dictionary.json'
        # Build the full output path
        output_path = os.path.join(out_dir, out_name)
        # Save the JSON text to a file
        with open(output_path, "w") as file:
            file.write(json_string)
        print(f'Data dictionary saved to: {output_path}')
    
    return schema

#---------------------------------------------------------------------------------- 

def write_dataframes_to_xlsx(dataframes,
                             out_dir,
                             out_name,
                             sheet_order=None):
    """
    Writes a dictionary of DataFrames to an xlsx file with a given sheet
    order, handling chunking for DataFrames exceeding Excel limits.

    Parameters:
    ----------
        dataframes (dict):
            A dictionary of key-value pairs where keys are sheet
            output names and values are pandas DataFrames.
        out_dir (str):
            Path to the output directory for the xlsx file.
        out_name (str):
            Desired name for the output xlsx file.
        sheet_order (list):
            A list specifying the desired order of the sheets in
            the output spreadsheet.
            Defaults to dictionary keys.

    Returns:
    -------
        output_path (str):
            Output path to the xlsx file
    """

    MAX_ROWS_EXCEL = 1048575  # Maximum rows allowed in an Excel sheet
    MAX_COLS_EXCEL = 16383  # Maximum columns allowed in an Excel sheet

    # If there's no ".xlsx" at all, append it
    if not out_name.endswith('.xlsx'):
        out_name = f'{out_name}.xlsx'
    output_path = os.path.join(out_dir, out_name)

    if not bool(sheet_order):
        sheet_order = list(dataframes.keys())

    # Create an ExcelWriter object
    writer = pd.ExcelWriter(output_path)

    # Create a tempfile as some environments don't allow file seek
    # (i.e dataBricks w/Azure Blob)
    temp_file = '/tmp/temp.xlsx'
    with pd.ExcelWriter(temp_file) as writer:
        # Iterate through the top-level keys (sheet names)
        for sheet_name in sheet_order:
            df = dataframes[sheet_name]

            # Check if splitting is needed
            if df.shape[0] > MAX_ROWS_EXCEL or df.shape[1] > MAX_COLS_EXCEL:
                chunk_size = MAX_ROWS_EXCEL
                count = 1

                for i in range(0, len(df), chunk_size):
                    chunk = df[i:i+chunk_size] 

                    # Combine the last two chunks if exceeding max rows
                    # if current_sheet_rows + len(chunk) > MAX_ROWS_EXCEL:
                    new_sheet_name = f"{count}_{sheet_name}"
                    chunk.to_excel(writer,
                                    sheet_name=new_sheet_name,
                                    index=False)
                    count += 1

            else:
                df.to_excel(writer,
                            sheet_name=sheet_name,
                            index=False)

    # Overwrite the file if it exists already
    if os.path.exists(output_path):
        os.remove(output_path)  # Remove existing file before copying
    shutil.copyfile(temp_file, output_path)
    print(f'Output saved to: {output_path}')

    try:
        os.remove(temp_file)  # Try to clean up the tempfile
    except:
        pass
    return output_path

				     
#---------------------------------------------------------------------------------- 

def dataset_schema_to_xlsx(file_path, 
                           out_dir, 
                           out_name, 
                           na_value='N/A',
                           multi_sheets=True
                           ):
                           
    """Generates a data dictionary XLSX file given a spreadsheet 
    (CSV, XLSX, or XLS) containing real data.

    Parameters:
    ----------
        file_path (str): 
            Path to the spreadsheet (CSV, XLSX, or XLS) file.
        out_dir (str, optional): 
            Path to the output directory for the JSON file. 
        out_name (str): 
            Desired name for the output JSON file (without extension). 
        na_value (str): 
            Value to use for filling missing data. 
            Defaults to 'N/A'.
        multi_sheets : bool, optional (default=True)
            If True, allows reading multiple sheets from Excel files.
            If False, raises an error if the Excel file has multiple sheets.            
    Returns:
    -------
        output_path (str):
            Output path to the XLSX file
    """

    # If there's no ".xlsx" at all, append it
    if not out_name.endswith('.xlsx'):
        out_name = f'{out_name}.xlsx'
    # If there's no "_data_dictionary" before the final ".xlsx", insert it
    if not out_name.endswith('_data_dictionary.xlsx'):
        out_name = f'{os.path.splitext(out_name)[0]}_data_dictionary.xlsx'
    output_path = os.path.join(out_dir, out_name)

    data_dictionary = {}
    dfs = read_csv_or_excel_to_df(file_path, infer=True, 
                                  multi_sheets=multi_sheets)

    for sheet_name, df in dfs.items():
        # generate the data dictionary
        data_dictionary[sheet_name] = build_data_dictionary(df)

    # Convert the data dictionary to DataFrames
    dataframes = {sheet_name: pd.DataFrame.from_dict(sheet_schema, orient='index') 
                  for sheet_name, sheet_schema in data_dictionary.items()}

    # Write the DataFrames to an XLSX file using the existing function
    write_dataframes_to_xlsx(dataframes, out_dir, out_name)

    return output_path

#---------------------------------------------------------------------------------- 

def get_dict_diffs(dict1, 
                   dict2
                   ):
    """
    Compares two dictionaries and returns a dictionary containing mismatches.

    Parameters:
    ----------
        dict1 (dict): 
            The test or control dictionary to compare against dict2.
        dict2 (dict): 
            The observed or actual values to compare against dict1.

    Returns:
    -------
        mismatches (dict):
            A dictionary containing differences between the two 
            dictionaries where the 'expected' key is the baseline 
            or test in dict1, and the 'observed' key is the value
            in dict2. Only unmatched values will be returned.

    Raises:
        TypeError: 
            If either `dict1` or `dict2` is not a dictionary.
    """
    if not isinstance(dict1, dict) or not isinstance(dict2, dict):
        raise TypeError("Both arguments must be dictionaries.")

    mismatches = {}

    for key, value in dict1.items():
        if key not in dict2.keys():
            mismatches[key] = {"expected": value, "observed": None}
        elif isinstance(value, list) and isinstance(dict2[key], list):
            try:
                # Sort both lists for accurate comparison
                sorted_value = sorted(value)
                sorted_dict2_value = sorted(dict2[key])
                if sorted_value != sorted_dict2_value:
                    mismatches[key] = {"expected": value, "observed": dict2[key]}
            except TypeError:
                # If sorting fails due to type mismatch, consider it a mismatch
                mismatches[key] = {"expected": value, "observed": dict2[key]}
        else:
            try:
                # Try to cast to ints
                value = downcast_ints(value)
                dict2[key] = downcast_ints(dict2[key])

                # Attempt casting dict2[key] to the datatype of value
                cast_dict2_value = type(value)(dict2[key])

                if cast_dict2_value != value:
                    mismatches[key] = {"expected": value, "observed": dict2[key]}
            except (ValueError, TypeError):
                # If casting fails, consider it a mismatch
                mismatches[key] = {"expected": value, "observed": dict2[key]}

    return mismatches

#---------------------------------------------------------------------------------- 

def schema_validate_column_types(attribute, 
                                 p_errors
                                ):
    """
    Checks if the observed data type matches the expected data type.

    Parameters:
    ----------
    attribute (str): 
        The name of the attribute to check.
    p_errors (dict): 
        A dictionary containing potential errors, where keys are attribute 
        names and values are dictionaries with 'expected' and 'observed' 
        values.

    Returns:
    -------
    str or None: 
        Returns the attribute name if an inequality is found, indicating 
        an error. 
        Returns None if the values match.

    Notes:
    ------
    The function uses the allowed_casting dictionary to determine if the 
    observed data type can be cast to the expected data type without loss 
    of information.
    """
    # Data type infer that pandas could misinterpret, but can be re-cast 
    # without losing data
    
    allowed_casting = {
        "String": ["String"],
        "Float": ["Float", "String"],
        "Boolean": ["Boolean", "String"],
        "Datetime": ["Datetime", "String"],
        "Integer": ["Integer", "Float", "String"],
        "Other": ["String"],
        "Null-Unknown": ["Integer", "Float", "String", "Boolean", "Datetime"]
    }

    observed_type = p_errors[attribute]['observed']
    expected_type = p_errors[attribute]['expected']

    if (expected_type != observed_type and 
        expected_type not in allowed_casting[observed_type]):
        return attribute
    return None

#---------------------------------------------------------------------------------- 

def schema_validate_column_length(attribute, 
                                  p_errors
                                  ):
    """
    Checks if the observed max string length of a column matches the expected 
    max string length.

    Parameters:
    ----------
        attribute (str): The name of the attribute to check.
        p_errors (dict): A dictionary containing potential errors, where keys 
                        are attribute names and values are dictionaries with 
                        'expected' and 'observed' values.

    Returns:
    -------
    str or None: Returns the attribute name if an inequality is found, 
                 indicating an error. Returns None if the values match.
    """

    obs_len = p_errors[attribute]['observed']
    exp_len = p_errors[attribute]['expected']

    is_obs_valid = isinstance(obs_len, (str, int, float))
    is_exp_valid = isinstance(exp_len, (str, int, float))

    if is_exp_valid and (not is_obs_valid or int(obs_len) > int(exp_len)):
        return attribute

    return None

#---------------------------------------------------------------------------------- 

def schema_validate_allow_null(attribute, 
                               p_errors
                              ):
    """
    Checks if null values are allowed for a given attribute.
    Parameters
    ----------
    attribute (str):
        The name of the attribute to check.
    p_errors (dict):
        A dictionary containing potential errors, where keys are attribute names
        and values are dictionaries with 'expected' and 'observed' values.
    Returns
    -------
    str or None:
        Returns the attribute name if a null value is not allowed, indicating an error.
        Returns None if null values are permitted.
    """
    if not p_errors[attribute]['expected'] and p_errors[attribute]['observed']:
        return attribute
    return None

#---------------------------------------------------------------------------------- 

def schema_validate_unique(attribute, 
                           p_errors
                           ):
    """
    Checks if column values are supposed to be unique.
    
    Parameters
    ----------
    attribute (str):
        The name of the attribute to check.
    p_errors (dict):
        A dictionary containing potential errors, where keys are attribute names
        and values are dictionaries with 'expected' and 'observed' values.
    Returns
    -------
    str or None:
        Returns the attribute name if unique values are expected but not observed, indicating an error.
        Returns None if unique values are expected and observed, or if unique values are not expected.
    """

    if p_errors[attribute]['expected'] and not p_errors[attribute]['observed']:
        return attribute
    return None

#---------------------------------------------------------------------------------- 

def schema_validate_range(attribute, 
                          p_errors,
                          msg_vals
                          ):
    """
    Checks if a numeric value for a given attribute falls within the expected range.

    Parameters:
    ----------
    attribute (str):
        The name of the attribute to check.
    p_errors (dict):
        A dictionary containing potential errors, where keys are attribute names
        and values are dictionaries with 'expected' and 'observed' values.
    msg_vals (dict):
        A dictionary to store values for error message formatting.

    Returns:
    -------
    str or None:
        Returns the attribute name if the value is outside the expected range,
        indicating an error. Returns None if the value is within the range.
    """

    # Check if the expected range is a numeric value
    if is_numeric_type(p_errors[attribute]['expected']):
        # Check if the observed value is also a numeric value
        if is_numeric_type(p_errors[attribute]['observed']):
            exp_val = p_errors[attribute]['expected']
            obs_val = p_errors[attribute]['observed']

            # Logic to determine when errors are flagged based on the attribute
            rng_logic = {
                'length': lambda expected, observed: expected < observed,
                'range_max': lambda expected, observed: expected < observed,
                'range_min': lambda expected, observed: expected > observed,
            }

            # Check if the observed value falls outside the expected range
            if rng_logic[attribute](exp_val, obs_val):
                # Store values for error message formatting
                msg_vals["expected"] = int(exp_val) if int(exp_val) == exp_val else exp_val
                msg_vals["observed"] = int(obs_val) if int(obs_val) == obs_val else obs_val
                return attribute
            else:
                # Update status and errors in case of data type mismatch
                p_errors[attribute]['status'] = 'Fail'
                p_errors[attribute]['errors'] = (
                    f'Data Type Error: Unable to validate {attribute}, check data types'
                )

    return None

#---------------------------------------------------------------------------------- 

def schema_validate_allowed_values(attribute, 
                                   p_errors,
                                   msg_vals
                                   ):
    """
    Checks if the observed values for a given attribute are within the allowed list.

    Parameters
    ----------
    attribute (str):
        The name of the attribute to check.
    p_errors (dict):
        A dictionary containing potential errors, where keys are attribute names
        and values are dictionaries with 'expected' and 'observed' values.
    msg_vals (dict):
        A dictionary to store values for error message formatting.

    Returns
    -------
    str or None:
        Returns the attribute name if there are values outside the allowed list,
        indicating an error. Returns None if all values are within the allowed list.
    """

    # Check if the expected and observed values are lists
    if isinstance(p_errors[attribute]['expected'], list) and isinstance(
            p_errors[attribute]['observed'], list):
        
        # Create sets for faster membership testing
        allowed_vals = set(map(str, p_errors[attribute]['expected']))
        observed_vals = set(map(str, p_errors[attribute]['observed']))

        # Check if all observed values are within the allowed list
        if not observed_vals.issubset(allowed_vals):
            # Identify values outside the allowed list
            err_vals = list(observed_vals - allowed_vals)

            # Regular expression for integers
            pattern = r"^-?\d+$"  # Matches integers only (no decimals)

            # Filter values matching the pattern
            int_vals = [int(v) for v in err_vals if re.match(pattern, str(v))]
            if len(int_vals) == len(err_vals):
                err_vals = int_vals

            # Store error values for error message formatting
            msg_vals['err_vals'] = err_vals
            return attribute

    return None

#---------------------------------------------------------------------------------- 

def schema_validate_attribute(attribute,
                              p_errors,
                              col,
                              msg_vals
                              ):
    """
    Validates specific schema attributes and returns the error type if applicable.

    Parameters:
    ----------
    attribute (str):
        The name of the attribute to validate.
    p_errors (dict):
        A dictionary containing potential errors, where keys are attribute names
        and values are dictionaries with 'expected' and 'observed' values.
    col (str):
        The name of the column being validated.
    msg_vals (dict):
        A dictionary to store values for error message formatting.

    Returns:
    -------
    str or None:
        Returns the error type if a violation is found for the attribute.
        Returns None if no errors are detected for the attribute.
    """
    # Attributes to test if expected numeric value is within a range    
    range_checks = ['length', 'range_max', 'range_min']

    if attribute == 'data_type':
        # Validate data type
        return schema_validate_column_types(attribute, p_errors)        
    elif attribute == 'allow_null':
        # Validate if null values are allowed
        return schema_validate_allow_null(attribute, p_errors)
    elif attribute == 'length':
        # Validate maximum string length
        return schema_validate_column_length(attribute, p_errors)
    elif attribute == 'unique_value':
        # Validate if column values are supposed to be unique
        return schema_validate_unique(attribute, p_errors)
    elif attribute == 'allowed_value_list':
        # Validate if observed values are within the allowed list
        return schema_validate_allowed_values(attribute, p_errors, msg_vals)        
    elif attribute in range_checks:
        # Validate if a numeric value falls within the expected range
        return schema_validate_range(attribute, p_errors, msg_vals)

    return None  # No error found for this attribute

#---------------------------------------------------------------------------------- 

def validate_schema(observed_schema,
                    data_dictionary,
                    schema_mapping
                    ):
    """
    Validates observed datasets against a data dictionary and returns 
    schema violations.

    Parameters:
    ----------
    observed_schema : dict
        The observed schema as a dictionary.
    data_dictionary : dict
        The data dictionary as a dictionary.
    schema_mapping : List[Dict]
        A list of mappings between observed datasets and corresponding 
        data dictionary sections.

    Returns:
    -------
    schema_violations : Dict
        A dictionary containing schema violations, where keys are dataset 
        names and values are dictionaries
        with flagged columns and their errors.
    """
    # Create a dict to hold schema violations
    schema_violations = {}
    _SET = Config.SCHEMA_ERROR_TEMPLATES

    # clean up the schema_mapping dict to remove references which have not data dict defined 
    # this modifies the original dictionary supplied 
    clean_mapping = schema_mapping[:]  # Create a copy
    for mapping in clean_mapping:
        data_dict_section = mapping['data_dict']
        if not data_dictionary.get(data_dict_section):
            schema_mapping.remove(mapping)
            print(f'''Warning: Authoritative schema not found for "{data_dict_section}". 
            Please check schema_mapping and update the data dictionary if needed.''')

    # Iterate over the schema_mapping object to test datasets against 
    # the given data dictionary			    
    for mapping in schema_mapping:
        observed_dataset = mapping['dataset']
        data_dict_section = mapping['data_dict']

        # Get the authoritative schema
        auth_schema = data_dictionary.get(data_dict_section)

        if not auth_schema:
            raise ValueError(f'''Authoritative schema "{data_dict_section}" not found. 
            Please check schema_mapping and update the data dictionary as needed!''')

        # Initialize the results dict
        v_results = {}

        # Iterate over columns and properties in auth_schema
        for col, col_props in auth_schema.items():
            errors = {}
            msg_vals = {"col": col}

            # Flag potential issues initially by checking expected vs observed
            if col in observed_schema[observed_dataset].keys():
                obs_vals = observed_schema[observed_dataset][col]
                p_errors = get_dict_diffs(col_props, obs_vals)

                for atttr in p_errors:
                    error_type = None
                    msg_vals["expected"] = p_errors[atttr]['expected']
                    msg_vals["observed"] = p_errors[atttr]['observed']

                    error_type = schema_validate_attribute(atttr, 
                                                           p_errors, 
                                                           col, 
                                                           msg_vals)
                    if error_type:
                        errors[atttr] = p_errors[atttr]
                        if error_type == 'allow_null':
                            null_count = obs_vals.get('null_count')
                            if null_count:
                                msg_vals["count"] = null_count
                        if error_type == 'unique_value':
                            dup_count = obs_vals.get('duplicate_count')
                            if dup_count:
                                msg_vals["count"] = dup_count

                        errors[atttr]['errors'] = _SET[atttr].format(**msg_vals)

            elif col_props['required']:
                # Missing required column
                errors = {"required_column": {
                    "expected": True,
                    "observed": False,
                    "errors": _SET['required_column'].format(**msg_vals)}
                }
            elif not col_props['required']:
                # Missing optional column
                errors = {"optional_column": {
                    "expected": True,
                    "observed": False,
                    "errors": _SET['optional_column'].format(**msg_vals)}
                }

            if bool(errors):
                v_results[col] = {'status': 'fail', 
                                  'required': col_props['required']
                                 } | errors

        schema_violations[observed_dataset] = {'schema_violations': v_results}

    return schema_violations

#---------------------------------------------------------------------------------- 

def value_errors_nulls(df,
                       column_name,
                       unique_column=None
                       ):
    """
    Identifies null values in a DataFrame column and returns either
    their row indices or unique values.

    Parameters:
    ----------
        df : pd.DataFrame
            The DataFrame to check.
        column_name : str
            The name of the column to check for null values.
        unique_column : str, optional
            The name of the column containing unique values.

    Returns:
    -------
        list of dict:
            A list of dictionaries, each containing 'Sheet Row', 'Error Type',
            'Column Name', and the unique column value (if provided).
    """
    null_mask = df[column_name].isnull()  # Create a boolean mask of null values

    results = []
    for row_index, row in df[null_mask].iterrows():
        output_dict = {
            'Sheet Row': row_index + 2,
            'Error Type': 'Null Value',
            'Column Name': column_name,
            'Error Value': row[column_name],
        }
        if unique_column and unique_column in df.columns:
            output_dict["Lookup Column"] = unique_column
            output_dict["Lookup Value"] = row[unique_column]
        results.append(output_dict)
    return results

#---------------------------------------------------------------------------------- 
def value_errors_duplicates(df,
                            column_name,
                            unique_column=None
                            ):
    """
    Identifies duplicate values in a DataFrame column and returns their
    row indices, unique values (if provided), and the actual values from
    the column, along with error type and column name.

    Parameters:
    ----------
    df : pd.DataFrame
        The DataFrame to check.
    column_name : str
        The name of the column to check for duplicates.
    unique_column : str, optional
        The name of the column containing unique values.

    Returns:
    -------
    list of dict:
        A list of dictionaries, each containing 'Sheet Row', 'Error Type',
        'Column Name', the unique column value (if provided), and the actual
        value from the 'column_name'.
    """
    # Create a boolean mask of duplicates
    null_mask = df[column_name].isnull()
    duplicate_mask = df[column_name].duplicated(keep=False) & ~null_mask

    results = []
    for row_index, row in df[duplicate_mask].iterrows():
        output_dict = {
            'Sheet Row': row_index + 2,
            'Error Type': 'Duplicate Value',
            'Column Name': column_name,
            'Error Value': row[column_name],
        }
        if unique_column and unique_column in df.columns:
            output_dict["Lookup Column"] = unique_column
            output_dict["Lookup Value"] = row[unique_column]
        results.append(output_dict)

    return results

#---------------------------------------------------------------------------------- 
def value_errors_unallowed(df,
                           column_name, 
                           allowed_values,
                           unique_column=None
                           ):
    """
    Identifies values in a DataFrame column that are not in a given list 
    of allowed values, considering data types for accurate comparison. 
    Optionally returns a unique value.

    Parameters:
    ----------
    df : pd.DataFrame
        The DataFrame to check.
    column_name : str
        The name of the column to check.
    allowed_values : list
        The list of allowed values.
    unique_column : str, optional
        The name of the column containing unique values.

    Returns:
    -------
    list of dict:
        A list of dictionaries, each containing 'Sheet Row', 
        'Error Type', 'Column Name', the unique column value 
        (if provided), and the actual value from the 'column_name'.
    """
    # Get the DataFrame column's data type
    column_dtype = df[column_name].dtype
    # Ensure the same data type for comparison
    allowed_values = pd.Series(allowed_values).astype(column_dtype)
    # Create a boolean mask
    null_mask = df[column_name].isnull()
    not_allowed_mask = ~df[column_name].isin(allowed_values) & ~null_mask

    results = []
    for row_index, row in df[not_allowed_mask].iterrows():
        output_dict = {
            'Sheet Row': row_index + 2,
            'Error Type': 'Unallowed Value',
            'Column Name': column_name,
            'Error Value': row[column_name],
        }
        if unique_column and unique_column in df.columns:
            output_dict["Lookup Column"] = unique_column
            output_dict["Lookup Value"] = row[unique_column]
        results.append(output_dict)

    return results

#---------------------------------------------------------------------------------- 
def value_errors_length(df,
                        column_name,
                        max_length,
                        unique_column=None
                        ):
    """
    Identifies values in a DataFrame column that exceed a specified maximum length,
    handling any data type by converting values to strings. Returns no results
    if all values can be converted to strings within the limit.

    Parameters:
    ----------
    df : pd.DataFrame
        The DataFrame to check.
    column_name : str
        The name of the column to check.
    max_length : int
        The maximum allowed length for values.
    unique_column : str, optional
        The name of the column containing unique values.

    Returns:
    -------
    list of dict:
        A list of dictionaries, each containing 'Sheet Row', 
        'Error Type', 'Column Name', the unique column value 
        (if provided), and the actual value from the 'column_name'.
        Returns an empty list if all values can be converted to 
        strings within the limit.
    """
    try:
        # Attempt to convert all values to strings
        #str_values = df[column_name].fillna('').astype(str)
        str_values = df[column_name].astype(str, errors='ignore').fillna('')
    except ValueError:
        return []  # Conversion failed, handle exceeding values

    exceeding_mask = str_values.str.len() > max_length

    results = []
    for row_index, row in df[exceeding_mask].iterrows():
        output_dict = {
            'Sheet Row': row_index + 2,
            'Error Type': f'Value Exceeds Max Length ({max_length})',
            'Column Name': column_name,
            'Error Value': row[column_name],
        }
        if unique_column and unique_column in df.columns:
            output_dict["Lookup Column"] = unique_column
            output_dict["Lookup Value"] = row[unique_column]
        results.append(output_dict)

    return results

#---------------------------------------------------------------------------------- 
def value_errors_out_of_range(df,
                              column_name,
                              test_type, 
                              value,
                              unique_column=None
                              ):
    """
    Identifies values in a DataFrame column that fall outside a specified range
    (either below a minimum or above a maximum value).

    Parameters:
    ----------
    df : pd.DataFrame
        The DataFrame to check.
    column_name : str
        The name of the column to check.
    test_type : str
        'min' or 'max' indicating the type of test to perform.
    value : int or float
        The minimum or maximum allowed value, depending on the test_type.
    unique_column : str, optional
        The name of the column containing unique values.

    Returns:
    -------
    list of dict:
        A list of dictionaries, each containing 'Sheet Row', 
        'Error Type', 'Column Name', the unique column value 
        (if provided), and the actual value from the 'column_name'.
    """

    results = []

    # Check for integers or float
    numeric_column = df[column_name].replace(r'^\s+$', pd.NA, regex=True)
    numeric_column.fillna(pd.NA)  
    try:
        numeric_column = pd.to_numeric(numeric_column)
        numeric_column.fillna(pd.NA) 
    except:
        pass

    if test_type not in ("min", "max"):
        raise ValueError("test_type must be either 'min' or 'max")

    if pd.api.types.is_numeric_dtype(numeric_column):
        error_type = None
        if test_type == "min":
            mask = numeric_column < value
            error_type = f"Below Minimum Allowed Value ({value})"
        elif test_type == "max":
            mask = numeric_column > value
            error_type = f"Exceeds Maximum Allowed Value ({value})"

        for row_index, row in df[mask].iterrows():
            # Check if the row falls outside the range (mask is True)
            if mask[row_index]:
                output_dict = {
                    "Sheet Row": row_index + 2,
                    "Error Type": error_type,
                    "Column Name": column_name,
                    "Error Value": row[column_name],
                }
                if unique_column and unique_column in df.columns:
                    output_dict["Lookup Column"] = unique_column
                    output_dict["Lookup Value"] = row[unique_column]
                results.append(output_dict)
    return results

#---------------------------------------------------------------------------------- 
def value_errors_regex_mismatches(df,
                                  column_name,
                                  regex_pattern,
                                  unique_column=None
                                  ):
    """
    Identifies values in a DataFrame column that do not match a 
    specified regex pattern, ignoring null values.

    Parameters:
    ----------
    df : pd.DataFrame
        The DataFrame to check.
    column_name : str
        The name of the column to check.
    regex_pattern : str
        The regular expression pattern to check against.
    unique_column : str, optional
        The name of the column containing unique values.

    Returns:
    -------
    list of dict
        A list of dictionaries, each containing 'Sheet Row', 
        'Error Type', 'Column Name', the unique column value 
        (if provided), and the actual value from the 'column_name'.
    """
    # Identify non-null values
    non_null_mask = df[column_name].notnull()  
    pattern_match = df.loc[non_null_mask, 
                           column_name].astype(str).str.match(regex_pattern)
    # Invert to get mismatches
    mismatch_mask = ~pattern_match  

    results = []
    # Filter for mismatches
    for row_index, row in df.loc[non_null_mask & mismatch_mask].iterrows():  
        output_dict = {
            'Sheet Row': row_index + 2,
            'Error Type': 'Invalid Value Formatting',
            'Column Name': column_name,
            'Error Value': row[column_name]
        }
        if unique_column and unique_column in df.columns:
            output_dict["Lookup Column"] = unique_column
            output_dict["Lookup Value"] = row[unique_column]
        results.append(output_dict)

    return results

#---------------------------------------------------------------------------------- 
def get_value_errors(dataset_path,
                     schema_errors,
                     data_dict,
                     schema_mapping,
                     ignore_errors=['allow_null']
                     ):
    """
    Identifies value errors within a dataset based on the results of 
    the schema validation JSON object, the data dictionary JSON object, 
    and schema mapping (if required) applied against a target spreadsheet. 

    Parameters:
    ----------
    dataset_path : str
        The path to the dataset file (CSV or Excel) to be validated.
    schema_errors: dict
        The result of validate_schema().
    data_dict : dict str
        The result of data_dict_to_json().
    schema_mapping : list of dicts
        A list of mappings between dataset names and corresponding 
        data dictionary sections.
    ignore_errors : list, optional
        A list of error types to exclude from the analysis. 
        Default is ['allow_null'].        

    Returns:
    -------
        list: A list of dictionaries, each containing 'sheet_row', 
              'Error Type', 'Column Name', and the unique column value 
              (if provided).
    """
    # read the dataframes in as sheets if needed
    dfs = read_csv_or_excel_to_df(dataset_path, 
                                  infer=True, 
                                  multi_sheets=True)

    # get the run uuid/file hashing
    uid = list(schema_errors.keys())[0]

    value_errors = {}
    for mapping in schema_mapping:
        observed_ds = mapping['dataset']
        data_dict_section = mapping['data_dict']
        auth_schema = data_dict.get(data_dict_section)
        
        if not auth_schema:
            _e = f'''Authoritative schema "{data_dict_section}" not found in keys, 
            please check schema_mapping!'''
            print(_e)
            return {data_dict_section: {'schema_mapping': schema_mapping, 'Errors': _e}}

        sheet_results = schema_errors[uid]["results"][observed_ds]
        sheet_v_errors = []

        df = dfs[observed_ds]
        schema_violations = sheet_results.get("schema_violations")

        unique_cols = [k for k in auth_schema.keys() 
                       if auth_schema[k]['unique_value']]
        unique_column = unique_cols[0] if unique_cols else None
        unique_column = get_best_uid_column(df, preferred_column=unique_column)

        if schema_violations:
            for col, errors in schema_violations.items():
                flagged_errs = list(errors.keys())
                if 'allow_null' in flagged_errs \
                    and 'allow_null' not in ignore_errors:
                    sheet_v_errors.append(
                        value_errors_nulls(df, col, 
                                           unique_column=unique_column)
                    )
                if 'unique_value' in flagged_errs \
                    and 'unique_value' not in ignore_errors:
                    sheet_v_errors.append(
                        value_errors_duplicates(df, col, 
                                                unique_column=unique_column)
                    )
                if 'length' in flagged_errs \
                    and 'length' not in ignore_errors:
                    max_len = errors['length']['expected']
                    sheet_v_errors.append(
                        value_errors_length(df, col, 
                                            max_length=max_len, 
                                            unique_column=unique_column)
                    )
                if 'range_max' in flagged_errs \
                    and 'range_max' not in ignore_errors:
                    rng_max = errors['range_max']['expected']
                    sheet_v_errors.append(
                        value_errors_out_of_range(df, col, 
                                                  test_type='max', 
                                                  value=rng_max, 
                                                  unique_column=unique_column)
                    )
                if 'range_min' in flagged_errs \
                    and 'range_min' not in ignore_errors:
                    rng_min = errors['range_min']['expected']
                    sheet_v_errors.append(
                        value_errors_out_of_range(df, col, 
                                                  test_type='min', 
                                                  value=rng_min, 
                                                  unique_column=unique_column)
                    )
                if 'allowed_value_list' in flagged_errs \
                    and 'allowed_value_list' not in ignore_errors:
                    allowed_vals = errors['allowed_value_list']['expected']
                    sheet_v_errors.append(
                        value_errors_unallowed(df, col, 
                                               allowed_values=allowed_vals, 
                                               unique_column=unique_column)
                    )
        
        if 'regex_pattern' not in ignore_errors:
            for col in df.columns:
                if auth_schema.get(col):
                    ptrn = auth_schema[col].get('regex_pattern')    
                    if isinstance(ptrn, str) and ptrn not in Config.NA_VALUES:
                        sheet_v_errors.append(
                            value_errors_regex_mismatches(df, col, 
                                                          regex_pattern=ptrn, 
                                                          unique_column=unique_column)
                        )

        merged_errors_list = []
        if bool(sheet_v_errors):
            if len(sheet_v_errors) > 0:
                merged_errors_list = [i for e in sheet_v_errors for i in e]
            else:
                merged_errors_list = sheet_v_errors
            merged_errors_list = json.loads(pd.DataFrame(merged_errors_list).to_json())
        value_errors[observed_ds] = merged_errors_list

    return {uid: value_errors}

#---------------------------------------------------------------------------------- 
def validate_dataset(dataset_path,
                     data_dict_path,
                     schema_mapping, 
                     list_errors=True,
                     out_dir=None,
                     out_name=None,
                     na_values=Config.NA_VALUES,
                     na_patterns=Config.NA_PATTERNS,
                     ignore_errors=['allow_null']
                     ): 
    """
    Validates a dataset against a data dictionary, performing both 
    schema and regex validation.
    
    Parameters
    ----------
    dataset_path : str
        The path to the dataset file (CSV or Excel) to be validated.
    data_dict_path : str
        The path to the data dictionary file (CSV or Excel) containing 
        schema and regex patterns.
    schema_mapping : list
        A list of mappings between dataset names and corresponding 
        data dictionary sections.
    list_errors : bool, optional
        Option to list all row.value level errors in a sheet/tab named
        in reference to the original dataset. 
        Defaults to True.
    out_dir : str, optional
        Path to the output directory for the JSON file. 
        Defaults to None.
    out_name : str, optional
        Desired name for the output JSON file (without extension). 
        Defaults to None.
    ignore_errors : list, optional
        A list of error types to exclude from the analysis. 
        Default is ['allow_null'].               
    Returns
    -------
    dict
        A dictionary containing the validation results.
    """
    # gather metadata 
    #----------------
    cur_ts = datetime.utcnow().isoformat()
    dataset_meta = get_spreadsheet_metadata(dataset_path) 
    data_dict_meta = get_spreadsheet_metadata(data_dict_path)

    data_dict_meta = {key: value for key, value in 
                data_dict_meta[list(data_dict_meta.keys())[0]].items() 
                if key in { "created", "file_md5_hash",
                            "file_name", "file_path",
                            "file_type", "modified"}}

    # generate the schema dictionaries to compare 
    #----------------
    data_dict = data_dict_to_json(data_dict_path, 
                                  na_values=na_values, 
                                  na_patterns=na_patterns)
    obs_schema = dataset_schema_to_json(dataset_path, 
                                        na_values=na_values, 
                                        na_patterns=na_patterns)

    # validate the observed json schema schema against the data dictionary
    #----------------
    schema_errors = validate_schema(observed_schema=obs_schema,
                                    data_dictionary=data_dict, 
                                    schema_mapping=schema_mapping)
    
    # build the output metadata 
    #---------------
    results = {}
    uid = None
    for k,v in dataset_meta.items():
        uid = f"{v['file_md5_hash']}_{data_dict_meta['file_md5_hash']}"
        results[uid] = {'run_metadata':{'start_time':cur_ts,
                                        'schema_mapping': schema_mapping},
                       'dataset_metadata':dataset_meta[k],
                       'data_dict_metadata':data_dict_meta,
                       'results': schema_errors
                      }
        
    # identify individual values errors
    #---------------
    if list_errors:
        value_errors = get_value_errors(dataset_path=dataset_path, 
                                        schema_errors=results, 
                                        data_dict=data_dict, 
                                        schema_mapping=schema_mapping,
                                        ignore_errors=ignore_errors)

        if value_errors:
            for sheet, errs in value_errors[uid].items():
                results[uid]["results"][sheet]["value_errors"]=errs
    # convert the dictionary to a formatted JSON string & output the results
    #---------------   

    if bool(out_dir) and bool(out_name):
        json_string = json.dumps(results, indent=4, sort_keys=True, cls=Config.jsonEncoder)
        output_path = os.path.join(out_dir, f'{out_name}_({uid}).json')
        # save the JSON text to a file
        with open(output_path, "w") as f:
            f.write(json_string)
        print(f'Data saved to: {output_path}')

    return results

#---------------------------------------------------------------------------------- 
def schema_validation_to_xlsx(validation_results, 
                              out_dir, 
                              out_name=None
                              ):
    """
    Writes a the dictionary results of validate_dataset to a spreadsheet
    report (.xlsx) detailing the metadata, error overview, and individual
    value errors (if included). 

    Parameters:
    ----------
        validation_results (dict):
            A dictionary of key-value pairs that are the results 
            of validate_dataset()
        out_dir (str): 
            Path to the output directory for the output xlsx file. 
        out_name (str, optional): 
            Desired name for the output xlsx file.  
            Defaults the the UUID/file hash ID string in 
            the validation_results.
    Returns:
    -------
        output_path (str):
            Output path to the xlsx file
    """
    uid = list(validation_results.keys())[0]

    # get a dataframe of metadata
    #----------------------------
    metadict = {key: value for key, value in validation_results[uid].items() 
                if key in {'run_metadata' ,'dataset_metadata', 'data_dict_metadata'}}

    metadata_df = pd.DataFrame([
                        {'Item': k, 'Attribute': k2, 'Value': v2}
                        for k, v in metadict.items() for k2, v2 in v.items()
                    ])

    rpt_sheets = {'Metadata':metadata_df}
    sheet_order = ['Metadata']

    # get a list of datasets/sheets
    datasets = list(validation_results[uid]['results'].keys())

    # get a dataframe of high level schema errors
    #----------------------------
    error_ov = []
    for ds in datasets:
        s_errs = validation_results[uid]['results'][ds].get('schema_violations')
        if not s_errs:
            continue
        for col, err_info in s_errs.items():
            if err_info['status'] == 'fail':
                req = err_info['required']
                col_errs = s_errs.get(col)
                if not bool(col_errs): 
                    continue
                for k, vals in col_errs.items():
                    if k not in ['status', 'required']:       
                        error_ov.append({
                                        'Dataset': str(ds),
                                        'Column': str(col), 
                                        'Status': str(err_info['status']).title(), 
                                        'Required': str(req).title(), 
                                        'Error Type': str(k), 
                                        'Error': str(vals['errors'])
                                        })
    if bool(error_ov):                    
        errors_ov_df = pd.DataFrame(error_ov)
    else:
        # use a blank sheet
        errors_ov_df = pd.DataFrame(columns=['Dataset', 'Column', 'Status', 
                                            'Required', 'Error Type', 'Error']) 

    rpt_sheets['Errors Overview'] = errors_ov_df
    sheet_order.append('Errors Overview')
    # get dataframes for each dataset/sheet of value errors
    #----------------------------
    value_errors = {}
    for ds in datasets:
        ve = validation_results[uid]['results'][ds].get('value_errors')
        if bool(ve): 
            val_errs_df = pd.DataFrame(ve)
            try:
                val_errs_df = val_errs_df.sort_values(by='Sheet Row',
                                                    ascending=True)
                value_errors[ds] = val_errs_df
            except:
                print(ve.keys())
                print(val_errs_df.columns)
    if bool(value_errors): 
        rpt_sheets = {**rpt_sheets, **value_errors}
        sheet_order.extend(list(value_errors.keys()))

    if not out_name:
        out_name = f"{uid}.xlsx"
    else:
        out_name = f"{out_name}_({uid}).xlsx"
    out_file = write_dataframes_to_xlsx( dataframes=rpt_sheets, 
                                            sheet_order=sheet_order, 
                                            out_dir=out_dir, 
                                            out_name=out_name)
    return(out_file)

#---------------------------------------------------------------------------------- 
