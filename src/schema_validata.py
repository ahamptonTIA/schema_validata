import os, shutil, json, ast, math, hashlib, re, warnings
from datetime import datetime
import pandas as pd
import numpy as np

# import openpyxl

#---------------------------------------------------------------------------------- 
# warnings to ignore
warnings.simplefilter("ignore", UserWarning)
try:
    # Ignore panda DtypeWarnigs fo low memory option (can't avoid in unknown schemas)
    warnings.filterwarnings('ignore', message="^Columns.*")
except:
    pass
try:
    # Ignore future warning on silent downcasting (code assumes new method)
    pd.set_option('future.no_silent_downcasting', True)
except:
    pass
#---------------------------------------------------------------------------------- 
# Constants

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

DATA_DICT_PRIMARY_KEY = "field_name"

SCHEMA_ERROR_MESSAGES = {
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

#----------------------------------------------------------------------------------

def get_byte_units(size_bytes):
    """Function converts bytes into the largest 
    possible unit of measure 
    Parameters
    ----------
    size_bytes: int
        numeric of bytes
    Returns
    ----------
    str :
        String representing the value and largest unit size
        Ex. '200 : GB'
    """
    if size_bytes == 0:
        return '0 : B'
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.floor(math.log(size_bytes, 1000))) #1024
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f'{s} : {size_name[i]}'

#----------------------------------------------------------------------------------

def get_md5_hash(file):
    """Function reads a file to generate an md5 hash.
    See hashlib.md5() for full documentation.
    Parameters
    ----------
    file : str
        String file path
    Returns
    ----------
    str :
        String md5 hash string
    """    
    with open(file, "rb") as f:
        f_hash = hashlib.md5()
        while chunk := f.read(8192):
            f_hash.update(chunk)
    return f_hash.hexdigest() 

#----------------------------------------------------------------------------------

def get_spreadsheet_metadata(file_path):
    """Function returns a dictionary that details
    general metadata for a csv file
    Parameters
    ----------
    dbutils: dbutils object
        DataBricks notebook dbutils object
    file_path : str
        DataBricks file storage path to a csv
    Returns
    ----------
    file_meta: dict
        dictionary of file metadata
    """
    
    # Extract filename and extension

    filename = os.path.basename(file_path)
    base_name, ext = os.path.splitext(filename)

    # get date time file metadata
    statinfo = os.stat(file_path)
    create_date = datetime.fromtimestamp(statinfo.st_ctime).isoformat()
    modified_date = datetime.fromtimestamp(statinfo.st_mtime).isoformat()

    # create dictionary to store the metadata
    file_meta = {}    

    # read the data into a pandas dataframe by sheet
    dfs = read_csv_or_excel_to_df(file_path, multi_sheets=True)

    _hash = get_md5_hash(file_path)
    for sheet_name, df in dfs.items():
        meta = {
                    'file_path': file_path,  
                    'file_name': filename,
                    'file_type': ext,
                    'file_size_bytes': f'{statinfo.st_size:,}',
                    'file_size_memory_unit': get_byte_units(int(statinfo.st_size)),
                    'record_qty': f'{len(df):,}',   
                    'column_qty': f'{len(df.columns):,}',   
                    'file_md5_hash': _hash,
                    'created' : create_date,
                    'modified' : modified_date
                } 
 
        # generate the schema dictionary
        file_meta[sheet_name] = meta
    return file_meta 

#---------------------------------------------------------------------------------- 

def downcast_ints(value):
    """
    Downcast a numeric value to an integer if it is equal to 
    a float representation.
    Parameters
    ----------
        value: The numeric value to downcast.
    Returns
    ----------
        The value as an integer if it is equal to its float 
        representation, otherwise the original value.
    """
    try:
        if not isinstance(value, (bool, pd.BooleanDtype))  and value == float(value):
            if int(value)==float(value):
                value = int(value) 
    except:
        pass
    return value

#---------------------------------------------------------------------------------- 

def get_best_uid_column(df, prefer_col=None):
    """
    Identifies the column with the most unique values (excluding nulls) in a DataFrame.
    Parameters:
    ----------
    schema_errors: dict
        df: pandas.DataFrame
            The input DataFrame.
        prefer_col : str, optional
            The preferred column if ties for uniqueness occur.
    Returns:
    -------
        The column name with the most unique values, or None if none qualify.
    Raises:
    -------
        ValueError: If `df` is not a pandas DataFrame.
    """
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
        uid_cols = [c for c, uc in uniq_cnts.items() if uc > 0 and uc == max_value]
    else:
        return prefer_col
    # Prioritize prefer_col in case of ties
    if len(uid_cols) > 1 and prefer_col:
        uid_cols = [c for c in uid_cols
                    if uniq_cnts[c] > uniq_cnts[prefer_col]]
        if len(uid_cols) == 0:
            return prefer_col
    # return the first column if tied
    return uid_cols[0]

#---------------------------------------------------------------------------------- 

def eval_nested_string_literals(data):
    """
    Iterates through a nested dictionary or JSON object, attempting to evaluate
    string representations of data types (e.g., lists, dict, tuples) into their 
    actual Python counterparts. Modifies the structure in-place, replacing string
    representations with evaluated values.

    Parameters:
    ----------
    data : dict or str
        The nested dictionary to iterate through, or a JSON string to be parsed.
    Returns:
    -------
    dict : dict
        The modified dictionary with string representations replaced by evaluated
        values.
    """   
    if isinstance(data, str):
        data = json.loads(data)  # Parse JSON string if needed
    for k, v in data.items():
        if isinstance(v, dict):
            eval_nested_string_literals(v)  # Recursive call for nested dictionaries
        else:
            try:
                x = ast.literal_eval(v)
                if x:  # Replace only if evaluation is successful
                    data[k] = x  # Replace the value in the dictionary
            except:
                pass  # Ignore evaluation errors
    return data

#---------------------------------------------------------------------------------- 

def df_to_pandas_chunks(df, chunk_size=100000, keys=[]):
    """
    Generator that sorts and then chunks a PySpark 
    or pandas DataFrame into DataFrames of the given
    chunk size.

    Parameters
    ----------
    df : pd.DataFrame or pyspark.sql.DataFrame
        The dataframe to sort and chunk.
    chunk_size: int
        The max size of each chunk
    keys: str or list
        Column name or list of column names to sort 
        a dataframe on before chunking.
        Default, None - Sorting will not be applied
    Returns
    -------
    generator : A generator that yields chunks of pandas DataFrames.
    """      
    # if a key was supplied, sort the dataframe
    if bool(keys):
        if not isinstance(keys, list):
            keys = [keys]
            
    # sort and yield chunked pandas dataframes from pyspark
    if not isinstance(df, pd.DataFrame):
        df = df.orderBy(keys)
        for i in range(0, df.count(), chunk_size):
            chunk = df.toPandas()[i:i + chunk_size]
            yield chunk
    else:
        # sort and yield chunked pandas dataframes 
        df = df.sort_values(by=keys)
        for i in range(0, len(df), chunk_size):
            chunk = df[i:i + chunk_size]
            yield chunk

#---------------------------------------------------------------------------------- 

def remove_pd_df_newlines(df, replace_char=''):
    """Removes newline characters ('\n') from all string 
    columns in a pandas DataFrame with the given replace 
    character.

    Parameters:
    ----------
    df : pandas.DataFrame
        The DataFrame to process.
    replace_char  : str, optional
        String value to replace newline character with.
        Defaults to single space (' ') .
    Returns:
    -------
    df : pandas.DataFrame
        The DataFrame with newlines removed from string columns.
    """

    df = df.replace('\n',replace_char, regex=True)
    return df

#---------------------------------------------------------------------------------- 

def infer_df_optimal_dtypes(df):
    """
    Try to infer the optimal data types for each column in a pandas DataFrame
    by analyzing the non-null values.

    Parameters:
    ----------
    df2 (pandas.DataFrame):
        The DataFrame to analyze.
    Returns:
    -------
    pandas.DataFrame:
        A new DataFrame with inferred data types.
    """

    # Replace any whitespace with nulls using regex
    df2 = df.copy()
    df2 = df2.replace(r'\s+', pd.NA, regex=True)
    df2 = df2.convert_dtypes()

    with warnings.catch_warnings():
        # Suppress RuntimeWarning during attempts to cast
        warnings.simplefilter("ignore", RuntimeWarning)  
        for col in df2.columns:
            try:
                df2[col] = pd.to_datetime(df2[col].astype(str),
                                          infer_datetime_format=True)
            except:
                pass  # Leave it
    return df2
#---------------------------------------------------------------------------------- 

def xlsx_tabs_to_pd_dataframes(path, header_idx=0, rm_newlines=True):
    """
    Read all sheets/tabs from an excel file into a list of 
    pandas DataFrames.

    Parameters:
    ----------
    path : str
        Path to the Excel file.
    header_idx : int, optional
        Row index to use as column names (0-indexed).
        Defaults to 0.
    rm_newlines : Boolean, optional
        Option to remove newline characters ('\n') from 
        all string columns.
        Defaults to True.
    Returns:
    -------
    list of pandas.DataFrame
        A list containing a DataFrame for each worksheet in 
        the Excel file.
    """

    dfs = {}
    xls = pd.ExcelFile(path)

    # Iterate through each worksheet and read its data into a DataFrame
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(path, sheet_name=sheet_name, header=header_idx)
        if rm_newlines:
            df = remove_pd_df_newlines(df)
        dfs[sheet_name] = df

    return dfs

#---------------------------------------------------------------------------------- 

def data_dict_to_json(data_dict_file, out_dir=None, out_name=None, na_value='N/A'):
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
        na_value (str, optional): 
        Value to use for filling missing data. 
            Defaults to 'N/A'.

    Returns:
    -------
        json_string (str):
        Formatted JSON string representing the processed data dictionary.
    """
    try:
        # Read the xlxs data dictionary file, convert each tab into a dataframe, 
        # Return a dictionary {tabName: dataframe}
        dfs = xlsx_tabs_to_pd_dataframes(data_dict_file)

        # Iterate through the dataframes to create a new subset dictionary
        data_dict = {}
        for k, df in dfs.items():
            # Check to see if each sheet/tab matches the data dictionary columns/schema
            if set(DATA_DICT_SCHEMA.keys()).issubset(set(list(df.columns))):
                # Ensure data types
                df2 = df.astype(DATA_DICT_SCHEMA, errors='ignore') 
                # Fill any null items with the na_value
                df2 = df2.dropna(subset=[DATA_DICT_PRIMARY_KEY], inplace=False)
                df2 = df2.fillna(na_value)
                # Convert the dataframes into dictionaries for easier lookup
                df2 = df2.set_index(DATA_DICT_PRIMARY_KEY)
                sheet_schema = df2.to_dict(orient='index') 
                sheet_schema = {k: {**v, DATA_DICT_PRIMARY_KEY: k} 
                                for k, v in sheet_schema.items()}
                data_dict[k] = sheet_schema
        del(dfs)

        # convert any nested string literal lists, dicts, tuples into python objects
        data_dict = eval_nested_string_literals(data_dict)

        # convert the dictionary to a formated JSON string
        json_string = json.dumps(data_dict, indent=4, sort_keys=True)

        if bool(out_dir) and bool(out_name):
            output_path = os.path.join(out_dir, f'{out_name}.json')
            # save the JSON text to a file
            with open(output_path, "w") as f:
                f.write(json_string)
            print(f'Data saved to: {output_path}')

    except Exception as e:  # Catch any type of exception
        print("An error occurred:", e)  # Print the error message

    return data_dict

#---------------------------------------------------------------------------------- 

def read_csv_or_excel_to_df(file_path, multi_sheets=False):
    """
    Reads a CSV or Excel file and returns data as a dictionary 
    of DataFrames, where keys are sheet names and values are 
    DataFrames containing data from each sheet.

    Parameters
    ----------
    file_path : str
        Path to the CSV, XLSX, or XLS file.

    multi_sheets : bool, optional (default=False)
        If True, allows reading multiple sheets from Excel files.
        If False, raises an error if the Excel file has multiple sheets.

    Returns
    -------
    dict
        Returns a dictionary of DataFrames, 
        where keys are sheet names and values are DataFrames containing data from each sheet.

    Raises
    ------
    ValueError
        If the file has multiple sheets and multi_sheets=False, or if the file format is unsupported.
    """

    # ext = file_path.split(".")[-1].lower()
    # base_name, ext = os.path.splitext(file_path)

    filename = os.path.basename(file_path)
    base_name, ext = os.path.splitext(filename)
 

    if ext in [".xlsx", ".xls"]:
        try:
            dfs = xlsx_tabs_to_pd_dataframes(file_path)
            if not multi_sheets and len(dfs.keys()) > 1:
                raise ValueError(f'File contains multiple sheets:{dfs.keys()}. Allow multi-sheets is set to False!')
            else:
                return dfs
        except ImportError:
            raise ValueError("Failed to import: {file_path}")
    elif ext == ".csv":
        # Read with string dtypes for accurate inference
        df = pd.read_csv(file_path)
        return {base_name : df}  
    else:
        raise ValueError(f"Unsupported file type: {ext}")

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
                 "Integer", "Float", "Boolean", "Datetime", "String", or "Other".
    """
    non_null_values = pd_series.replace(r'^\s+$', pd.NA, regex=True)  # Empty strings, including whitespaces
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

    # Check for categorical data
    elif pd.api.types.is_categorical_dtype(non_null_values):
        return "String"

    # Check for datetime
    elif pd.api.types.is_datetime64_any_dtype(non_null_values):
        return "Datetime"

    # Check for strings
    elif pd.api.types.is_string_dtype(non_null_values):
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

def isnull_or_empty(series):
    """Checks if a Pandas Series contains any null values or empty strings.
    Parameters:
        series : (pd.Series)
            The Pandas Series to check.
    Returns:
        bool: 
            True if the Series contains any null values or empty strings, False otherwise.
    """
    return  series.isnull().any() or series.astype(str).str.strip().eq('').any()

#---------------------------------------------------------------------------------- 

def get_numeric_range(series, attribute, na_val='N/A'):
    """Calculates the minimum or maximum value for a numeric Series, handling both 
    numerical and non-numerical cases.
    Parameters:
        series (pd.Series): 
            The Pandas Series to process.
        attribute (str): 
            The desired statistical attribute, either 'min' or 'max'.
        na_val : (Any, optional): 
            The value to return if the Series is empty or non-numeric. 
            Defaults to 'N/A'.
    Returns:
        float, int, str: 
            The minimum or maximum value in the Series, or `na_val` if the Series is 
            empty or non-numeric. If the Series is numeric the min or max value as an
            integer if possible, otherwise as a float. If the Series is empty or 
            non-numeric, returns (na_val).
    """
    if not pd.api.types.is_numeric_dtype(series):
        return na_val  # Return `na_val` for non-numeric Series
    
    if attribute == 'min':
        return int(series.min()) if int(series.min()) == float(series.min()) else float(series.min())
    elif attribute == 'max':
        return int(series.max()) if int(series.max()) == float(series.max()) else float(series.max())
#---------------------------------------------------------------------------------- 

def build_data_dictionary(df, 
                          max_unique_vals=100,
                          false_val=False,
                          true_val=True,
                          na_val="N/A"):
    """
    Creates a detailed data dictionary from a Pandas DataFrame, capturing key attributes for each column.

    Parameters:
    ----------
    df (pandas.DataFrame): 
        The DataFrame to analyze.
    max_unique_vals (int, optional): 
        The maximum number of unique values to include in the 
        allowed value list for string columns. 
            Defaults to 25.
    false_val (str, optional): 
        The value to use for False boolean values. 
            Defaults to False.
    true_val (str, optional): 
        The value to use for True boolean values. 
        Defaults to True.
    na_val (str, optional): 
        The value to use for N/A or not applicable values. Defaults to "N/A".

    Returns:
    -------
    dict: A dictionary of dictionaries, each representing a column with the following attributes:
        - field_name (str): Name of the column.
        - data_type (str): Data type of the column.
        - allow_null (bool): Indicates whether the column allows null values.
        - (int or str): Maximum string  for string columns, or 'N/A' for other types.
        - range_min (float or int): Minimum value for numeric columns, or 'N/A' for other types.
        - range_max (float or str): Maximum value for numeric columns, or 'N/A' for other types.
        - regex_pattern (str): Regular expression pattern for the column, or 'N/A' if not applicable.
        - unique_value (bool): Indicates whether the column has unique values.
        - allowed_value_list (list or str): A sorted list of allowed values for non-unique string 
            columns with a manageable number of unique values, or 'N/A' otherwise.
    """
    data_dict = {}

    df = infer_df_optimal_dtypes(df)
    for col in df.columns:
        # get a null mask
        null_mask = df[col].isnull()
        # check if the entire column is null
        null_col = null_mask.all()

        # Identify non-null values
        non_null_mask = df[col].notnull() 
        # create a series of non-nulls
        _s = df.loc[non_null_mask, col]
        # identify duplicates and count
        dups =  _s.duplicated(keep=False)
        dup_cnt = int(dups.sum())

        # default column info structure/values (null columns)
        column_info = {
            "field_name": col,
            "data_type": "Null-Unknown",
            "allow_null": true_val,
            "null_count": int(len(df)),
            "duplicate_count": dup_cnt,
            "length": na_val,
            "range_min": na_val,
            "range_max": na_val,
            "regex_pattern": na_val,  
            "unique_value": na_val,
            "allowed_value_list": na_val,
            "required": false_val
            }  

        # create column info structure/values (non-null columns)
        if not null_col:

            column_info = {
                "field_name": col,
                "data_type": infer_data_types(_s),
                "allow_null":  true_val if isnull_or_empty(df[col]) else false_val,
                "null_count": int(null_mask.sum()),
                "duplicate_count": dup_cnt,
                "length": na_val,
                "range_min": get_numeric_range(_s, 'min', na_val),  
                "range_max": get_numeric_range(_s, 'max', na_val),
                "regex_pattern": na_val,
                "unique_value": true_val if dup_cnt == 0 else false_val,
                "allowed_value_list": na_val,
                "required": true_val,
            }

            # document allowed values found           
            if pd.api.types.is_numeric_dtype(_s):
                try:
                    # try to cast the series as an int 
                    _s = _s.astype(int)   
                except:
                    pass

            if pd.api.types.is_string_dtype(_s) or pd.api.types.is_integer_dtype(_s):  
                if _s.nunique() <= max_unique_vals: 
                    if pd.api.types.is_integer_dtype(_s):
                        column_info["allowed_value_list"] = sorted([int(x) for x in list(_s.unique())])  

                    elif column_info['data_type'] == 'String':
                        column_info["allowed_value_list"] = sorted(list(_s.astype(str).unique()))  

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

def dataset_schema_to_json(file_path, out_dir=None, out_name=None, na_value='N/A'):
    """Generates a data dictionary JSON string given a spread 
    sheet (CSV, XLSX, or XLS) file.

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
        na_value (str, optional): 
            Value to use for filling missing data. 
            Defaults to 'N/A'.
        schema (JSON, optional): 
            Optional data dictionary/schema.  Attempts will be
            made to cast datatypes            
    Returns:
    -------
        json_string (str):
        Formatted JSON string representing the processed data dictionary.
    """

    _schema = {}
    dfs = read_csv_or_excel_to_df(file_path, multi_sheets=True)

    # attempt to cast data types for each dataframe

    for sheet_name, df in dfs.items():
 
        # generate the schema dictionary
        _schema[sheet_name] = build_data_dictionary(df, na_val=na_value)
        
    # convert any nested string literal lists, dicts, tuples into python objects
    _schema = eval_nested_string_literals(_schema)  
        
    # convert the dictionary to a json object
    json_string = json.dumps(_schema, indent=4, sort_keys=True)

    if bool(out_dir) and bool(out_name):
        # If there's no ".json" at all, append it
        if not out_name.endswith('.json'):
            out_name = f'{out_name}.json'
        # If there's no "_data_dictionary" before the final ".json", insert it
        if not out_name.endswith('_data_dictionary.json'):
            out_name = f'{os.path.splitext(out_name)[0]}_data_dictionary.json'
        output_path = os.path.join(out_dir, out_name)
        # save the JSON text to a file
        with open(output_path, "w") as f:
            f.write(json_string)
        print(f'Data saved to: {output_path}')

    return _schema
    
#---------------------------------------------------------------------------------- 

def write_dataframes_to_xlsx(dataframes, out_dir, out_name, sheet_order=None):
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
            A list specifying the desired order of the sheets in the output spreadsheet.
            Defaults to dictionary keys.          
    Returns:
    -------
        output_path (str):
            Output path to the xlsx file
    """

    MAX_ROWS = 1048575  # Maximum rows allowed in an Excel sheet
    MAX_COLS = 16383  # Maximum columns allowed in an Excel sheet

    # If there's no ".xlsx" at all, append it
    if not out_name.endswith('.xlsx'):
        out_name = f'{out_name}.xlsx'
    output_path = os.path.join(out_dir, out_name)

    if not bool(sheet_order):
        sheet_order=list(dataframes.keys())

    # Create an ExcelWriter object
    writer = pd.ExcelWriter(output_path)

    # create a tempfile as some env's don't allow file seek (dataBricks w/Azure Blob)
    temp_file = '/tmp/temp.xlsx'
    with pd.ExcelWriter(temp_file) as writer:
        # Iterate through the top-level keys (sheet names)
        for sheet_name in sheet_order:
            df = dataframes[sheet_name]

            # Check if splitting is needed
            if df.shape[0] > MAX_ROWS or df.shape[1] > MAX_COLS:
                chunk_size = MAX_ROWS  
                count = 1

                while len(df) > 0:
                    chunk = df[:chunk_size]
                    df = df[chunk_size:]

                    # Use a generator expression for iteration over chunks
                    for chunk in (df[i:i+chunk_size] for i in range(0, len(df), chunk_size)):
                        new_sheet_name = f"{count}_{sheet_name}"
                        chunk.to_excel(writer, sheet_name=new_sheet_name, index=False)
                        count += 1
            else:
                df.to_excel(writer, sheet_name=sheet_name, index=False)

    # overwrite the file if it exists already
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

def dataset_schema_to_xlsx(file_path, out_dir, out_name, na_value='N/A',multi_sheets=True):
    """Generates a data dictionary XLSX file given a spread 
    sheet (CSV, XLSX, or XLS) containing real data.

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
        multi_sheets : bool, optional (default=False)
            If True, allows reading multiple sheets from Excel files.
            If False, raises an error if the Excel file has multiple sheets.            
    Returns:
    -------
        output_path (str):
            Output path to the xlsx file
    """
   # If there's no ".xlsx" at all, append it
    if not out_name.endswith('.xlsx'):
        out_name = f'{out_name}.xlsx'
    # If there's no "_data_dictionary" before the final ".xlsx", insert it
    if not out_name.endswith('_data_dictionary.xlsx'):
        out_name = f'{os.path.splitext(out_name)[0]}_data_dictionary.xlsx'
    output_path = os.path.join(out_dir, out_name)

                 
    _schema = {}
    dfs = read_csv_or_excel_to_df(file_path, 
                                  multi_sheets=multi_sheets)

    for sheet_name, df in dfs.items():
        # generate the schema dictionary
        _schema[sheet_name] = build_data_dictionary(df,  
                                                    false_val='False',
                                                    true_val='True',
                                                    na_val=na_value)
        
    # Create an ExcelWriter object
    writer = pd.ExcelWriter(output_path)

    # create a tempfile as some env's don't allow file seek (dataBricks w/Azure Blob)
    temp_file = '/tmp/temp.xlsx'
    with pd.ExcelWriter(temp_file) as writer:
        # Iterate through the top-level keys (sheet names)
        for sheet_name, sheet_schema in _schema.items():
            
            df = pd.DataFrame.from_dict(sheet_schema, 
                                        orient='index')

            df.to_excel(writer, sheet_name=sheet_name, index=False)

    # overwrite the fiel if it exists already
    if os.path.exists(output_path):
        os.remove(output_path)  # Remove existing file before copying
    shutil.copyfile(temp_file, output_path)
    print(f'Data Dictionary saved to: {output_path}')

    try:
        os.remove(temp_file)  # Try to clean up the tempfile
    except:
        pass
    return output_path
    
#---------------------------------------------------------------------------------- 

def get_dict_diffs(dict1, dict2):
    """
    Compares two dictionaries and returns a dictionary containing mismatches.
    Parameters:
    ----------
        dict1 (dict): 
            The test ort control dictionary to compare dict2 aginst.
        dict2 (dict): 
            The observed or actual values to compare aginst dict1.
    Returns:
    -------
        mismatches (dict):
            A dictionary containing differences between the two 
            dictionaries where the 'expected' key is the baseline 
            or test in dict1 and the 'observed' key is the value
            in dict2.  Only unmatched values will be returned
    Raises:
        TypeError: If either `dict1` or `dict2` is not a dictionary.
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
                # try to cast to ints
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

def schema_validate_column_types(attribute, p_errors):
    """
    Checks if the observed data type matches the expected data type.
    Parameters
    ----------
        attribute (str): 
            The name of the attribute to check.
        p_errors (dict): 
        A dictionary containing potential errors, where keys are attribute names 
        and values are dictionaries with 'expected' and 'observed' values.
    Returns:
        str or None: Returns the attribute name if an inequality is found, 
        indicating an error. Returns None if the values match.
    """
    # data type infer that pandas could misinterpreted, but can be re-cast without losing data
    allowed_casting = {
                        "String"	    : ["String"],
                        "Float"		    : ["Float", "String"], 
                        "Boolean"	    : ["Boolean", "String"],
                        "Datetime"	    : ["Datetime", "String"],
                        "Integer"	    : ["Integer", "Float", "String"],
                        "Other"		    : ["String"],
                        "Null-Unknown"  : ["Integer", "Float", "String", "Boolean", "Datetime"]
                      }
    
    obs_type = p_errors[attribute]['observed']
    exp_type = p_errors[attribute]['expected']
    
    if exp_type != obs_type and exp_type not in allowed_casting[obs_type]: 
        return attribute
    return None

#---------------------------------------------------------------------------------- 

def schema_validate_column_lenth(attribute, p_errors):
    """
    Checks if the observed max string length of a column matches the 
    expected max string length.
    Parameters
    ----------
        attribute (str): 
            The name of the attribute to check.
        p_errors (dict): 
        A dictionary containing potential errors, where keys are attribute names 
        and values are dictionaries with 'expected' and 'observed' values.
    Returns:
        str or None: Returns the attribute name if an inequality is found, 
        indicating an error. Returns None if the values match.
    """

    obs_len = p_errors[attribute]['observed']
    exp_len = p_errors[attribute]['expected']

    if (isinstance(exp_len, str) and exp_len.isnumeric()) or isinstance(exp_len, (int, float)):
        if not (isinstance(obs_len, str) and obs_len.isnumeric()) and not isinstance(obs_len, (int, float)):
            return attribute
        elif int(obs_len) > int(exp_len):
            return attribute

    return None

#----------------------------------------------------------------------------------

def schema_validate_allow_null(attribute, p_errors):
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

def schema_validate_unique(attribute, p_errors):
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
        Returns the attribute name if a null value is not allowed, indicating an error.
        Returns None if null values are permitted.
    """

    if p_errors[attribute]['expected'] and not p_errors[attribute]['observed']:
        return attribute
    return None

#---------------------------------------------------------------------------------- 

def schema_validate_range(attribute, p_errors, msg_vals):
    """
    Checks if a numeric value for a given attribute falls within the expected range.
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
        Returns the attribute name if the value is outside the expected range,
        indicating an error. Returns None if the value is within the range.
    """
    if isinstance(p_errors[attribute]['expected'], (int, float)):
        if isinstance(p_errors[attribute]['observed'], (int, float)):
            exp_val = p_errors[attribute]['expected']
            obs_val = p_errors[attribute]['observed']
            # logic to determine when errors are flagged
            rng_logic = {
                'length': lambda expected, observed: expected < observed,
                'range_max': lambda expected, observed: expected < observed,
                'range_min': lambda expected, observed: expected > observed,
            }
            if rng_logic[attribute](exp_val, obs_val):
                msg_vals["expected"] = int(exp_val) if int(exp_val)==exp_val else exp_val
                msg_vals["observed"] = int(obs_val) if int(obs_val)==obs_val else obs_val
                return attribute
            else:
                p_errors[attribute]['status'] = 'Fail'
                p_errors[attribute]['erors'] = (
                    f'Data Type Error: Unable to validate {attribute}, check data types'
                )
    return None

#---------------------------------------------------------------------------------- 

def schema_validate_allowed_values(attribute, p_errors, msg_vals):
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

    if isinstance(p_errors[attribute]['expected'], list) and isinstance(
        p_errors[attribute]['observed'], list):
        allowed_vals = set([str(x) for x in p_errors[attribute]['expected']])
        observed_vals = set([str(x) for x in p_errors[attribute]['observed']]) 
        if not observed_vals.issubset(allowed_vals): 
            err_vals = list(observed_vals - allowed_vals)

            # Regular expression for integers
            pattern = r"^-?\d+$"  # Matches integers only (no decimals)
            # Filter values matching the pattern
            int_vals = [int(v) for v in err_vals if re.match(pattern, str(v))]
            if len(int_vals) == len(err_vals):
                err_vals=int_vals
            msg_vals['err_vals'] = err_vals
            return attribute
    return None

#---------------------------------------------------------------------------------- 

def schema_validate_attribute(attribute, p_errors, col, msg_vals):
    """
    Validates specific schema attributes and returns the error type if applicable.
    Parameters
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
    Returns
    -------
    str or None:
        Returns the error type if a violation is found for the attribute.
        Returns None if no errors are detected for the attribute.
    """
    # attributes to test if expected numeric value is within a range    
    range_checks= ['length','range_max','range_min']
    
    if attribute == 'data_type':
        return schema_validate_column_types(attribute, p_errors)        
    elif attribute == 'allow_null':
        return schema_validate_allow_null(attribute, p_errors)
    elif attribute == 'length':
        return schema_validate_column_lenth(attribute, p_errors)
    elif attribute == 'unique_value':
        return schema_validate_unique(attribute, p_errors)
    elif attribute == 'allowed_value_list':
        return schema_validate_allowed_values(attribute, p_errors, msg_vals)        
    elif attribute in range_checks:
        return schema_validate_range(attribute, p_errors, msg_vals)
    return None  # No error found for this attribute

#---------------------------------------------------------------------------------- 

def validate_schema(observed_schema, data_dictionary, schema_mapping):
    """
    Validates observed datasets against a data dictionary and returns schema violations.

    Parameters
    ----------
    observed_schema : dict
        The observed schema as a dictionary.
    data_dictionary : dict
        The data dictionary as a dictionary.
    schema_mapping : List[Dict]
        A list of mappings between observed datasets and corresponding data dictionary sections.

    Returns
    -------
    schema_violations : Dict
        A dictionary containing schema violations, where keys are dataset names and values are dictionaries
        with flagged columns and their errors.
    """

    # create a dict for hold schema violations
    schema_violations = {}

    em = SCHEMA_ERROR_MESSAGES

    # Iterate the schema_mapping object to 
    # test datasets against the given data dictionary
    for mapping in schema_mapping:
        # get the schema mapping (observed and expected)
        observed_dataset = mapping['dataset']
        data_dict_section = mapping['data_dict']
        
        # Iterate the authoritative schema by key/section and flag potential errors
        v_results = {}

        # get the authoritative schema 
        auth_schema = data_dictionary.get(data_dict_section)
        # if the auth_schema in not found return an error
        if not auth_schema:
            _e = f'Authoritative schema "{data_dict_section}" not found in keys, please check schema_mapping!'
            print(_e)
            return {data_dict_section: {'schema_mapping': schema_mapping, 'errors': _e}}

        # iterate the columns and properties in auth_schema 
        for col, col_props in auth_schema.items():
            errors = {}
            msg_vals = {"col": col}  

            # Initially flag all potential issues by checking expected vs observed  
            # Follow-on steps/logic will determine if these are truly errors.
            if col in observed_schema[observed_dataset].keys():
                obs_vals = observed_schema[observed_dataset][col]
                # get potential errors by calling get_dict_diffs 
                p_errors = get_dict_diffs(col_props, obs_vals)

                # iterate any potenital errors to determine if each is truly an error
                for atttr in p_errors:
                    error_type = None
                    msg_vals["expected"]=p_errors[atttr]['expected']
                    msg_vals["observed"]=p_errors[atttr]['observed']

                    error_type = schema_validate_attribute(atttr, p_errors, col, msg_vals)
                    if error_type:
                        errors[atttr] = p_errors[atttr]
                        if error_type =='allow_null':
                            null_count = obs_vals.get('null_count')
                            if null_count: 
                                msg_vals["count"]=null_count
                        if error_type =='unique_value':
                            dup_count = obs_vals.get('duplicate_count')
                            if dup_count: 
                                msg_vals["count"]=dup_count       
                                                 
                        errors[atttr]['errors'] = SCHEMA_ERROR_MESSAGES[atttr].format(**msg_vals)

            elif col_props['required'] == True:
                # if the column is missing, return a required column error
                errors = {"required_column": { 
                                "expected": True, 
                                "observed": False,
                                "errors": SCHEMA_ERROR_MESSAGES['required_column'].format(**msg_vals)}
                            }
            elif col_props['required'] == False:
                # if the column is missing, but optional
                errors = {"optional_column": { 
                                "expected": True, 
                                "observed": False,
                                "errors": SCHEMA_ERROR_MESSAGES['optional_column'].format(**msg_vals)}
                            }
                        

            if bool(errors):
                v_results[col] = {'status':'fail', 'required':col_props['required']}  | errors
        schema_violations[observed_dataset] = {'schema_violations': v_results}                        
    return schema_violations

#---------------------------------------------------------------------------------- 

def value_errors_nulls(df, column_name, unique_column=None):
    """
    Identifies null values in a DataFrame column and returns either
    their row indices or unique values.
    Parameters:
    ----------
        df : (pd.DataFrame)
            The DataFrame to check.
        column_name : (str)
            The name of the column to check for null values.
        unique_column : (str, optional)
            The name of the column containing unique values.
    Returns:
    -------
        list: A list of dictionaries, each containing 'sheet_row', 'Error Type', 'Column Name',
              and the unique column value (if provided).
    """
    null_mask = df[column_name].isnull()  # Create a boolean mask of null values

    results = []
    for row_index, row in df[null_mask].iterrows():
        output_dict = {
                        'Sheet Row': row_index + 2,
                        'Error Type': 'Null Value',
                        'Column Name': column_name,
                        'Error Value': row[column_name]
                      }
        if unique_column and unique_column in df.columns:
            output_dict[f"Lookup Column"]=unique_column
            output_dict[f"Lookup Value"] = row[unique_column]
        results.append(output_dict)
    return results

#---------------------------------------------------------------------------------- 

def value_errors_duplicates(df, column_name, unique_column=None):
    """
    Identifies duplicate values in a DataFrame column and returns their
    row indices, unique values (if provided), and the actual values from the column,
    along with error type and column name.
    Parameters:
    ----------
        df : (pd.DataFrame)
            The DataFrame to check.
        column_name : (str)
            The name of the column to check for duplicates.
        unique_column : (str, optional)
            The name of the column containing unique values.
    Returns:
    -------
        list: A list of dictionaries, each containing 'sheet_row', 'Error Type', 'Column Name',
              the unique column value (if provided), and the actual value from the 'column_name'.
    """
    # Create a boolean mask of duplicates
    null_mask = df[column_name].isnull()
    duplicate_mask =  df[column_name].duplicated(keep=False) & ~null_mask

    results = []
    for row_index, row in df[duplicate_mask].iterrows():
        output_dict = {
            'Sheet Row': row_index + 2,
            'Error Type': 'Duplicate Value',
            'Column Name': column_name,
            'Error Value': row[column_name]
        }
        if unique_column and unique_column in df.columns:
            output_dict[f"Lookup Column"]=unique_column
            output_dict[f"Lookup Value"] = row[unique_column]
        results.append(output_dict)

    return results

#---------------------------------------------------------------------------------- 

def value_errors_unallowed(df, column_name, allowed_values, unique_column=None):
    """
    Identifies values in a DataFrame column that are not in a given list of allowed values,
    considering data types for accurate comparison.  Optionally returns a unique value.
    Parameters:
    ----------
        df : (pd.DataFrame)
            The DataFrame to check.
        column_name : (str)
            The name of the column to check.
        allowed_values : (list)
            The list of allowed values.
        unique_column : (str, optional)
            The name of the column containing unique values.
    Returns:
    -------
        list: A list of dictionaries, each containing 'sheet_row', 'Error Type', 'Column Name',
              the unique column value (if provided), and the actual value from the 'column_name'.
    """
    # Get the DataFrame column's data type
    column_dtype = df[column_name].dtype  
    # Ensure same data type for comparison
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
            'Error Value': row[column_name]
        }
        if unique_column and unique_column in df.columns:
            output_dict[f"Lookup Column"]=unique_column
            output_dict[f"Lookup Value"] = row[unique_column]
        results.append(output_dict)

    return results

#----------------------------------------------------------------------------------

def value_errors_length(df, column_name, max_length, unique_column=None):
    """
    Identifies values in a DataFrame column that exceed a specified maximum length,
    handling any data type by converting values to strings. Returns no results
    if all values can be converted to strings within the limit.
    Parameters:
    ----------
        df : (pd.DataFrame)
            The DataFrame to check.
        column_name : (str)
            The name of the column to check.
        max_length : (int)
            The maximum allowed length for values.
        unique_column : (str, optional)
            The name of the column containing unique values.
    Returns:
    -------
        list: A list of dictionaries, each containing 'sheet_row', 'Error Type', 'Column Name',
              the unique column value (if provided), and the actual value from the 'column_name'.
              Returns an empty list if all values can be converted to strings within the limit.
    """
    try:
        # Attempt to convert all values to strings
        _s= df[column_name].fillna('').astype(str)
    except ValueError:
        return []  # Conversion failed, handle exceeding values

    exceeding_mask = _s.str.len() > max_length 

    results = []
    for row_index, row in df[exceeding_mask].iterrows():
        output_dict = {
            'Sheet Row': row_index + 2,
            'Error Type': f'Value Exceeds Max Length ({max_length})',
            'Column Name': column_name,
            'Error Value': row[column_name]
        }
        if unique_column and unique_column in df.columns:
            output_dict[f"Lookup Column"]=unique_column
            output_dict[f"Lookup Value"] = row[unique_column]
        results.append(output_dict)
        
    return results

#----------------------------------------------------------------------------------

def value_errors_out_of_range(df, column_name, test_type, value, unique_column=None):
    """
    Identifies values in a DataFrame column that fall outside a specified range 
    (either below a minimum or above a maximum value).
    Parameters:
    ----------
    df : (pd.DataFrame)
        The DataFrame to check.
    column_name : (str)
        The name of the column to check.
    test_type : (str)
        'min' or 'max' indicating the type of test to perform.
    value : (int, float): 
        The minimum or maximum allowed value, depending on the test_type.
    unique_column : (str, optional)
        The name of the column containing unique values.
    Returns:
    -------
    list: A list of dictionaries, each containing 'Sheet Row', 'Error Type', 'Column Name',
         the unique column value (if provided), and the actual value from the 'column_name'.
    """
   
    null_mask = df[column_name].isnull()
    
    if test_type not in ("min", "max"):
        raise ValueError("test_type must be either 'min' or 'max'")

    error_type = None
    if test_type == "min":
        mask = df[column_name].notna() & (df[column_name] < value)
        error_type = f"Below Minimum Allowed Value ({value})"
    elif test_type == "max":
        mask = df[column_name].notna() & (df[column_name] > value)
        error_type = f"Exceeds Maximum Allowed Value ({value})"

    results = []
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
                output_dict[f"Lookup Column"]=unique_column
                output_dict[f"Lookup Value"] = row[unique_column]
            results.append(output_dict)
    return results

#----------------------------------------------------------------------------------

def value_errors_regex_mismatches(df, column_name, regex_pattern, unique_column=None):
    """
    Identifies values in a DataFrame column that do not match a specified regex pattern, ignoring null values.
    Parameters:
    ----------
        df : (pd.DataFrame)
            The DataFrame to check.
        column_name : (str)
            The name of the column to check.
        regex_pattern : (str)
            The regular expression pattern to check against.
        unique_column : (str, optional)
            The name of the column containing unique values.
    Returns:
    -------
        list: A list of dictionaries, each containing 'sheet_row', 'Error Type', 'Column Name',
              the unique column value (if provided), and the actual value from the 'column_name'.
    """
    non_null_mask = df[column_name].notnull()  # Identify non-null values
    pattern_match = df.loc[non_null_mask, column_name].astype(str).str.match(regex_pattern)
    mismatch_mask = ~pattern_match  # Invert to get mismatches

    results = []
    for row_index, row in df.loc[non_null_mask & mismatch_mask].iterrows():  # Filter for mismatches
        output_dict = {
            'Sheet Row': row_index + 2,
            'Error Type': 'Invalid Value Formatting',
            'Column Name': column_name,
            'Error Value': row[column_name]
        }
        if unique_column and unique_column in df.columns:
            output_dict[f"Lookup Column"]=unique_column
            output_dict[f"Lookup Value"] = row[unique_column]
        results.append(output_dict)

    return results

#----------------------------------------------------------------------------------

def get_value_errors(dataset_path, schema_errors, data_dictionary, 
                     schema_mapping, ignore_errors=['allow_null']):
    """
    Identifies value errors within a dataset based on the results of 
    the schema validation JSON object, the data dictionary JSON object, 
    and schema mapping (if required) applied aginst a target spearadsheet 
    dataset. 

    Parameters:
    ----------
    dataset_path : str
        The path to the dataset file (CSV or Excel) to be validated.
    schema_errors: dict
        The result of validate_schema()
    data_dictionary : dict str
        The result of data_dict_to_json()
    schema_mapping : list of dicts
        A list of mappings between dataset names and corresponding data 
        dictionary sections.
    ignore_errors : list, optional
        A list of error types to exclude from the analysis. 
        Default is ['allow_null'].        
    Returns:
    -------
        list: A list of dictionaries, each containing 'sheet_row', 'Error Type', 'Column Name',
              and the unique column value (if provided).
    """
    # read the dataframes in as sheets if needed
    dfs = read_csv_or_excel_to_df(dataset_path, multi_sheets=True)

    # get the run uuid/file hashing
    uid = list(schema_errors.keys())[0]

    value_errors = {}
    for mapping in schema_mapping:
        # get the schema mapping (observed and expected)
        observed_dataset = mapping['dataset']
        data_dict_section = mapping['data_dict']

        # get the authoritative schema 
        auth_schema = data_dictionary.get(data_dict_section)
        # if the auth_schema in not found return an error
        if not auth_schema:
            _e = f'Authoritative schema "{data_dict_section}" not found in keys, please check schema_mapping!'
            print(_e)
            return {data_dict_section: {'schema_mapping': schema_mapping, 'Errors': _e}}

        sheet_result = schema_errors[uid]["results"][observed_dataset]
        # list to hold error values
        sheet_v_errors = []

        df = infer_df_optimal_dtypes(dfs[observed_dataset])
        schema_violations = sheet_result.get("schema_violations")
        unique_cols = [k for k in auth_schema.keys() if auth_schema[k]['unique_value']]  
        if schema_violations:
            if bool(unique_cols):
                unique_column=unique_cols[0] # use the first unique column as a primary key
            else:
                unique_column=None
            unique_column = get_best_uid_column(df, prefer_col=unique_column)

            for col, errors in schema_violations.items():
                flagged_errors = list(errors.keys())  # list all error types
                if 'allow_null' in flagged_errors and 'allow_null' not in ignore_errors: 
                    sheet_v_errors.append(
                        value_errors_nulls(df, 
                                           col, 
                                           unique_column=unique_column))
                if 'unique_value' in flagged_errors and 'unique_value' not in ignore_errors: 
                    sheet_v_errors.append(
                        value_errors_duplicates(df, 
                                                col,
                                                unique_column=unique_column) )
                if 'length' in flagged_errors and 'length' not in ignore_errors: 
                    max_length = errors['length']['expected']
                    sheet_v_errors.append(
                        value_errors_length(df,
                                            col,
                                            max_length=max_length,
                                            unique_column=unique_column))
                if 'range_max' in flagged_errors and 'range_max' not in ignore_errors: 
                    max_length = errors['range_max']['expected']
                    sheet_v_errors.append(                   
                        value_errors_out_of_range(df, 
                                                  col, 
                                                  test_type='max', 
                                                  value=max_length, 
                                                  unique_column=unique_column))
                if 'range_min' in flagged_errors and 'range_min' not in ignore_errors: 
                    min_length = errors['range_min']['expected']
                    sheet_v_errors.append(                  
                        value_errors_out_of_range(df, 
                                                  col,
                                                   test_type='min',
                                                   value=min_length,
                                                   unique_column=unique_column))   
                if 'allowed_value_list' in flagged_errors and 'allowed_value_list' not in ignore_errors: 
                    a_vals = errors['allowed_value_list']['expected']
                    sheet_v_errors.append(                  
                        value_errors_unallowed(df,
                                               col, 
                                               allowed_values=a_vals,
                                               unique_column=unique_column))
        # check value errors beyond a schema
        if 'regex_pattern' not in ignore_errors:
            for col in df.columns:
                if auth_schema.get(col):
                    # check regex patterns
                    ptrn =  auth_schema[col].get('regex_pattern')    
                    if ptrn and ptrn != 'N/A':
                        sheet_v_errors.append(                  
                            value_errors_regex_mismatches(df, 
                                                        col,
                                                        regex_pattern=ptrn,
                                                        unique_column=unique_column))
        merged_e_list =[]
        if bool(sheet_v_errors):
            if len(sheet_v_errors) > 0:
                 # merge the  list                                                       
                merged_e_list = [i for e in sheet_v_errors for i in e]
            else:
                merged_e_list = sheet_v_errors
            # ensure values are exportable to JSON
            merged_e_list = json.loads(pd.DataFrame(merged_e_list).to_json())
        value_errors[observed_dataset] = merged_e_list
    return {uid: value_errors}

#----------------------------------------------------------------------------------

def validate_dataset(dataset_path, data_dict_path, schema_mapping, 
                     list_errors=True, out_dir=None, out_name=None,
                     ignore_errors=['allow_null']): 
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
        A list of mappings between dataset names and corresponding data 
        dictionary sections.
    out_dir (str, optional): 
        Path to the output directory for the JSON file. 
            Defaults to None.
    out_name (str, optional): 
        Desired name for the output JSON file (without extension). 
            Defaults to None.
    Returns
    -------
    str
        A JSON string containing the validation results, including:
            - Dataset metadata
            - Data dictionary metadata
            - Schema validation errors
            - Regex validation errors (if any)
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
    data_dict = data_dict_to_json(data_dict_path, na_value='N/A')
    obs_schema = dataset_schema_to_json(dataset_path, na_value='N/A')

    # validate the observed json schema scehmaaginst the data dictionary
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
                                        data_dictionary=data_dict, 
                                        schema_mapping=schema_mapping,
                                        ignore_errors=ignore_errors)
        if value_errors:
            for sheet, errs in value_errors[uid].items():
                results[uid]["results"][sheet]["value_errors"]=errs
    # convert the dictionary to a formated JSON string & output the results
    #---------------   
    json_string = json.dumps(results, indent=4, sort_keys=True)

    if bool(out_dir) and bool(out_name):
        output_path = os.path.join(out_dir, f'{out_name}_({uid}).json')
        # save the JSON text to a file
        with open(output_path, "w") as f:
            f.write(json_string)
        print(f'Data saved to: {output_path}')

    return results
    
#---------------------------------------------------------------------------------- 

def schema_validation_to_xlsx(validation_results, out_dir, out_name=None):
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
            Defualts the the UUID/file hash ID string in 
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
    # metadata_df = infer_df_optimal_dtypes(metadata_df)

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
    # errors_ov_df = infer_df_optimal_dtypes(errors_ov_df)
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
                # val_errs_df = infer_df_optimal_dtypes(val_errs_df)
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