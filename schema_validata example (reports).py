# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook Purpose:
# MAGIC
# MAGIC This example notebook utilizes the schema_validata package to validate BEAD challenge results against a data dictionary stored in a user-friendly Excel (XLSX) format. 
# MAGIC
# MAGIC - Accessibility for Non-Coding Experts:
# MAGIC
# MAGIC   - Excel spreadsheets are familiar and easy to maintain for Subject Matter Experts (SMEs) without requiring coding knowledge. 
# MAGIC - Trade-offs
# MAGIC   - Initial Read and Re-reads for Data Type Determination: 
# MAGIC     - While the package strives to accurately determine data types and null representations, there's a trade-off in efficiency. schema_validata performs an initial read of each dataset to determine appropriate data types. It then re-reads the data to ensure consistent handling of null values (NA_VALUES, NA_PATTERNS) during validation. This re-reading step introduces some inefficiency.
# MAGIC   - Tailored for Multiple Spreadsheet Tabs: 
# MAGIC     - schema_validata was originally designed to process data from multiple datasets stored as separate tabs within a single Excel spreadsheet. This example focuses on individual CSV files with that require subsequent merging of results which also creates a trade-off in efficiency.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and import statements

# COMMAND ----------

# Install databricks_helper library
%pip install --force-reinstall git+https://github.com/ahamptonTIA/databricks_helper.git

# Install schema_validata library
%pip install --force-reinstall git+https://github.com/ahamptonTIA/schema_validata.git

# COMMAND ----------

# Restart the Python library in Databricks
dbutils.library.restartPython()

# COMMAND ----------

# Import the necessary libraries for the code
import databricks_helper as dh       # Custom library for Databricks functionality
import schema_validata as sv         # Custom library for schema validation
import os                            # Library for interacting with the operating system
import datetime                      # Library for working with dates and times
import hashlib                       # Library for generating hash values
import pandas as pd                  # Library for data manipulation and analysis
import json                          # Library for working with JSON objects

# COMMAND ----------

# Create a dictionary that maps state abbreviations to their corresponding FIPS codes
state_fips_lookup = {
    'AL': 1, 'AK': 2, 'AS': 60, 'AZ': 4, 'AR': 5,
    'CA': 6, 'CO': 8, 'CT': 9, 'DE': 10, 'DC': 11,
    'FL': 12, 'GA': 13, 'GU': 66, 'HI': 15, 'ID': 16,
    'IL': 17, 'IN': 18, 'IA': 19, 'KS': 20, 'KY': 21,
    'LA': 22, 'ME': 23, 'MD': 24, 'MA': 25, 'MI': 26,
    'MN': 27, 'MS': 28, 'MO': 29, 'MT': 30, 'NE': 31,
    'NV': 32, 'NH': 33, 'NJ': 34, 'NM': 35, 'NY': 36,
    'NC': 37, 'ND': 38, 'MP': 69, 'OH': 39, 'OK': 40,
    'OR': 41, 'PA': 42, 'PR': 72, 'RI': 44, 'SC': 45,
    'SD': 46, 'TN': 47, 'TX': 48, 'UM': 74, 'VI': 78,
    'UT': 49, 'VT': 50, 'VA': 51, 'WA': 53, 'WV': 54,
}

# COMMAND ----------

# MAGIC %md
# MAGIC # Parameters:

# COMMAND ----------

# List of state abbreviations as options for the dropdown widget
state_options = list(state_fips_lookup.keys())

# Create the dropdown widget using dbutils
state_dropdown = dbutils.widgets.dropdown("Selected State", state_options[0], state_options)

# Collect the selected state from the dropdown
selected_state = dbutils.widgets.get("Selected State")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set the data dictionary path:

# COMMAND ----------

# Assign the file path of the data dictionary in DBFS to the variable data_dict_path
data_dict_path = '/mnt/data/beadChallengeProcessDev/dataDictionary/bead_challenge_data_dict_v0.1.xlsx'

# Convert the DBFS path to a local os type path
data_dict_path = dh.dbfs_path.db_path_to_local(data_dict_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set the dataset path: 

# COMMAND ----------

# Set the input file path representing the actual data file
raw_dir = f'/mnt/data/beadChallengeProcessDev/rawSubmission/{selected_state}'

# Convert the DBFS path to a local os type path
raw_dir = dh.dbfs_path.db_path_to_local(raw_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the schema mapping (what data dictionary to validate each dataset)

# COMMAND ----------

# Get a list of file paths for all the CSV files in the given 'raw_dir' directory
csvs = dh.dbfs_path.list_file_paths(dbutils, dir_path=raw_dir, ext='csv')

# Initialize an empty dictionary to store the run data
run_data = {}

# Iterate over each file path in the list of CSV files
for csv in csvs:
    # Extract the filename from the file path
    filename = os.path.basename(csv)
    
    # Split the filename into the base name and file extension
    base_name, ext = os.path.splitext(filename)
    
    # Create a dictionary with keys 'dataset' and 'data_dict' and add it to the run_data dictionary
    run_data[csv] = [{'dataset': base_name,
                      'data_dict': f'{base_name.upper()}_SCHEMA'}]

# COMMAND ----------

# Display the content of the mapping dictionary
display(run_data)

# COMMAND ----------

# Display which values are treated as nulls by default
# Changes can be made by modifying sv.Config.NA_VALUES, For example, sv.Config.NA_VALUES.append()
for i in range(0, len(sv.Config.NA_VALUES), 10): 
    print(sv.Config.NA_VALUES[i:i+10])

# COMMAND ----------

# Display null value that are treated as nulls by default
# Changes can be made by modifying sv.Config.NA_PATTERNS, For example, sv.Config.NA_PATTERNS.append()
for np in sv.Config.NA_PATTERNS: display(np)

# COMMAND ----------

# MAGIC %md
# MAGIC # Validation:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define the output locations:

# COMMAND ----------

# Set the output directory for the data validation results
output_par_dir = raw_dir.replace('rawSubmission','dataValidationResults')

# Convert the output directory path to the local file system
output_report_dir = dh.dbfs_path.db_path_to_local(output_par_dir)

# Create the output directory if it does not exist
dh.dbfs_path.create_dir(dbutils,output_report_dir)

# Get the state abbreviation from the raw directory path
state_abbrv = os.path.basename(raw_dir)

# Get the current UTC datetime
now = datetime.datetime.utcnow()

# Extract the date part in the format YYYYMMDD
date_str = now.strftime("%Y%m%d")

# Set the output merged xlsx file name using the raw directory name and the current date
out_file_name = f"{os.path.basename(raw_dir)}_schema_({date_str})"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate the schema validation JSON/dictionary and intermediate xlsx output files

# COMMAND ----------

xlsx_files = []  # Initialize an empty list to hold the paths of the generated Excel files
datasets_metadata = {}  # Initialize an empty dictionary to hold dataset metadata
uid_list = []  # Initialize an empty list to hold the unique identifiers of the results
all_results = {}  # Initialize an empty dictionary to hold all the validation results

for csv, schema_mapping in run_data.items():
    # # Ignore schemas that are not in the specified list 'schemas'
    # if schema_mapping[0]['dataset'] not in schemas:
    #     continue
    
    filename = os.path.basename(csv)  # Get the filename from the CSV path
    base_name, ext = os.path.splitext(filename)  # Split the filename into base name and extension

    out_name_base = f"{os.path.basename(raw_dir)}_{base_name}_({date_str})"  # Generate the base name for the output files

    # Perform dataset validation and get the validation results
    results = sv.validate_dataset(csv, 
                                 data_dict_path, 
                                 schema_mapping,
                                 list_errors=True,
                                 out_dir=output_report_dir,
                                 out_name=out_name_base,
                                 ignore_errors=['allow_null'])

    all_results = {**all_results, **results}  # Merge the current results with all_results dictionary
     
    uid = list(results.keys())[0]  # Get the unique identifier of the validation results

    # Generate the Excel file with schema validation results
    out_file = sv.schema_validation_to_xlsx(validation_results=results, 
                                            out_dir=output_report_dir, 
                                            out_name=out_name_base)
    xlsx_files.append(out_file)  # Append the path of the generated Excel file to the xlsx_files list

# Generate a json string to return when the notebook is complete
full_results_json = json.dumps(all_results)

# Generate a list of corresponding JSON files for the generated Excel files
json_files = [x.replace('.xlsx', '.json') for x in xlsx_files if x.endswith('.xlsx')]  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge the output xlsx files into one dictionary of {sheet_name: data}

# COMMAND ----------

# Create an empty dictionary to hold dataframes
dfs = {}

# Create an empty list to hold the order of sheet names
sheet_order = []

# Iterate over each Excel file
for f in xlsx_files:
    # Convert the DBFS path to local path
    f = dh.dbfs_path.db_path_to_local(f)

    # Open the Excel file
    excel_file = pd.ExcelFile(f)

    # Extract the filename from the path
    filename = os.path.basename(f)

    # Iterate over each sheet in the Excel file
    for sheet_name in excel_file.sheet_names:
        # Add the sheet name to the sheet_order list if it doesn't exist
        if sheet_name not in sheet_order:
            sheet_order.append(sheet_name)
        
        # Read the sheet into a dataframe
        df = pd.read_excel(excel_file, sheet_name=sheet_name)

        # Check if the sheet is the 'Metadata' sheet
        if sheet_name == 'Metadata':
            # Add a new column 'file_validated' to the dataframe with the file name value
            df['file_validated'] = df[df['Attribute'] == 'file_name']['Value'].iloc[0]

        # Error handling: Check if the sheet already exists in the dfs dictionary
        if sheet_name in dfs:
            # Concatenate the existing dataframe with the new dataframe and update the value in the dfs dictionary
            existing_df = dfs[sheet_name]
            dfs[sheet_name] = pd.concat([existing_df, df], ignore_index=True)
        else:
            # Add the new dataframe to the dfs dictionary
            dfs[sheet_name] = df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Export the schema validation report to a spreadsheet

# COMMAND ----------

# Sorting the list of unique identifiers
uid_list.sort()

# Generating a merged file UUID that will be unique to the files/data dict used in this run
merge_file_postfix = hashlib.md5(''.join(uid_list).encode()).hexdigest()

# Generating an Excel file that contains multiple dataframes
out_xlsx = sv.write_dataframes_to_xlsx( dataframes=dfs, 
                                        out_dir=output_report_dir, 
                                        out_name= f"{out_file_name}_({merge_file_postfix})", 
                                        sheet_order=sheet_order)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Zip the output files for a single archive file

# COMMAND ----------

# Creating a zip file by zipping a list of files
# The list of files includes the json_files and out_xlsx variables
# The name of the output zip file is generated by concatenating out_file_name with the merge_file_postfix value
# The remove_files parameter is set to False, indicating that the original files will not be removed after zipping
files_to_zip = json_files + [out_xlsx]
zip = dh.zip_files(files_to_zip, output_filename=f"{out_file_name}_({merge_file_postfix})", remove_files=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean up the intermediate xlsx output files

# COMMAND ----------

# Delete the files
print('Cleaning up intermediate xlsx files')
# Iterate over each file in the xlsx_files list, check if the file exists
# If the file exists, remove it and print a message if the file was removed or not
for f in xlsx_files:
    try:
        if os.path.exists(f):
            os.remove(f)
            print(f' -Removed file: {f}')
        else:
            print(f' -File does not exist: {f}')
    except:
        print(f' -Could not remove: {f}!')

# COMMAND ----------

# MAGIC %md
# MAGIC # Create rolling log tables

# COMMAND ----------

def delete_nested_key(data, key_to_delete):
    """
    Recursively deletes a key and its subkeys from a nested dictionary.

    Parameters
    ----------
        data: The dictionary to modify.
        key_to_delete: The key to remove.

    Returns:
    ----------
        The modified dictionary (modified in-place).
    """

    if isinstance(data, dict):
        for k, v in list(data.items()):
            if k == key_to_delete:
                del data[k]
            else:
                delete_nested_key(v, key_to_delete)
    return data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the error log table

# COMMAND ----------

# Extract the relevant information from all_results
# Remove the 'value_errors' key and its subkeys from all_results
all_results = delete_nested_key(all_results, 'value_errors')

# Initialize an empty list to store the error information
error_list = []

# Iterate over each key-value pair in all_results
for uid, results in all_results.items():

    # Extract the valid file name from the dataset metadata
    valid_file = results['dataset_metadata']['file_name']

    # Iterate over each key-value pair in the 'results' dictionary
    for k,v in results['results'].items():

        # Iterate over each key-value pair in the 'schema_violations' dictionary
        for k2, v2 in v['schema_violations'].items():

            # Iterate over each key-value pair in the innermost dictionary
            for k3, v3 in v2.items():

                # Check if the key is not 'status' or 'required'
                if k3 not in ['status', 'required']:
                    errs = v3.get('errors')

                    # Check if there are errors
                    if errs:
                        # Append the error information as a dictionary to error_list
                        error_list.append({'uuid':uid,
                                           'state': state_abbrv,
                                           'file': valid_file,
                                           'column': k2,
                                           'error_type': k3,
                                           'error_message': errs}) 

# Convert the error_list to a pandas DataFrame
run_errors_df = pd.DataFrame(error_list)

# Display the run_errors_df DataFrame
run_errors_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Export the results to the error log, overwriting previous results for each uuid, state, and file

# COMMAND ----------

# Get the parent directory path for the output_report_dir
log_dir = os.path.dirname(output_report_dir)

# Create the output error log path by joining the parent directory path and the file name
out_error_path = os.path.join(log_dir, 'overview_file_error_log.csv')

# Write the run_errors_df DataFrame to a CSV file at the specified output path
# The 'pandas_upsert_csv' function is used to upsert the data, meaning it will overwrite any existing data with the same key columns
# The upsert key columns are ['uuid', 'state', 'file']
# The mode is set to 'overwrite' to replace any existing file at the output path
out_error_file = dh.file_ops.pandas_upsert_csv(df=run_errors_df, 
                                               output_path=out_error_path, 
                                               upsert_columns=['uuid','state','file'],
                                               mode='overwrite'
                                               )

# generate a json string for notebook exit results
error_overview_json = run_errors_df.to_json()
# Display the path of the output error file
out_error_file

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the metadata log table

# COMMAND ----------

# Initialize an empty list to store the log information
full_log = []

# Iterate over each key-value pair in ov_results
for uid, results in all_results.items():
    error_count = 0
    review_comments = ''
    # Create a dictionary for the log information with 'uuid' and 'state' as initial values
    if bool(run_errors_df['uuid'].value_counts().get(uid)):
        error_count = run_errors_df['uuid'].value_counts().get(uid)
        reviewed = "True"
    elif run_data[results['dataset_metadata']['file_path']]:
        reviewed = "True"
    else: 
        reviewed = "False"
        review_comments = 'Not reviewed - Incorrect schema mapping or missing data dictionary'
    uid_log = {'uuid':uid, 
               'state':state_abbrv, 
               'state_fips' : state_fips_lookup[state_abbrv],
               'has_schema_errors': 'True' if error_count > 0 else 'False',
               'schema_reviewed' : reviewed,
               'comemnts' : review_comments
              }
    
    # Extract the base name of the raw_dir path
    os.path.basename(raw_dir)
    
    # Add the 'start_time' value from the 'run_metadata' to the uid_log dictionary
    uid_log['start_time'] = results['run_metadata']['start_time']
    
    # Merge the uid_log dictionary with the 'dataset_metadata' dictionary from results
    uid_log = uid_log | results['dataset_metadata']
    
    # Add the key-value pairs from the 'data_dict_metadata' dictionary to the uid_log dictionary with a prefix 'dd_'
    uid_log = uid_log | {f'dd_{k}': v for k,v in results['data_dict_metadata'].items()}

    # Append the uid_log dictionary to the full_log list
    full_log.append(uid_log)

# Convert the full_log list to a pandas DataFrame
meta_data_df = pd.DataFrame(full_log)

# generate a json string for notebook exit results
metadata_log_json = meta_data_df.to_json()

# Display the meta_data_df DataFrame
meta_data_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Export the metadata to the file metadata log, upserting on uuid

# COMMAND ----------

# Get the parent directory path for output_report_dir
log_dir = os.path.dirname(output_report_dir)

# Create the output metadata log path by joining the parent directory path and the file name
out_meta_path = os.path.join(log_dir, 'overview_file_metadata_log.csv')

# Write the meta_data_df DataFrame to a CSV file at the specified output path
# The 'pandas_upsert_csv' function is used to upsert the data, meaning it will overwrite any existing data with the same key columns
# The upsert key column is 'uuid'
out_meta_file = dh.file_ops.pandas_upsert_csv(df=meta_data_df, 
                                              output_path=out_meta_path, 
                                              upsert_columns=['uuid'])

# Display the path of the output metadata file
out_meta_file

# COMMAND ----------

# Exiting the notebook and returning each result json
dbutils.notebook.exit({
                        'metadata_log': metadata_log_json,
                        'error_overview' : error_overview_json,
                        'full_results': full_results_json,
                    })
