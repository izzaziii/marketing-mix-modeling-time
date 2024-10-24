# Preparation for Marketing Mix Modeling

## Sales Data

### **Overview**

This script is designed to:

- **Fetch data** from a MongoDB collection.
- **Process** the data by converting date columns, grouping, and resampling.
- **Filter** the data based on a specified start date.
- **Export** the processed data to a CSV file.

The script is modular, with each function performing a specific task. Error handling and logging are implemented to ensure robustness and ease of debugging.

---

### **Dependencies**

```python
from databases import create_connection, convert_column_to_datetime
import pandas as pd
from datetime import datetime
import logging
```

- **databases**: Custom module assumed to contain `create_connection` and `convert_column_to_datetime`.
- **pandas**: Used for data manipulation.
- **datetime**: For handling date and time objects.
- **logging**: For logging informational messages and errors.

---

### **Function Definitions**

#### **1. fetch_data_from_mongodb**

```python
def fetch_data_from_mongodb(database: str, collection: str) -> pd.DataFrame:
    """
    Fetch data from a MongoDB collection and return it as a DataFrame.

    Parameters:
    ----------
    database : str
        The name of the MongoDB database.
    collection : str
        The name of the MongoDB collection.

    Returns:
    -------
    pd.DataFrame
        DataFrame containing the fetched data.

    Raises:
    ------
    ValueError
        If no data is found in the MongoDB collection.
    Exception
        If an error occurs during the MongoDB connection or data fetching.
    """
    try:
        coll = create_connection(database, collection)
        data = pd.DataFrame(coll.find({}))
        if data.empty:
            raise ValueError("No data found in the MongoDB collection.")
        return data
    except Exception as e:
        logging.error(f"Error fetching data from MongoDB: {e}")
        raise
```

- **Purpose**: Connects to a MongoDB collection and retrieves all documents, returning them as a pandas DataFrame.
- **Error Handling**:
  - Logs and raises an exception if the data is empty or if any other error occurs.

#### **2. process_dataframe**

```python
def process_dataframe(df: pd.DataFrame, date_column: str) -> pd.DataFrame:
    """
    Process the DataFrame by converting the date column, grouping, and resampling.

    Parameters:
    ----------
    df : pd.DataFrame
        The raw DataFrame.
    date_column : str
        The name of the date column to be converted to datetime.

    Returns:
    -------
    pd.DataFrame
        Processed DataFrame with weekly counts, grouped and resampled.

    Raises:
    ------
    KeyError
        If required columns are missing from the DataFrame.
    Exception
        If an error occurs during processing.
    """
    try:
        # Convert date column to datetime
        df = convert_column_to_datetime(df, date_column)

        # Check for required columns
        required_columns = [
            " Channel",
            "Funnel Fact.Package",
            "Blk State",
            "Blk Cluster",
            "Funnel SO No",
            date_column,
        ]
        if not all(col in df.columns for col in required_columns):
            missing_cols = set(required_columns) - set(df.columns)
            raise KeyError(f"Missing required columns: {missing_cols}")

        # Drop rows with missing dates, group, and resample
        processed_df = (
            df.dropna(subset=[date_column])
            .set_index(date_column)
            .groupby([" Channel", "Funnel Fact.Package", "Blk State", "Blk Cluster"])
            .resample("W-SUN")["Funnel SO No"]
            .count()
            .reset_index()
        )
        return processed_df
    except KeyError as e:
        logging.error(f"KeyError in processing DataFrame: {e}")
        raise
    except Exception as e:
        logging.error(f"Error processing DataFrame: {e}")
        raise
```

- **Purpose**: Converts the specified date column to datetime, checks for required columns, groups the data, and resamples it on a weekly basis.
- **Error Handling**:
  - Raises a `KeyError` if required columns are missing.
  - Logs and raises other exceptions that may occur during processing.

#### **3. filter_by_date**

```python
def filter_by_date(
    df: pd.DataFrame, date_column: str, start_date: datetime
) -> pd.DataFrame:
    """
    Filter the DataFrame to include only rows with dates on or after the start date.

    Parameters:
    ----------
    df : pd.DataFrame
        The DataFrame to filter.
    date_column : str
        The name of the date column to filter on.
    start_date : datetime
        The starting date for filtering.

    Returns:
    -------
    pd.DataFrame
        Filtered DataFrame.

    Raises:
    ------
    ValueError
        If the filtered DataFrame is empty after filtering.
    Exception
        If an error occurs during filtering.
    """
    try:
        filtered_df = df.loc[df[date_column] >= start_date]
        if filtered_df.empty:
            raise ValueError(
                f"No data available after filtering from {start_date} onwards."
            )
        return filtered_df
    except ValueError as e:
        logging.error(f"ValueError in filtering DataFrame: {e}")
        raise
    except Exception as e:
        logging.error(f"Error filtering DataFrame: {e}")
        raise
```

- **Purpose**: Filters the DataFrame to include only rows where the date is on or after the specified `start_date`.
- **Error Handling**:
  - Raises a `ValueError` if the filtered DataFrame is empty.
  - Logs and raises other exceptions that may occur during filtering.

#### **4. export_dataframe_to_csv**

```python
def export_dataframe_to_csv(df: pd.DataFrame, output_filepath: str) -> None:
    """
    Export the DataFrame to a CSV file.

    Parameters:
    ----------
    df : pd.DataFrame
        The DataFrame to export.
    output_filepath : str
        The path to save the CSV file.

    Returns:
    -------
    None

    Raises:
    ------
    Exception
        If an error occurs during exporting.
    """
    try:
        df.to_csv(output_filepath, index=False)
        logging.info(f"Data successfully exported to {output_filepath}.")
    except Exception as e:
        logging.error(f"Error exporting DataFrame to CSV: {e}")
        raise
```

- **Purpose**: Exports the DataFrame to a CSV file at the specified file path.
- **Error Handling**:
  - Logs and raises any exceptions that occur during the export process.

#### **5. export_data_to_csv**

```python
def export_data_to_csv(
    database: str,
    collection: str,
    date_column: str,
    start_date: datetime,
    output_filepath: str,
) -> None:
    """
    Main function to fetch data, process it, filter by date, and export to CSV.

    Parameters:
    ----------
    database : str
        The name of the MongoDB database.
    collection : str
        The name of the MongoDB collection.
    date_column : str
        The name of the date column to be converted and filtered.
    start_date : datetime
        The starting date for filtering the data.
    output_filepath : str
        The path to save the exported CSV file.

    Returns:
    -------
    None

    Raises:
    ------
    Exception
        If an error occurs during the data export process.
    """
    try:
        logging.info("Starting data export process...")

        # Step 1: Fetch data from MongoDB
        df = fetch_data_from_mongodb(database, collection)

        # Step 2: Process the data (grouping, resampling)
        processed_df = process_dataframe(df, date_column)

        # Step 3: Filter data from the start date onwards
        filtered_df = filter_by_date(processed_df, date_column, start_date)

        # Step 4: Export the filtered data to CSV
        export_dataframe_to_csv(filtered_df, output_filepath)

        logging.info("Data export process completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred during the export process: {e}")
        raise
```

- **Purpose**: Orchestrates the entire data export process by calling the other functions in sequence.
- **Error Handling**:
  - Logs and raises any exceptions that occur during the process.

---

### **Main Execution Block**

```python
if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    database_name = "deep-diver"
    collection_name = "boreport"
    date_col = "Probability 90% Date"
    output_file = "modeling_sales.csv"
    start_filter_date = datetime(2022, 1, 1)

    export_data_to_csv(
        database_name, collection_name, date_col, start_filter_date, output_file
    )
```

- **Purpose**: Provides an example of how to use the `export_data_to_csv` function.
- **Logging Configuration**: Sets the logging level to `INFO` to capture informational messages and errors.
- **Parameters**:
  - `database_name`: The name of the MongoDB database to connect to.
  - `collection_name`: The MongoDB collection to fetch data from.
  - `date_col`: The name of the date column in the data.
  - `output_file`: The file path for the exported CSV.
  - `start_filter_date`: The date from which to include data.

---

### **Additional Details**

#### **Error Handling**

- **General Exceptions**: Each function uses `try-except` blocks to catch exceptions.
- **Logging**: Errors are logged using the `logging` module with appropriate error messages.
- **Raising Exceptions**: After logging, exceptions are re-raised to ensure that the calling function is aware of the failure.

#### **Type Hints**

- **Function Parameters and Return Types**: All functions include type hints for parameters and return values to enhance code readability and facilitate static type checking.

#### **Modularity**

- **Atomic Functions**: Each function performs a single, well-defined task.
- **Reusability**: Functions can be reused in other scripts or extended for additional functionality.

#### **Dependencies on Custom Modules**

- The script relies on a custom `databases` module, which should include:
  - `create_connection(database: str, collection: str) -> Collection`: Connects to MongoDB and returns a collection object.
  - `convert_column_to_datetime(df: pd.DataFrame, column_name: str) -> pd.DataFrame`: Converts a specified column to datetime format in the DataFrame.

---

### **Usage Instructions**

1. **Ensure Dependencies Are Met**:
   - Install required packages: `pandas`.
   - Provide the custom `databases` module with the necessary functions.

2. **Configure Logging**:
   - Adjust the logging level if needed (e.g., `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`).

3. **Set Parameters**:
   - Update `database_name`, `collection_name`, `date_col`, `output_file`, and `start_filter_date` as needed.

4. **Run the Script**:
   - Execute the script directly, or import the functions into another script as needed.

---

### **Example Execution**

```shell
$ python your_script.py
INFO:root:Starting data export process...
INFO:root:Data successfully exported to modeling_sales.csv.
INFO:root:Data export process completed successfully.
```

---

### **Potential Enhancements**

- **Validation**:
  - Add more comprehensive data validation to check for data consistency.
- **Configuration File**:
  - Use a configuration file (e.g., YAML, JSON) to store parameters.
- **Command-Line Arguments**:
  - Use `argparse` to accept command-line arguments for flexibility.
- **Unit Tests**:
  - Implement unit tests for each function to ensure reliability.
- **Exception Classes**:
  - Define custom exception classes for more granular error handling.

---

### **Important Notes**

- **Data Privacy**: Ensure that exporting data complies with data privacy regulations.
- **Error Logging**: Sensitive information should not be logged.
- **Custom Module Availability**: The `databases` module must be accessible and correctly implemented for the script to function.

---

### **Conclusion**

This documentation provides a comprehensive overview of the script's functionality, including detailed explanations of each function, parameters, return types, and error handling mechanisms. By following this guide, you should be able to understand, maintain, and extend the script effectively.

---


## Events Data

### `events.py`
This script is designed to:
- Parse date strings from an Excel file, handling various date formats and ranges.
- Process multiple sheets from an Excel workbook, each representing a different year.
- Combine the processed data into a single DataFrame.
- Export the consolidated data to a CSV file.

The script includes functions to parse individual date strings, process dates from Excel sheets, and orchestrate the overall processing flow.

### Purpose
- Coordinates the processing of dates from multiple sheets (years).
- Combines the processed data into a single DataFrame.
- Exports the consolidated data to a CSV file.

### Function Logic
1. Define File Path:
- Sets the filepath to the location of the Excel workbook.

2. Process Each Year's Sheet:
- Calls process_dates for each sheet representing a different year (e.g., "2022", "2023", "2024").
- Stores the resulting DataFrames (df_2022, df_2023, df_2024).

3. Concatenate DataFrames:
- Uses pd.concat to combine the individual DataFrames into a single DataFrame full_df.

4. Export to CSV:
- Defines the output_path for the CSV file.
- Attempts to save full_df to the CSV file using to_csv.

5. Error Handling:
- Catches exceptions during the CSV export process.
- Prints an error message if an exception occurs.

6. Execution Check:
- The if __name__ == "__main__": block ensures that main() is called only when the script is run directly.

7. Error Handling
- Exception: Catches any exceptions during CSV export, prints an error message.

### `eventscalendar.py`

# **Script Documentation**

## **Overview**

This script is designed to:

- **Process** data from a CSV file by adding new columns and mapping existing data to categories and products.
- **Export** the processed DataFrame to a specified CSV file.

The script includes functions to map business units to categories, categories to products, process the data, and export the DataFrame to a CSV file. It includes type hints, detailed docstrings, and error handling to ensure robustness and maintainability.

---

## **Dependencies**

```python
import pandas as pd
from typing import Optional
```

- **pandas**: For data manipulation and handling DataFrames.
- **typing**: For type hinting (`Optional`).

---

## **Function Definitions**

### **1. `map_business_unit_to_category`**

```python
def map_business_unit_to_category(business_unit: str) -> str:
    """
    Maps a business unit to a corresponding category.

    Parameters
    ----------
    business_unit : str
        The name of the business unit.

    Returns
    -------
    str
        The category corresponding to the business unit.
    """
    if business_unit in ["Consumer", "Retail", "Retail (Consumer)", "Consumer & Retail"]:
        return "Fibre to the Home"
    elif business_unit == "Retail (SME)":
        return "Fibre to the Office"
    else:
        return "Enterprise, Wholesale, or Public Sector"
```

#### **Purpose**

- **Maps** the 'Business Unit' values to corresponding 'category' values based on predefined mappings.

#### **Parameters**

- `business_unit` (`str`): The name of the business unit.

#### **Returns**

- `str`: The category corresponding to the business unit.

#### **Logic**

- Checks if `business_unit` matches specific strings and returns the corresponding category:
  - `"Consumer"`, `"Retail"`, `"Retail (Consumer)"`, `"Consumer & Retail"` → `"Fibre to the Home"`
  - `"Retail (SME)"` → `"Fibre to the Office"`
  - Any other value → `"Enterprise, Wholesale, or Public Sector"`

---

### **2. `map_category_to_product`**

```python
def map_category_to_product(category: str) -> str:
    """
    Maps a category to a corresponding product.

    Parameters
    ----------
    category : str
        The name of the category.

    Returns
    -------
    str
        The product corresponding to the category.
    """
    if category == "Fibre to the Home":
        return "All Home Fibre Speeds"
    elif category == "Fibre to the Office":
        return "All Business Fibre Speeds"
    else:
        return "Enterprise-grade products"
```

#### **Purpose**

- **Maps** the 'category' values to corresponding 'product' values based on predefined mappings.

#### **Parameters**

- `category` (`str`): The name of the category.

#### **Returns**

- `str`: The product corresponding to the category.

#### **Logic**

- Checks if `category` matches specific strings and returns the corresponding product:
  - `"Fibre to the Home"` → `"All Home Fibre Speeds"`
  - `"Fibre to the Office"` → `"All Business Fibre Speeds"`
  - Any other value → `"Enterprise-grade products"`

---

### **3. `process_time_data`**

```python
def process_time_data(filepath: str) -> pd.DataFrame:
    """
    Processes the Time data from a CSV file and returns a DataFrame with added columns and mappings.

    Parameters
    ----------
    filepath : str
        The file path to the CSV file containing the data.

    Returns
    -------
    pd.DataFrame
        The processed DataFrame with added 'brand', 'company', 'category', and 'product' columns.

    Raises
    ------
    FileNotFoundError
        If the specified file does not exist.
    KeyError
        If the required 'Business Unit' column is missing in the data.
    Exception
        For any other exceptions that may occur during processing.
    """
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(filepath)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"File not found: {filepath}") from e
    except Exception as e:
        raise Exception(f"An error occurred while reading the file '{filepath}': {e}") from e

    # Check if 'Business Unit' column exists
    if "Business Unit" not in df.columns:
        raise KeyError("The required 'Business Unit' column is missing in the data.")

    try:
        # Add 'brand' and 'company' columns
        df["brand"] = "Time"    # Specify Brand
        df["company"] = "Time"  # Specify Company

        # Map 'Business Unit' to 'category'
        df["category"] = df["Business Unit"].apply(map_business_unit_to_category)

        # Map 'category' to 'product'
        df["product"] = df["category"].apply(map_category_to_product)

    except Exception as e:
        raise Exception(f"An error occurred during data processing: {e}") from e

    return df
```

#### **Purpose**

- **Processes** the data from the CSV file by adding new columns and mapping existing data to categories and products.

#### **Parameters**

- `filepath` (`str`): The file path to the CSV file containing the data.

#### **Returns**

- `pd.DataFrame`: The processed DataFrame with added 'brand', 'company', 'category', and 'product' columns.

#### **Raises**

- `FileNotFoundError`: If the specified file does not exist.
- `KeyError`: If the required 'Business Unit' column is missing in the data.
- `Exception`: For any other exceptions that may occur during processing.

#### **Function Logic**

1. **Read CSV File**:
   - Uses `pd.read_csv` to read the data into a DataFrame.
   - Catches `FileNotFoundError` and general exceptions, providing informative error messages.

2. **Check for 'Business Unit' Column**:
   - Ensures the 'Business Unit' column exists in the DataFrame.
   - Raises `KeyError` if the column is missing.

3. **Add 'brand' and 'company' Columns**:
   - Adds new columns 'brand' and 'company' with the value `"Time"`.

4. **Apply Mapping Functions**:
   - Maps 'Business Unit' to 'category' using `map_business_unit_to_category`.
   - Maps 'category' to 'product' using `map_category_to_product`.

5. **Error Handling**:
   - Catches any exceptions during processing and raises them with informative messages.

---

### **4. `export_dataframe_to_csv`**

```python
def export_dataframe_to_csv(df: pd.DataFrame, output_filepath: str) -> None:
    """
    Exports the DataFrame to a CSV file at the specified file path.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame to export.
    output_filepath : str
        The file path where the CSV will be saved.

    Raises
    ------
    IOError
        If an I/O error occurs during file writing.
    Exception
        For any other exceptions that may occur during the export.
    """
    try:
        df.to_csv(output_filepath, index=False)
        print(f"Data successfully exported to {output_filepath}")
    except IOError as e:
        raise IOError(f"An I/O error occurred while writing to file '{output_filepath}': {e}") from e
    except Exception as e:
        raise Exception(f"An error occurred during exporting to CSV: {e}") from e
```

#### **Purpose**

- **Exports** the processed DataFrame to a CSV file at the specified file path.

#### **Parameters**

- `df` (`pd.DataFrame`): The DataFrame to export.
- `output_filepath` (`str`): The file path where the CSV will be saved.

#### **Raises**

- `IOError`: If an I/O error occurs during file writing.
- `Exception`: For any other exceptions that may occur during the export.

#### **Function Logic**

1. **Export DataFrame to CSV**:
   - Uses `df.to_csv(output_filepath, index=False)` to write the DataFrame to a CSV file without including the index.

2. **Success Message**:
   - Prints a confirmation message indicating successful export.

3. **Error Handling**:
   - Catches `IOError` exceptions specific to I/O operations.
   - Catches general exceptions and raises them with informative messages.

---

### **5. `main` Function**

```python
def main():
    """
    Main function to process the data and export it to a CSV file.
    """
    input_filepath = "../data/interim/001_20241022_events.csv"
    output_filepath = "../data/processed/001_izzaz_20241022_events.csv"

    try:
        # Process the data
        processed_df = process_time_data(input_filepath)

        # Export the processed data to CSV
        export_dataframe_to_csv(processed_df, output_filepath)

    except Exception as e:
        print(f"Error: {e}")
```

#### **Purpose**

- **Coordinates** the data processing and exporting steps.
- **Defines** the input and output file paths.
- **Handles** exceptions that may occur during processing and exporting.

#### **Function Logic**

1. **Set File Paths**:
   - `input_filepath`: Path to the input CSV file.
   - `output_filepath`: Path where the processed CSV file will be saved.

2. **Process Data**:
   - Calls `process_time_data(input_filepath)` to process the data.

3. **Export Data**:
   - Calls `export_dataframe_to_csv(processed_df, output_filepath)` to save the processed data.

4. **Error Handling**:
   - Catches any exceptions raised during processing or exporting and prints an error message.

---

## **Execution Block**

```python
if __name__ == "__main__":
    main()
```

- Ensures that the `main` function is called only when the script is run directly, not when imported as a module.

---

## **Usage Instructions**

1. **Ensure Dependencies Are Installed**:

   Install required packages:

   ```bash
   pip install pandas
   ```

2. **Verify File Paths**:

   - **Input File**: Ensure that the input file exists at `"../data/interim/001_20241022_events.csv"`.
   - **Output Directory**: Ensure that the directory `"../data/processed/"` exists and is writable.

3. **Run the Script**:

   Execute the script from the command line or an IDE:

   ```bash
   python script_name.py
   ```

4. **Check Output**:

   After successful execution, the processed data should be available at `"../data/processed/001_izzaz_20241022_events.csv"`.

---

## **Testing the Functions**

You can test the code using a sample CSV file:

```python
# Sample data for testing
sample_data = {
    "Business Unit": ["Consumer", "Retail", "Retail (SME)", "Enterprise", "Other"],
    "Some Other Column": [1, 2, 3, 4, 5]
}
test_df = pd.DataFrame(sample_data)

# Save sample data to a CSV file
test_input_filepath = "test_input.csv"
test_df.to_csv(test_input_filepath, index=False)

# Update the file paths in the main function for testing
def main():
    """
    Main function to process the data and export it to a CSV file.
    """
    input_filepath = "test_input.csv"
    output_filepath = "test_output.csv"

    try:
        # Process the data
        processed_df = process_time_data(input_filepath)

        # Export the processed data to CSV
        export_dataframe_to_csv(processed_df, output_filepath)

        # Print the processed DataFrame
        print(processed_df)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
```

**Expected Output:**

```shell
Data successfully exported to test_output.csv
  Business Unit  Some Other Column brand company  \
0      Consumer                  1  Time    Time   
1        Retail                  2  Time    Time   
2  Retail (SME)                  3  Time    Time   
3    Enterprise                  4  Time    Time   
4         Other                  5  Time    Time   

                                category                   product  
0                    Fibre to the Home      All Home Fibre Speeds  
1                    Fibre to the Home      All Home Fibre Speeds  
2                   Fibre to the Office   All Business Fibre Speeds  
3  Enterprise, Wholesale, or Public Sector  Enterprise-grade products  
4  Enterprise, Wholesale, or Public Sector  Enterprise-grade products  
```

---

## **Additional Notes**

- **Data Integrity**: Ensure that the input CSV file contains the 'Business Unit' column with appropriate values.

- **Directory Permissions**: Verify that you have write permissions for the output directory to avoid `IOError`.

- **Logging**: For production environments, consider using the `logging` module instead of `print` statements for better control over logging levels and outputs.

- **Extensibility**: You can extend the mapping functions to handle additional business units or categories as needed.

---

## **Potential Enhancements**

- **Logging**:

  - Replace `print` statements with the `logging` module for better logging practices.

- **Error Handling Improvements**:

  - Implement more granular exception handling.
  - Provide more informative error messages or logs.

- **Unit Tests**:

  - Write unit tests for each function to ensure they handle various inputs correctly.

- **Configuration File**:

  - Externalize file paths and other configurations to a separate file (e.g., YAML, JSON).

- **Command-Line Arguments**:

  - Use `argparse` to accept file paths and other parameters from the command line.

---

## **Conclusion**

By adding the `export_dataframe_to_csv` function, the code now fully processes the data and exports it to the specified CSV file, fulfilling the requirements. The code is modular, includes type hints, detailed docstrings, and comprehensive error handling, making it robust and maintainable.

---

**Feel free to reach out if you have any questions or need further assistance!**