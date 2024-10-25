from connection import create_connection
import pandas as pd
from datetime import datetime
import logging


def convert_column_to_datetime(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Converts the specified column in the DataFrame from UNIX timestamp format to datetime.

    Args:
        df (pd.DataFrame): The DataFrame containing the column to convert.
        column (str): The name of the column to convert to datetime.

    Returns:
        pd.DataFrame: The DataFrame with the specified column converted to datetime.
    """
    try:
        df[column] = pd.to_datetime(
            df[column], unit="ms"
        )  # Convert UNIX timestamp to datetime
        return df
    except KeyError:
        print(f"Column {column} does not exist in the DataFrame.")
        return df
    except Exception as e:
        print(f"An error occurred while converting {column} to datetime: {e}")
        return df


def fetch_data_from_mongodb(database: str, collection: str) -> pd.DataFrame:
    """
    Fetch data from a MongoDB collection and return it as a DataFrame.

    Parameters:
    - database: str: The name of the MongoDB database.
    - collection: str: The name of the MongoDB collection.

    Returns:
    - pd.DataFrame: DataFrame containing the fetched data.
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


def process_dataframe(df: pd.DataFrame, date_column: str) -> pd.DataFrame:
    """
    Process the DataFrame by converting the date column, grouping, and resampling.

    Parameters:
    - df: pd.DataFrame: The raw DataFrame.
    - date_column: str: The name of the date column to be converted to datetime.

    Returns:
    - pd.DataFrame: Processed DataFrame with weekly counts, grouped and resampled.
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
            "Funn Monthcontractperiod",
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
            .groupby(
                [
                    " Channel",
                    "Funnel Fact.Package",
                    "Blk State",
                    "Blk Cluster",
                    "Funn Monthcontractperiod",
                ]
            )
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


def filter_by_date(
    df: pd.DataFrame, date_column: str, start_date: datetime
) -> pd.DataFrame:
    """
    Filter the DataFrame to include only rows with dates on or after the start date.

    Parameters:
    - df: pd.DataFrame: The DataFrame to filter.
    - date_column: str: The name of the date column to filter on.
    - start_date: datetime: The starting date for filtering.

    Returns:
    - pd.DataFrame: Filtered DataFrame.
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


def remove_zero_value_columns(
    df: pd.DataFrame, column_to_filter: str = "Funnel SO No"
) -> pd.DataFrame:
    return df.loc[df[column_to_filter] > 0]


def export_dataframe_to_csv(df: pd.DataFrame, output_filepath: str) -> None:
    """
    Export the DataFrame to a CSV file.

    Parameters:
    - df: pd.DataFrame: The DataFrame to export.
    - output_filepath: str: The path to save the CSV file.

    Returns:
    - None
    """
    try:
        df.to_csv(output_filepath, index=False)
        logging.info(f"Data successfully exported to {output_filepath}.")
    except Exception as e:
        logging.error(f"Error exporting DataFrame to CSV: {e}")
        raise


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
    - database: str: The name of the MongoDB database.
    - collection: str: The name of the MongoDB collection.
    - date_column: str: The name of the date column to be converted and filtered.
    - start_date: datetime: The starting date for filtering the data.
    - output_filepath: str: The path to save the exported CSV file.

    Returns:
    - None
    """
    try:
        logging.info("Starting data export process...")

        # Step 1: Fetch data from MongoDB
        df = fetch_data_from_mongodb(database, collection)

        # Step 2: Process the data (grouping, resampling)
        processed_df = process_dataframe(df, date_column)

        # Step 3: Filter data from 2022 onwards
        filtered_df = filter_by_date(processed_df, date_column, start_date)

        # Step 3a: Remove 0 value rows from dataframe
        no_zero_df = remove_zero_value_columns(filtered_df)

        # Step 4: Export the filtered data to CSV
        export_dataframe_to_csv(no_zero_df, output_filepath)

        logging.info("Data export process completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred during the export process: {e}")
        raise


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)
    database_name = "deep-diver"
    collection_name = "boreport"
    date_col = "Probability 90% Date"
    today_date = datetime.now().strftime("%Y-%m-%d %H%M%S")
    output_file = f"../data/processed/002_izzaz_{today_date}_sales.csv"
    start_filter_date = datetime(2022, 1, 1)

    export_data_to_csv(
        database_name, collection_name, date_col, start_filter_date, output_file
    )
