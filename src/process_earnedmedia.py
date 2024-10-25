import pandas as pd
from typing import Union
from datetime import datetime


def process_earned_media(filepath: str) -> pd.DataFrame:
    """
    Processes an earned media CSV file by transforming dates, adding brand and company
    information, and unpivoting the 'Ad Value' and 'PR Value' columns.

    Args:
        filepath (str): The path to the CSV file.

    Returns:
        pd.DataFrame: The processed DataFrame with unpivoted columns and additional transformations.

    Raises:
        FileNotFoundError: If the provided file path does not exist.
        ValueError: If required columns are missing from the input file.
    """
    try:
        # Load the CSV file into a DataFrame
        df = pd.read_csv(filepath)

        # Ensure the required columns exist in the DataFrame
        required_columns = [
            "Date",
            "No",
            "Channel",
            "Title",
            "category",  # Manually added
            "product",  # Manually added
            "Media Name",
            "Link URL",
            "Sentiment",
        ]
        if not all(col in df.columns for col in required_columns):
            raise ValueError(
                f"The DataFrame is missing one or more required columns: {required_columns}"
            )

        # Process Dates into datetime. Dates are actual dates, not resampled weekly.
        df["Date"] = df["Date"].astype("datetime64[ns]")
        if df["Date"].isnull().any():
            print("Warning: Some 'Date' values could not be converted to datetime.")

        # Add brand and company columns
        df["brand"] = "Time"
        df["company"] = "Time"

        # Unpivot the DataFrame: melt 'Ad Value' and 'PR Value' into 'earned_category' and 'metric_value'
        unpivot_df = pd.melt(
            df,
            id_vars=[
                "No",
                "Channel",
                "Date",
                "Title",
                "category",
                "product",
                "Media Name",
                "Link URL",
                "Sentiment",
                "brand",
                "company",
            ],
            var_name="metric_name",
            value_name="metric_value",
        )

        # Return the transformed DataFrame
        return unpivot_df.rename(
            columns={"variable": "metric_name", "value": "metric_value"}
        )

    except FileNotFoundError:
        raise FileNotFoundError(f"The file at {filepath} was not found.")
    except pd.errors.EmptyDataError:
        raise ValueError(f"The file at {filepath} is empty or corrupted.")
    except Exception as e:
        raise RuntimeError(f"An unexpected error occurred: {str(e)}")


def export_to_csv(df: pd.DataFrame, output_filepath: str) -> None:
    """
    Exports the provided DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): The DataFrame to export.
        output_filepath (str): The output file path where the CSV will be saved.
    """
    try:
        df.to_csv(output_filepath, index=False)
        print(f"Data successfully exported to {output_filepath}")
    except Exception as e:
        raise RuntimeError(f"Failed to export data to {output_filepath}: {str(e)}")


def main(filepath: str, output_filepath: str) -> None:
    """
    Main function to process the earned media CSV and export the result to a new CSV file.

    Args:
        filepath (str): The input file path to the earned media CSV.
        output_filepath (str): The output file path where the processed CSV will be saved.
    """
    try:
        # Process the earned media data
        processed_df = process_earned_media(filepath)

        # Export the processed data to a CSV file
        export_to_csv(processed_df, output_filepath)

    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    today_date = datetime.now().strftime("%Y-%m-%d %H%M%S")

    # Define input and output file paths
    input_filepath = "../data/interim/002_2024-10-24_earnedmedia2024.csv"
    output_filepath = (
        f"../data/processed/001_izzaz_{today_date}_earned_media_processed.csv"
    )

    # Run the main function
    main(input_filepath, output_filepath)
