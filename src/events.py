import pandas as pd
from typing import Optional


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
    if business_unit in [
        "Consumer",
        "Retail",
        "Retail (Consumer)",
        "Consumer & Retail",
    ]:
        return "Fibre to the Home"
    elif business_unit == "Retail (SME)":
        return "Fibre to the Office"
    else:
        return "Enterprise, Wholesale, or Public Sector"


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
        raise Exception(
            f"An error occurred while reading the file '{filepath}': {e}"
        ) from e

    # Check if 'Business Unit' column exists
    if "Business Unit" not in df.columns:
        raise KeyError("The required 'Business Unit' column is missing in the data.")

    try:
        # Add 'brand' and 'company' columns
        df["brand"] = "Time"  # Specify Brand
        df["company"] = "Time"  # Specify Company

        # Map 'Business Unit' to 'category'
        df["category"] = df["Business Unit"].apply(map_business_unit_to_category)

        # Map 'category' to 'product'
        df["product"] = df["category"].apply(map_category_to_product)

    except Exception as e:
        raise Exception(f"An error occurred during data processing: {e}") from e

    return df


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
        raise IOError(
            f"An I/O error occurred while writing to file '{output_filepath}': {e}"
        ) from e
    except Exception as e:
        raise Exception(f"An error occurred during exporting to CSV: {e}") from e


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


if __name__ == "__main__":
    main()
