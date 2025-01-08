import pandas as pd
from datetime import datetime
from os import path


def read_file(filepath: str) -> pd.DataFrame:
    """Reads csv file of tiktok export

    Args:
        filepath (str): File path to the csv file

    Returns:
        pd.DataFrame: Returns a dataframe
    """
    df: pd.DataFrame = pd.read_csv(filepath)
    if not df.empty:
        return df
    else:
        print("Empty dataframe!")


def process_tiktok_data(df: pd.DataFrame) -> pd.DataFrame:
    """Processes the data for use in the MMM. Steps:
    - Sets date column to datetime and the index
    - Groups by Campaign name, campaign objective type, campaign type, ad group type, optimization goal
    - resamples by W-SUN
    - Sums up Cost, reach, Impressions
    - Resets index

    Args:
        df (pd.DataFrame): DataFrame

    Returns:
        pd.DataFrame: DataFrame
    """
    try:
        return (
            df.astype({"Date": "datetime64[ns]"})
            .set_index("Date")
            .groupby(
                [
                    "Campaign name",
                    "Campaign objective type",
                    "Campaign type",
                    "Ad group name",
                    "Optimization goal",
                ]
            )
            .resample("W-SUN")[["Cost", "Reach", "Impressions"]]
            .sum()
            .reset_index()
        )
    except Exception as e:
        print(f"Error: {e}")


def get_export_details(
    file_version: str,
    processed_filepath: str = "C:\\Users\\izzaz\\Documents\\1 Projects\\T - Onboarding of Mutinex MMM\\data\\processed",
    name_responsible: str = "izzaz",
    dataset_name: str = "paidmedia",
    dataset_type: str = "tiktok",
) -> str:
    """_summary_

    Args:
        file_version (str): Number version, ie 001, 002, 003
        processed_filepath (str, optional): Processed data folder. Defaults to "C:\\Users\\izzaz\\Documents\\1 Projects\\T - Onboarding of Mutinex MMM\\data\\processed".
        name_responsible (str, optional): Name of person generating this data. Defaults to "izzaz".
        dataset_name (str, optional): Label of data. Defaults to "paidmedia".
        dataset_type (str, optional): breakdown category of data. Defaults to "tiktok".

    Returns:
        str: Returns the full filepath
    """
    today: str = datetime.now().strftime("%Y%m%d")
    full_filename: str = (
        f"{file_version}_{name_responsible}_{today}_{dataset_name}_{dataset_type}.csv"
    )
    full_filepath = path.join(processed_filepath, full_filename)
    return full_filepath


def export_to_csv(df: pd.DataFrame, full_filepath: str) -> None:
    """Exports dataframe to csv in the processed folder

    Args:
        df (pd.DataFrame): DataFrame
        full_filepath (str): Processed folder
    """
    try:
        df.to_csv(full_filepath, index=False)
        print(f"File exported to {full_filepath}")
    except Exception as e:
        print(f"Error: {e}")


def main():
    """Main Logic"""
    filepath: str = "../data/raw/20250106_tiktok.csv"
    df: pd.DataFrame = read_file(filepath)
    processed_df: pd.DataFrame = process_tiktok_data(df)
    full_filepath: str = get_export_details("002")
    export_to_csv(processed_df, full_filepath)


if __name__ == "__main__":
    main()
