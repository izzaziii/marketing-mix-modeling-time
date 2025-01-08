import pandas as pd
from os import path
from datetime import datetime


def read_file(filepath: str) -> pd.DataFrame:
    """Reads csv file of twitter export

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


def process_twitter_cost(df: pd.DataFrame) -> pd.DataFrame:
    "Processes the cost"
    return (
        df.fillna(0)
        .astype({"Date": "datetime64[ns]"})
        .set_index("Date")
        .groupby(
            [
                "Campaign",
                "Campaign currency",
                "Campaign objective",
                "Goal",
                "Line item placements",
                "City",
                "Promoted tweet media type",
            ]
        )
        .resample("W-SUN")
        .Cost.sum()
        .reset_index()
    )


def process_twitter_impr(df: pd.DataFrame) -> pd.DataFrame:
    """Processes the impressions"""
    return (
        df.fillna(0)
        .astype({"Date": "datetime64[ns]"})
        .set_index("Date")
        .groupby(
            [
                "Campaign",
                "Campaign currency",
                "Campaign objective",
                "Goal",
                "Line item placements",
                "City",
                "Promoted tweet media type",
            ]
        )
        .resample("W-SUN")
        .Impressions.sum()
        .reset_index()
    )


def get_export_details(
    file_version: str,
    processed_filepath: str = "C:\\Users\\izzaz\\Documents\\1 Projects\\T - Onboarding of Mutinex MMM\\data\\processed",
    name_responsible: str = "izzaz",
    dataset_name: str = "paidmedia",
    dataset_type: str = "twitter",
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
    """Main logic"""
    filename: str = "../data/raw/20250106_twitter.csv"
    df: pd.DataFrame = read_file(filename)
    weekly_impressions: pd.DataFrame = process_twitter_impr(df)
    weekly_cost: pd.DataFrame = process_twitter_cost(df)
    merged_df = pd.concat([weekly_impressions, weekly_cost["Cost"]], axis=1)

    full_filepath: str = get_export_details("002")
    export_to_csv(merged_df, full_filepath)


if __name__ == "__main__":
    main()
