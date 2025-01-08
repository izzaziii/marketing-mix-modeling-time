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


def process_metrics(df: pd.DataFrame) -> list[pd.DataFrame]:
    metric_columns = [
        "Impressions",
        "Media cost (Advertiser)",
        "Total media cost (Advertiser)",
        "Revenue (Advertiser)",
    ]
    grouped_columns = [
        "Advertiser",
        "Advertiser ID",
        "Advertiser currency",
        "Insertion order",
        "Line item",
        "Region",
        "Line item type",
        "Ad position",
        "Creative",
    ]
    dataframes_list: list = []
    for col in metric_columns:
        df0 = (
            df.astype({"Date": "datetime64[ns]"})
            .set_index("Date")
            .groupby(grouped_columns)
            .resample("W-SUN")[col]
            .sum()
            .reset_index()
        )
        dataframes_list.append(df0)
    return dataframes_list


def get_export_details(
    file_version: str,
    processed_filepath: str = "C:\\Users\\izzaz\\Documents\\1 Projects\\T - Onboarding of Mutinex MMM\\data\\processed",
    name_responsible: str = "izzaz",
    dataset_name: str = "paidmedia",
    dataset_type: str = "dv360",
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
    filepath: str = "../data/raw/20250106_DV360.csv"

    df: pd.DataFrame = read_file(filepath)
    dataframes_list: list[pd.DataFrame] = process_metrics(df)
    weekly_impr: pd.DataFrame = dataframes_list[0]
    weekly_mediacost: pd.DataFrame = dataframes_list[1]
    weekly_totalmediacost: pd.DataFrame = dataframes_list[2]
    weekly_revenue: pd.DataFrame = dataframes_list[3]
    merged_df: pd.DataFrame = pd.concat(
        [
            weekly_impr,
            weekly_mediacost["Media cost (Advertiser)"],
            weekly_totalmediacost["Total media cost (Advertiser)"],
            weekly_revenue["Revenue (Advertiser)"],
        ],
        axis=1,
    )
    columns: list[str] = [
        "Advertiser",
        "Advertiser ID",
        "Advertiser currency",
        "Insertion order",
        "Line item",
        "Region",
        "Line item type",
        "Ad position",
        "Creative",
        "Date",
        "Impressions",
        "Media cost",
        "Total media cost",
        "Revenue",
    ]
    merged_df.columns = columns

    full_filepath: str = get_export_details("002")
    export_to_csv(merged_df, full_filepath)


if __name__ == "__main__":
    main()
