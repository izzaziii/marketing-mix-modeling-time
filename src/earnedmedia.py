import pandas as pd
from typing import List
from datetime import datetime


def read_data_from_earned_media_sheets(
    filepath: str, sheet_names: List[str]
) -> pd.DataFrame:
    """
    Reads multiple sheets from an Excel file and concatenates them into a single DataFrame.

    Args:
        filepath (str): The full path to the Excel file containing the sheets.
        sheet_names (List[str]): A list of sheet names to be read and concatenated.

    Returns:
        pd.DataFrame: A single DataFrame containing the concatenated data from all sheets.
    """
    # Initialize an empty DataFrame to hold the concatenated data
    df_total = pd.DataFrame()

    # Loop through each sheet and append it to the total DataFrame
    for sheet in sheet_names:
        try:
            # Attempt to read each sheet into a DataFrame
            df = pd.read_excel(filepath, sheet_name=sheet)

            # Concatenate the current sheet's DataFrame to df_total
            df_total = pd.concat([df_total, df], ignore_index=True)

        except Exception as e:
            # Log the error with the sheet name and error message
            print(f"Error reading sheet '{sheet}': {e}")

    return df_total


def main() -> None:
    """
    Main function to read data from multiple sheets and save it to a CSV file with today's date in the filename.
    """
    filepath = "C:\\Users\\izzaz\\Documents\\1 Projects\\T - Onboarding of Mutinex MMM\\data\\interim\\001_20241024_earnedmedia_2024.xlsx"
    sheets = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
    ]

    # Read data from the earned media sheets
    df = read_data_from_earned_media_sheets(filepath=filepath, sheet_names=sheets)

    # Get today's date in YYYY-MM-DD format
    today = datetime.today().strftime("%Y-%m-%d")

    # Dynamically generate the filename with today's date
    output_filepath = f"../data/interim/002_{today}_earnedmedia2024.csv"

    # Save the concatenated DataFrame to a CSV file with today's date
    df.to_csv(output_filepath, index=False)


if __name__ == "__main__":
    main()
