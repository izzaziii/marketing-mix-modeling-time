import pandas as pd
import re
from typing import Tuple, Optional
import dateparser


def parse_dates(
    date_str: str, current_year: str
) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    """
    Parses a date string and returns the start and end dates.

    Parameters:
    ----------
    date_str : str
        The date string to parse.
    current_year : str
        The current year to append if the year is missing in the date string.

    Returns:
    -------
    Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]
        A tuple containing the start date and end date as pandas Timestamps,
        or (None, None) if parsing fails.
    """
    # Remove extra spaces and normalize the string
    date_str = re.sub(r"\s+", " ", date_str.strip())

    # Initialize start_date and end_date
    start_date = None
    end_date = None

    try:
        # Check if it's a date range
        if "-" in date_str:
            # Split on '-'
            parts = re.split(r"\s*-\s*", date_str)

            # Clean parts
            parts = [part.strip() for part in parts if part.strip()]

            if len(parts) == 2:
                start_str, end_str = parts

                # If start_str does not contain month, copy from end_str
                months_regex = (
                    r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
                    r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|"
                    r"Dec(?:ember)?)"
                )
                if not re.search(months_regex, start_str, re.IGNORECASE):
                    month_match = re.search(months_regex, end_str, re.IGNORECASE)
                    if month_match:
                        start_str += " " + month_match.group(0)

                # Append year if missing
                if not re.search(r"\b(20\d{2})\b", start_str):
                    start_str += " " + str(current_year)
                if not re.search(r"\b(20\d{2})\b", end_str):
                    end_str += " " + str(current_year)

                # Parse dates
                start_date = dateparser.parse(start_str)
                end_date = dateparser.parse(end_str)

                # Handle parsing failures
                if not start_date or not end_date:
                    start_date = None
                    end_date = None
                else:
                    # Adjust for date ranges that span over to the next month or year
                    if end_date < start_date:
                        # If end_date is before start_date, add one year
                        end_date = end_date.replace(year=end_date.year + 1)
            else:
                # If splitting doesn't result in two parts, attempt to parse as a single date
                start_date = dateparser.parse(date_str)
                end_date = start_date
        else:
            # Single date
            if not re.search(r"\b(20\d{2})\b", date_str):
                date_str += " " + str(current_year)

            # Parse date
            start_date = dateparser.parse(date_str)
            end_date = start_date

        # Convert to pandas Timestamps
        if start_date:
            start_date = pd.Timestamp(start_date)
        if end_date:
            end_date = pd.Timestamp(end_date)

    except Exception as e:
        print(f"Error parsing date string '{date_str}': {e}")
        start_date = None
        end_date = None

    return start_date, end_date


def process_dates(filepath: str, sheet_name: str) -> pd.DataFrame:
    """
    Processes the dates from an Excel sheet and returns a DataFrame with parsed dates.

    Parameters:
    ----------
    filepath : str
        The file path to the Excel workbook.
    sheet_name : str
        The name of the sheet to process.

    Returns:
    -------
    pd.DataFrame
        A DataFrame with the original dates and parsed 'Start Date' and 'End Date' columns.
    """
    try:
        df = pd.read_excel(filepath, sheet_name=sheet_name)
    except FileNotFoundError:
        print(f"File not found: {filepath}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading Excel file '{filepath}': {e}")
        return pd.DataFrame()

    # Ensure 'Date' column exists
    if "Date" not in df.columns:
        print(f"'Date' column not found in sheet '{sheet_name}'")
        return pd.DataFrame()

    # Convert 'Date' column to string
    df["Date"] = df["Date"].astype(str)

    # Apply the parsing function
    df[["Start Date", "End Date"]] = df["Date"].apply(
        lambda x: pd.Series(parse_dates(x, sheet_name))
    )

    # Drop rows where parsing failed
    df = df.dropna(subset=["Start Date", "End Date"])

    # Format dates
    df["Start Date"] = df["Start Date"].dt.strftime("%Y-%m-%d")
    df["End Date"] = df["End Date"].dt.strftime("%Y-%m-%d")

    return df


def main():
    """
    Main function to process dates from multiple Excel sheets and save to a CSV file.
    """
    filepath = r"C:\Users\izzaz\Documents\1 Projects\T - Onboarding of Mutinex MMM\data\raw\Events Team Calendar 2022 - 2024.xlsx"

    # Process each year's sheet
    df_2022 = process_dates(filepath, "2022")
    df_2023 = process_dates(filepath, "2023")
    df_2024 = process_dates(filepath, "2024")

    # Concatenate dataframes
    full_df = pd.concat([df_2022, df_2023, df_2024], axis=0, ignore_index=True)

    # Save to CSV
    output_path = "../data/interim/events.csv"
    try:
        full_df.to_csv(output_path, index=False)
        print(f"Successfully saved to {output_path}")
    except Exception as e:
        print(f"Error saving CSV file '{output_path}': {e}")


if __name__ == "__main__":
    main()
