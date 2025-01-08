import pandas as pd
from os import path
import chardet
from datetime import datetime


def generate_filename(
    version: str, name_responsible: str, folder_path: str, data: str = "ownedmedia"
) -> str:
    """
    Generate a file path with a standardized naming convention.

    Parameters
    ----------
    version : str
        Version number or tag for the file (e.g., "001", "002").
    name_responsible : str
        Name of the person responsible for the export.
    folder_path : str
        The directory in which to place the resulting file.
    data : str, optional
        An extra string to describe the file contents, by default "ownedmedia".

    Returns
    -------
    str
        A string representing the full path (folder + filename).
    """
    today: str = datetime.now().strftime("%Y%m%d")
    export_filename: str = f"{version}_{name_responsible}_{today}_{data}.csv"
    filename: str = path.join(folder_path, export_filename)
    return filename


def find_encoding(filepath: str) -> str:
    """
    Detect the text encoding of a given file.

    Parameters
    ----------
    filepath : str
        The path to the file for which we want to detect encoding.

    Returns
    -------
    str
        The encoding detected by chardet.
    """
    try:
        with open(filepath, "rb") as file:
            result = chardet.detect(file.read())
        return result["encoding"]
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {filepath}")
    except PermissionError:
        raise PermissionError(f"No permission to read file: {filepath}")
    except Exception as e:
        raise Exception(f"Error reading file {filepath}: {e}")


def process_dataframe(filepath: str, metric: str, encoding: str) -> pd.DataFrame:
    """
    Read and clean a CSV file to return a dataframe with a date index and one metric column.

    Parameters
    ----------
    filepath : str
        The full path of the CSV file.
    metric : str
        The name of the metric to label the second column.
    encoding : str
        The file encoding determined by chardet.

    Returns
    -------
    pd.DataFrame
        A dataframe with 'Date' as its index and one column named after the metric.
    """
    try:
        df = pd.read_csv(filepath, encoding=encoding)
        # Remove the first two rows (often headers or extraneous data)
        df = df.iloc[2:]
        # Renaming columns: Date and Metric
        df.columns = ["Date", metric]

        # Clean up the Date column
        df["Date"] = df["Date"].str.split("T", expand=True)[0]
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        # Convert metric column to int
        df[metric] = df[metric].astype("int64")

        # Set Date as index
        df.set_index("Date", inplace=True)

        return df

    except pd.errors.EmptyDataError:
        raise ValueError(f"The file is empty or corrupted: {filepath}")
    except pd.errors.ParserError as e:
        raise ValueError(f"Parsing error for file {filepath}: {e}")
    except Exception as e:
        raise Exception(f"Unexpected error while processing {filepath}: {e}")


def combine_dataframes(pathnames: list[str], filenames: list[str]) -> pd.DataFrame:
    """
    Combine multiple CSV files into a single dataframe where each metric is a column.

    Parameters
    ----------
    pathnames : list[str]
        A list of full file paths to the CSV files to be combined.
    filenames : list[str]
        A list of metrics corresponding to each file (used for column naming).

    Returns
    -------
    pd.DataFrame
        A dataframe with Date as index and columns for each metric.
    """
    dataframes: list[pd.DataFrame] = []
    for filepath, metric in zip(pathnames, filenames):
        encoding = find_encoding(filepath)
        df = process_dataframe(filepath, metric, encoding)
        dataframes.append(df)

    try:
        combined_df = pd.concat(dataframes, axis=1)
    except ValueError as e:
        raise ValueError(f"Error concatenating dataframes: {e}")

    return combined_df


def group_and_resample(
    df: pd.DataFrame, channel: str, campaign: str, freq: str = "W-SUN"
) -> pd.DataFrame:
    """
    Add channel and campaign columns, then group and resample the dataframe by a specified frequency.

    Parameters
    ----------
    df : pd.DataFrame
        The initial dataframe with metric columns and a Date index.
    channel : str
        The name of the channel (e.g., 'facebook', 'instagram').
    campaign : str
        The campaign name or descriptor (e.g., 'Time Internet', 'profile page').
    freq : str, optional
        Frequency code for resampling, by default "W-SUN" (weekly ending on Sunday).

    Returns
    -------
    pd.DataFrame
        A resampled dataframe aggregated by sum for each metric, with additional columns
        for 'channel' and 'campaign'.
    """
    try:
        metrics = ["Follows", "Interactions", "Link clicks", "Reach", "Visits"]

        # Add channel and campaign info
        df = df.assign(channel=channel, campaign=campaign)

        # Group and resample
        df = (
            df.groupby(["channel", "campaign"])
            .resample(freq)[metrics]
            .sum()
            .reset_index()
        )

        # Reordering the final columns
        df = df[["Date"] + metrics + ["channel", "campaign"]]
        return df

    except KeyError as e:
        raise KeyError(f"Missing expected columns for grouping or resampling: {e}")
    except Exception as e:
        raise Exception(f"Error while grouping and resampling: {e}")


def main():
    """
    Main entry point to read CSVs from Facebook and Instagram paths, process them,
    group & resample by channel, then export a combined file.
    """
    try:
        # Define paths
        facebook_path = (
            "C:\\Users\\izzaz\\Documents\\1 Projects\\T - Onboarding of Mutinex MMM"
            "\\data\\raw\\Owned Media\\20250107 Export from Facebook Business Manager\\Facebook"
        )
        instagram_path = (
            "C:\\Users\\izzaz\\Documents\\1 Projects\\T - Onboarding of Mutinex MMM"
            "\\data\\raw\\Owned Media\\20250107 Export from Facebook Business Manager\\Instagram"
        )

        filenames = ["Follows", "Interactions", "Link clicks", "Reach", "Visits"]
        full_filenames = [name + ".csv" for name in filenames]

        # Generate list of file paths
        all_fb_pathnames = [
            path.join(facebook_path, filename) for filename in full_filenames
        ]
        all_ig_pathnames = [
            path.join(instagram_path, filename) for filename in full_filenames
        ]

        # Process Facebook CSVs
        facebook_df: pd.DataFrame = combine_dataframes(all_fb_pathnames, filenames)
        # Group and resample for Facebook
        facebook_df = group_and_resample(facebook_df, "facebook", "Time Internet")

        # Process Instagram CSVs
        instagram_df: pd.DataFrame = combine_dataframes(all_ig_pathnames, filenames)
        # Group and resample for Instagram
        instagram_df = group_and_resample(instagram_df, "instagram", "profile page")

        # Combine Facebook & Instagram data
        combined_df: pd.DataFrame = pd.concat(
            [facebook_df.set_index("Date"), instagram_df.set_index("Date")], axis=0
        ).fillna(0)

        # Generate path to save processed data
        processed_folder_path: str = (
            "C:\\Users\\izzaz\\Documents\\1 Projects\\T - Onboarding of Mutinex MMM"
            "\\data\\processed"
        )
        processed_filename: str = generate_filename(
            "002", "izzaz", processed_folder_path
        )

        # Export combined dataframe
        combined_df.to_csv(processed_filename)
        print(f"File exported to {processed_folder_path}")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
