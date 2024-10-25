from databases.connection import create_connection
import pandas as pd
from pymongo.errors import PyMongoError


def retrieve_data_as_dataframe(
    coll_name: str, database: str, query: dict = {}, projection: dict = None
) -> pd.DataFrame:
    """
    Retrieves data from a MongoDB collection and converts it into a pandas DataFrame.

    Args:
        coll_name (str): MongoDB collection name.
        database (str): MongoDB database name.
        query (dict, optional): MongoDB query to filter data. Defaults to {} (retrieve all).
        projection (dict, optional): Fields to include or exclude. Defaults to None (include all).

    Returns:
        pd.DataFrame: DataFrame containing the retrieved data.
    """
    try:
        coll = create_connection(database, coll_name)
        data = list(coll.find(query, projection))
        return pd.DataFrame(data)
    except PyMongoError as e:
        print(f"Failed to retrieve data from MongoDB. Error: {e}")
        raise


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


def main():
    """
    Main function to retrieve data from MongoDB, convert the date column, and display sorted results.
    """
    try:
        # Retrieve the data from MongoDB
        df = retrieve_data_as_dataframe("boreport", "deep-diver")

        # Convert the 'Probability 90% Date' column to datetime
        new_df = convert_column_to_datetime(df, "Probability 90% Date")

        # Sort the dataframe by the 'Probability 90% Date' column
        sorted_df = new_df["Probability 90% Date"].sort_values(ascending=False)
        print(sorted_df.head())  # Display the top rows of sorted data

    except Exception as e:
        print(f"An error occurred in the main function: {e}")


# Run the main function if the script is executed
if __name__ == "__main__":
    main()
