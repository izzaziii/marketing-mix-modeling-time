import pandas as pd


def process_metrics(df):
    dataframes_list = []
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


filepath = "../data/raw/Paid Media for MMM - DV360.csv"

df = pd.read_csv(filepath)

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

metric_columns = ["Impressions", "Media cost", "Total media cost", "Revenue"]

dataframes_list = process_metrics(df)

weekly_impr = dataframes_list[0]
weekly_mediacost = dataframes_list[1]
weekly_totalmediacost = dataframes_list[2]
weekly_revenue = dataframes_list[3]

merged_df = pd.concat(
    [
        weekly_impr,
        weekly_mediacost["Media cost"],
        weekly_totalmediacost["Total media cost"],
        weekly_revenue["Revenue"],
    ],
    axis=1,
)

merged_df.to_csv("../data/processed/001_izzaz_20241128_paidmedia_dv360.csv")
