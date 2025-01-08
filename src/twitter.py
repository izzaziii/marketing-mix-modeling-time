import pandas as pd

df = pd.read_csv("../data/raw/Paid Media for MMM - Twitter.csv")

weekly_cost = (
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

weekly_impressions = (
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

merged_df = pd.concat([weekly_impressions, weekly_cost["Cost"]], axis=1)

merged_df.to_csv("../data/processed/001_izzaz_20241128_twitter.csv")
