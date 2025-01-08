import pandas as pd
from datetime import datetime, timedelta

filename = "../data/interim/001_izzaz_20241216_paidmedia_ooh.csv"
df = pd.read_csv(filename)

df["Start Date"] = pd.to_datetime(df["Start Date"], format="%d/%m/%Y")
df["End Date"] = pd.to_datetime(df["End Date"], format="%d/%m/%Y")

df["startdate_weekday"] = df["Start Date"].dt.weekday
df["adjusted_startdate"] = df["Start Date"] + pd.to_timedelta(
    6 - df["startdate_weekday"], unit="d"
)

df["enddate_weekday"] = df["End Date"].dt.weekday
df["adjusted_enddate"] = df["End Date"] + pd.to_timedelta(
    6 - df["enddate_weekday"], unit="d"
)

df = df.drop(columns=["Start Date", "End Date", "startdate_weekday", "enddate_weekday"])

df["no_of_weeks_ran"] = (df["adjusted_enddate"] - df["adjusted_startdate"]).dt.days

df.sort_values(by="no_of_weeks_ran", ascending=False)
