from dagster import asset
import pandas as pd

@asset
def a():
    df = pd.read_csv("Solution/bike_info.csv")
    return df

@asset(
    deps = [a]
)
def b(): ...

@asset(
    deps = [b]
)
def c(): ...

