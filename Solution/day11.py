from dagster import asset, Config, asset_check ,AssetKey, AssetCheckResult, AssetCheckSeverity, AssetExecutionContext, MetadataValue, ConfigurableResource, Definitions
import pandas as pd
from pydantic import Field


class MyFilePath(Config):
    path : str = Field(
        default="Solution/bike_info.csv", description="The path to the csv to parse."
    )


class MyCSVToFile(ConfigurableResource):
    path : str

    def write(self, data: pd.DataFrame):
        data.to_csv(self.path)

    def read(self) -> pd.DataFrame:
        return pd.read_csv(self.path)



@asset (
    owners = ["voduyanqn1972@gmail.com"]
    , tags={'layer': 'bronze'}
    , kinds = {'pandas', 'csv'}
)
def bike_raw_cleaned(context: AssetExecutionContext, config: MyFilePath, csv_file_resource: MyCSVToFile):
    """ Reads in the raw bike sales data from a local csv file. TODO: pull the file from the web dynamically """

    df = pd.read_csv(config.path)
    df.columns = df.columns = ['index','junk', 'bike', 'msrp', 'price', 'brand','category','type','details', 'junk2']
    df = df.drop(['index', 'junk', 'junk2', 'details'], axis = 1)
    bikes = df.dropna()

    bikes['price'] = bikes['price'].str.replace(',', '')
    bikes['price'] = bikes['price'].str.replace('$', '').astype(float)
    
    nrow = len(bikes)
    context.log.info(f"Parsed a csv file and now have {nrow}")
    context.add_output_metadata({
       "rows": nrow,
       "head": MetadataValue.md(bikes.head().to_markdown()),
       "columns": MetadataValue.md(str(bikes.columns))  
    })

    csv_file_resource.write(bikes)
    # bikes.to_csv("bike_raw_cleaned.csv")

    return 

@asset_check(
    asset = bike_raw_cleaned
    , blocking = True
)
def check_bike_raw_cleaned(csv_file_resource: MyCSVToFile):

    bike_raw_cleaned: pd.DataFrame = csv_file_resource.read() # pd.DataFrame = pd.read_csv("bike_raw_cleaned.csv")

    nrow = len(bike_raw_cleaned)

    return AssetCheckResult(
        severity =  AssetCheckSeverity.WARN
        , passed = nrow < 0
        , metadata={
            "rows": nrow
        }
    )


@asset(
   deps=[bike_raw_cleaned]
) 
def bikes_summary(context: AssetExecutionContext, csv_file_resource: MyCSVToFile): 

    bike_raw_cleaned: pd.DataFrame = csv_file_resource.read() # pd.DataFrame = pd.read_csv("bike_raw_cleaned.csv")

    context.log.info(bike_raw_cleaned)
    bike_summary = bike_raw_cleaned.groupby('type')['price'].mean()
    print(bike_summary)
    return 

@asset(
    deps=[bikes_summary]
) 
def c(): ...


defs = Definitions(
    assets = [bike_raw_cleaned, bikes_summary]
    , asset_checks = [check_bike_raw_cleaned]
    , resources= {
        "csv_file_resource": MyCSVToFile(path = "bike_raw_cleaned.csv")
    }
)
