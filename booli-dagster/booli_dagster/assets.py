import logging

import pandas as pd
from dagster import AssetCheckResult, asset, asset_check
from pymongo import MongoClient
from rich.logging import RichHandler

FORMAT = "%(message)s"
logging.basicConfig(
    level="INFO",
    format=FORMAT,
    datefmt="[%X]",
    handlers=[RichHandler()],
)
log = logging.getLogger("rich")

mongo_uri = "mongodb://root:root@localhost:27017/"
db_name = "booli"
collection = "sold"


@asset
def sundbybergs_torg_1e() -> pd.DataFrame:
    client = MongoClient(mongo_uri)
    db = client[db_name]
    col = db[collection]

    query = {"street_address": "Sundbybergs Torg 1E"}
    result = col.find(query).sort("sold_date", -1)

    df = pd.DataFrame(result)
    df.drop("_id", axis=1, inplace=True)
    df["sold_date"] = pd.to_datetime(df["sold_date"])

    logging.info(f"Found {len(df)} documents")
    logging.info(df.dtypes)

    return df


@asset_check(asset=sundbybergs_torg_1e, blocking=True)
def check_sundbybergs_torg(
    sundbybergs_torg_1e: pd.DataFrame,
) -> AssetCheckResult:
    passed = bool(
        (sundbybergs_torg_1e["street_address"] == "Sundbybergs Torg 1E").all()
    )
    return AssetCheckResult(passed=passed)


@asset
def print_it(sundbybergs_torg_1e: pd.DataFrame) -> None:
    # logging.info(sundbybergs_torg_1e)
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    print("\n" + sundbybergs_torg_1e.to_string())


@asset
def save_it(sundbybergs_torg_1e: pd.DataFrame) -> None:
    path = "/mnt/c/Programming/Python/notebooks/data/sundbybergs_torg_1e.parquet"
    sundbybergs_torg_1e.to_parquet(path, index=False)
    logging.info(f"Saved to {path}")
