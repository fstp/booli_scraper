import itertools
import json
import logging
import re
import time
from datetime import datetime
from string import Template

from bs4 import BeautifulSoup
from curl_cffi import requests
from glom import Coalesce, T, glom
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

cookie = "didomi_token=eyJ1c2VyX2lkIjoiMTkyZjY2MGUtYzM1Ni02ZGVlLTliZGItZGJmNmM1YjZjNDJkIiwiY3JlYXRlZCI6IjIwMjQtMTEtMDRUMDg6NTM6MjAuMDUzWiIsInVwZGF0ZWQiOiIyMDI0LTExLTA0VDA4OjUzOjIyLjUyOFoiLCJ2ZW5kb3JzIjp7ImVuYWJsZWQiOlsiZ29vZ2xlIiwiYzppbnRlcmNvbSIsImM6Ym9vbGktSkFEeUQ4NmUiXX0sInB1cnBvc2VzIjp7ImVuYWJsZWQiOlsiZ2VvbG9jYXRpb25fZGF0YSIsInByZXN0YW5kYSJdfSwidmVyc2lvbiI6MiwiYWMiOiJDS1dBRUJFa0VVb0EuQUFBQSJ9; booli_tracking_consent=true; euconsent-v2=CQHkJEAQHkJEAAHABBENBNFoAP_AAAAAACQgF5wBwAPAAyAGiAP0AiIBigF5gAAAHLQAYAAgqIIAAFIAMAAQVECQAYAAgqIOgAwABBUQhABgACCohKADAAEFRAAA.f_gAAAAAAAAA; booli_visitor=192f660e-c356-6dee-9bdb-dbf6c5b6c42d; BooliHasAbot=false; none; booli_has_bot=false; booli_session=5d57843d-e861-4f37-aa4d-a4c0182a5411"
headers = {
    "Host": "www.booli.se",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Referer": "https://www.booli.se/sok/slutpriser",
    "content-type": "application/json",
    "api-client": "booli.se",
    "Origin": "https://www.booli.se",
    "DNT": "1",
    "Sec-GPC": "1",
    "Connection": "keep-alive",
    "Cookie": cookie,
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Priority": "u=4",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
}


def get_detailed_info(id: int) -> str:
    graphql_payload = {
        "operationName": "selectedListing",
        "variables": {"id": id},
        "query": """query selectedListing($id: ID!) {
          object: propertyByListingId(listingId: $id) {
            __typename
            ... on Property {
              id
              salesOfResidence {
                id
                booliId
                agency {
                  id
                  name
                  url
                  thumbnail
                }
                agent {
                  id
                  name
                  image
                  premium
                }
              }
            }
          }
        }""",
    }
    r = requests.post(
        "https://www.booli.se/graphql",
        headers=headers,
        json=graphql_payload,
    )
    r.raise_for_status()
    return r.text


def save_to_mongo(documents, collection):
    mongo_uri = "mongodb://root:root@localhost:27017/"
    db_name = "booli"
    collection_name = "sold_2020"

    client = None
    try:
        client = MongoClient(mongo_uri)

        db = client[db_name]
        collection = db[collection_name]

        for doc in documents:
            filter_query = {"_id": doc["_id"]}
            update_query = {
                "$set": doc
            }  # Use $set to update or insert the entire document
            result = collection.update_one(filter_query, update_query, upsert=True)

            log.info(f"Document with _id: {doc['_id']} upserted: {result.acknowledged}")
    except Exception:
        log.exception("Failed to insert into MongoDB")
    finally:
        if client:
            client.close()


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")


def scraping_page(
    page: int, collection: str, template: Template, start_date: str, end_date: str
):
    log.info(f"Scraping page {page} in range [{start_date}, {end_date}]")
    log.info(f"Saving into collection: {collection}")

    script_tags = []
    for _ in range(5):
        r = requests.get(
            template.substitute(start_date=start_date, end_date=end_date, page=page),
            headers=headers,
        )
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")
        script_tags = soup.find_all("script", type="application/json")
        if script_tags:
            break
        log.error(f"Retrying page {page}...")
        time.sleep(5)

    json_data = json.loads(script_tags[0].text)
    gloomed = glom(json_data, "props.pageProps.__APOLLO_STATE__")
    filtered_data = {k: v for k, v in gloomed.items() if k.startswith("SoldProperty")}

    with open("not_processed.json", "w", encoding="utf-8") as file:
        json.dump(filtered_data, file, indent=4, ensure_ascii=False)

    documents = []

    for _, v in filtered_data.items():
        spec = {
            "_id": ("id", T, int),
            "id": ("id", T, int),
            "amenities": "amenities",
            "sold_price": "soldPrice.raw",
            "street_address": "streetAddress",
            "sold_sqm_price": Coalesce("soldSqmPrice.formatted", default=None),
            "sold_price_absolute_diff": Coalesce(
                "soldPriceAbsoluteDiff.formatted", default=None
            ),
            "sold_price_percentage_diff": Coalesce(
                "soldPricePercentageDiff.formatted", default=None
            ),
            "list_price": Coalesce("listPrice.formatted", default=None),
            "living_area": Coalesce("livingArea.formatted", default=None),
            "rooms": Coalesce("rooms.formatted", default=None),
            "floor": Coalesce("floor.value", default=None),
            "area_name": "descriptiveAreaName",
            "days_active": "daysActive",
            "sold_date": Coalesce("soldDate", default=None),
            "latitude": "latitude",
            "longitude": "longitude",
            "url": "url",
        }
        v = glom(v, spec)

        if v["sold_sqm_price"] is not None:
            cleaned = v["sold_sqm_price"].replace("\xa0", "")
            cleaned = re.findall(r"\d+", cleaned)[0]
            v["sold_sqm_price"] = int(cleaned)

        if v["sold_price_absolute_diff"] is not None:
            cleaned = v["sold_price_absolute_diff"].replace(" ", "")
            cleaned = re.findall(r"[+-]?\d+", cleaned)[0]
            v["sold_price_absolute_diff"] = int(cleaned)

        if v["sold_price_percentage_diff"] is not None:
            cleaned = v["sold_price_percentage_diff"].replace(",", ".").replace("%", "")
            cleaned = cleaned.replace("+/-", "")
            v["sold_price_percentage_diff"] = float(cleaned)

        if v["list_price"] is not None:
            cleaned = v["list_price"].replace("\xa0", "")
            cleaned = re.findall(r"\d+", cleaned)[0]
            v["list_price"] = int(cleaned)

        if v["living_area"] is not None:
            cleaned = v["living_area"].replace("\xa0", "")
            cleaned = re.findall(r"\d+", cleaned)[0]
            v["living_area"] = int(cleaned)

        if v["rooms"] is not None:
            cleaned = re.findall(r"\d+", v["rooms"])[0]
            v["rooms"] = int(cleaned)

        if v["floor"] is not None:
            cleaned = v["floor"].replace(",", ".")
            cleaned = 0.0 if cleaned == "BV" else float(cleaned)
            v["floor"] = cleaned

        if v["sold_date"] is not None:
            cleaned = datetime.strptime(v["sold_date"], "%Y-%m-%d")
            v["sold_date"] = cleaned

        documents.append(v)

    with open("processed.json", "w", encoding="utf-8") as file:
        json.dump(documents, file, indent=4, ensure_ascii=False, default=json_serial)

    log.info("Saving to MongoDB")
    save_to_mongo(documents, collection)


stockholm_34_rooms = Template(
    "https://www.booli.se/sok/slutpriser?areaIds=2&maxSoldDate=$end_date&minSoldDate=$start_date&objectType=L%C3%A4genhet&rooms=3,4&page=$page&searchType=slutpriser"
)

collection = "sold"
start_date = "2020-01-01"
end_date = "2020-12-31"

try:
    for i in itertools.count(300):
        scraping_page(i, collection, stockholm_34_rooms, start_date, end_date)
        time.sleep(5)
except Exception:
    log.exception("Finished scraping")
