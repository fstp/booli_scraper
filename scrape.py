import itertools
import json
import logging
import re
import time
from datetime import datetime
from string import Template

from bs4 import BeautifulSoup
from curl_cffi import requests
from glom import SKIP, Coalesce, T, glom
from pymongo import MongoClient
from rich import print_json
from rich.logging import RichHandler
from rich.pretty import pprint

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


def to_datetime(date_str: str):
    return datetime.strptime(date_str, "%Y-%m-%d")


def graphql_sales_data(url_id: int) -> str:
    graphql_payload = {
        "operationName": "salesOfProperty",
        "variables": {"residenceId": url_id, "booliId": url_id},
        "query": """query salesOfProperty($residenceId: ID, $booliId: ID) {
          salesOfProperty(residenceId: $residenceId, booliId: $booliId) {
            id
            url
            agent {
              id
              recommendations
              email
              name
              overallRating
              reviewCount
              url
              premium
              image
              listingStatistics {
                startDate
                endDate
                publishedCount
                publishedValue {
                  raw
                }
                recommendedCount
              }
            }
            agency {
              id
              name
              url
              thumbnail
            }
          }
        }
        """,
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

    client = None
    try:
        client = MongoClient(mongo_uri)

        db = client[db_name]
        collection = db[collection]

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
            "amenities": "amenities",
            "sold_price": "soldPrice.raw",
            "street_address": "streetAddress",
            "sold_sqm_price": Coalesce("soldSqmPrice.formatted", default=SKIP),
            "sold_price_absolute_diff": Coalesce(
                "soldPriceAbsoluteDiff.formatted", default=SKIP
            ),
            "sold_price_percentage_diff": Coalesce(
                "soldPricePercentageDiff.formatted", default=SKIP
            ),
            "list_price": Coalesce("listPrice.formatted", default=SKIP),
            "living_area": Coalesce("livingArea.formatted", default=SKIP),
            "rooms": Coalesce("rooms.formatted", default=SKIP),
            "floor": Coalesce("floor.value", default=SKIP),
            "area_name": "descriptiveAreaName",
            "days_active": "daysActive",
            "sold_date": Coalesce(("soldDate", T, to_datetime), default=SKIP),
            "latitude": "latitude",
            "longitude": "longitude",
            "url": "url",
        }
        v = glom(v, spec)

        if "sold_sqm_price" in v:
            cleaned = v["sold_sqm_price"].replace("\xa0", "")
            cleaned = re.findall(r"\d+", cleaned)[0]
            v["sold_sqm_price"] = int(cleaned)

        if "sold_price_absolute_diff" in v:
            cleaned = v["sold_price_absolute_diff"].replace(" ", "")
            cleaned = re.findall(r"[+-]?\d+", cleaned)[0]
            v["sold_price_absolute_diff"] = int(cleaned)

        if "sold_price_percentage_diff" in v:
            cleaned = v["sold_price_percentage_diff"].replace(",", ".").replace("%", "")
            cleaned = cleaned.replace("+/-", "")
            v["sold_price_percentage_diff"] = float(cleaned)

        if "list_price" in v:
            cleaned = v["list_price"].replace("\xa0", "")
            cleaned = re.findall(r"\d+", cleaned)[0]
            v["list_price"] = int(cleaned)

        if "living_area" in v:
            cleaned = v["living_area"].replace("\xa0", "")
            cleaned = re.findall(r"\d+", cleaned)[0]
            v["living_area"] = int(cleaned)

        if "rooms" in v:
            cleaned = re.findall(r"\d+", v["rooms"])[0]
            v["rooms"] = int(cleaned)

        if "floor" in v:
            cleaned = v["floor"].replace(",", ".")
            cleaned = 0.0 if cleaned == "BV" else float(cleaned)
            v["floor"] = cleaned

        # if "sold_date" in v:
        #     cleaned = datetime.strptime(v["sold_date"], "%Y-%m-%d")
        #     v["sold_date"] = cleaned

        v["url_id"] = int(re.findall(r"\d+", v["url"])[0])

        while True:
            try:
                v["sales"] = get_sales_data(v["url_id"])["sales"]
                time.sleep(2)
            except Exception:
                log.exception(f"Failed to get sales data for id: {v['_id']}")
                time.sleep(180)
            else:
                break

        log.info(f"Scraped data for id: {v['_id']}")
        documents.append(v)

    with open("processed.json", "w", encoding="utf-8") as file:
        json.dump(documents, file, indent=4, ensure_ascii=False, default=json_serial)

    log.info("Saving to MongoDB")
    save_to_mongo(documents, collection)


def run_scrape():
    stockholm_34_rooms = Template(
        "https://www.booli.se/sok/slutpriser?areaIds=2&maxSoldDate=$end_date&minSoldDate=$start_date&objectType=L%C3%A4genhet&rooms=3,4&page=$page&searchType=slutpriser"
    )
    stockholm_12_rooms = Template(
        "https://www.booli.se/sok/slutpriser?areaIds=2&maxSoldDate=$end_date&minSoldDate=$start_date&objectType=L%C3%A4genhet&rooms=1,2&page=$page&searchType=slutpriser"
    )

    years = [
        # "2009",
        # "2010",
        # "2011",
        # "2012",
        # "2013",
        "2014",
        "2015",
        "2016",
        "2017",
        "2018",
        "2019",
        "2020",
        "2021",
        "2022",
        "2023",
        # "2024",
        # "2025",
    ]

    collection = "sold"

    for year in years:
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"

        i = 1
        try:
            for i in itertools.count(1):
                scraping_page(i, collection, stockholm_12_rooms, start_date, end_date)
                time.sleep(5)
        except Exception as e:
            log.exception("Finished scraping")
            with open("error_log.txt", "a") as f:
                f.write(
                    f"An error occurred during scraping {year} on page {i}: {str(e)}\n\n"
                )

        # Save the last update time as metadata
        save_to_mongo([{"_id": "meta", "last_update": datetime.now()}], "sold")


def get_sales_data(url_id: int):
    data = json.loads(graphql_sales_data(url_id))
    spec = {
        "sales": Coalesce(
            (
                "data.salesOfProperty",
                [
                    {
                        "id": ("id", T, int),
                        "url": "url",
                        "agent": Coalesce(
                            (
                                "agent",
                                {
                                    "id": ("id", T, int),
                                    "recommendations": "recommendations",
                                    "email": "email",
                                    "name": "name",
                                    "overall_rating": "overallRating",
                                    "review_count": "reviewCount",
                                    "url": "url",
                                    "image": Coalesce("image", default=SKIP),
                                    "premium": Coalesce("premium", default=SKIP),
                                    "listing_statistics": (
                                        "listingStatistics",
                                        {
                                            "start_date": ("startDate", T, to_datetime),
                                            "end_date": ("endDate", T, to_datetime),
                                            "published_count": "publishedCount",
                                            "published_value": "publishedValue.raw",
                                            "recommended_count": "recommendedCount",
                                        },
                                    ),
                                },
                            ),
                            default=None,
                        ),
                        "agency": Coalesce(
                            (
                                "agency",
                                {
                                    "id": ("id", T, int),
                                    "name": "name",
                                    "url": Coalesce("url", default=SKIP),
                                    "thumbnail": Coalesce("thumbnail", default=SKIP),
                                },
                            ),
                            default=None,
                        ),
                    }
                ],
            ),
            default=[],
        ),
    }
    v = glom(data, spec)
    return v


if __name__ == "__main__":
    run_scrape()
    # json_data = get_sales_data(595043)
    # pprint(json_data)
