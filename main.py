import json
import logging
import re
from datetime import datetime
from string import Template
from typing import Callable

from bs4 import BeautifulSoup
from curl_cffi import requests
from glom import SKIP, Assign, Coalesce, T, assign, delete, glom
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

# jslde = JsonLdExtractor()

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
    # "Content-Length": "5457",  # Note: This should be a string
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


def get_num_pages():
    graphql_payload = {
        "operationName": "searchSold",
        "variables": {
            "input": {
                "filters": [{"key": "rooms", "value": "3,2"}],
                "areaId": "77104",
                "sort": "",
                "page": 1,
                "ascending": False,
            }
        },
        "query": """query searchSold($input: SearchRequest!) {
          search: searchSold(input: $input) {
            ...SearchSoldResult
            __typename
          }
        }

        fragment SearchSoldResult on SearchSoldResult {
          pages
          totalCount
          adTargetingProperties {
            key
            value
            __typename
          }
          result {
            id
            booliId
            amenities {
              key
              label
              __typename
            }
            soldPrice {
              formatted
              raw
              value
              unit
              __typename
            }
            streetAddress
            soldSqmPrice {
              formatted
              __typename
            }
            soldPriceAbsoluteDiff {
              formatted
              __typename
            }
            soldPricePercentageDiff {
              formatted
              raw
              __typename
            }
            listPrice {
              formatted
              __typename
            }
            livingArea {
              formatted
              __typename
            }
            rooms {
              formatted
              __typename
            }
            floor {
              formatted
              value
              __typename
            }
            objectType
            descriptiveAreaName
            location {
              region {
                municipalityName
                __typename
              }
              __typename
            }
            soldPriceType
            daysActive
            soldDate
            latitude
            longitude
            url
            plotArea {
              formatted
              __typename
            }
            __typename
          }
          meta {
            tilesQuery
            __typename
          }
          __typename
        }""",
    }
    r = requests.post(
        "https://www.booli.se/graphql",
        headers=headers,
        json=graphql_payload,
    )
    return glom(json.loads(r.text), "data.search.pages")


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


stockholm_34_rooms = Template(
    "https://www.booli.se/sok/slutpriser?areaIds=2&maxSoldDate=$end_date&minSoldDate=$start_date&objectType=L%C3%A4genhet&rooms=3,4&page=$page&searchType=slutpriser"
)

collection = "sold_2020"
start_date = "2020-01-01"
end_date = "2020-12-31"
page = 469

log.info(f"Scraping page {page} in range [{start_date}, {end_date}]")

r = requests.get(
    # f"https://www.booli.se/sok/slutpriser?objectType=L%C3%A4genhet&rooms=3&page={page}",
    stockholm_34_rooms.substitute(start_date=start_date, end_date=end_date, page=page),
    headers=headers,
)
r.raise_for_status()

soup = BeautifulSoup(r.text, "html.parser")
script_tags = soup.find_all("script", type="application/json")

if len(script_tags) == 1:
    # json_data = json.loads(script_tags[0].text)
    # print_json(json_data)
    # pprint(script_tags[0].text)
    json_data = json.loads(script_tags[0].text)
    gloomed = glom(json_data, "props.pageProps.__APOLLO_STATE__")
    filtered_data = {k: v for k, v in gloomed.items() if k.startswith("SoldProperty")}

    with open("data.json", "w", encoding="utf-8") as file:
        pretty_json = json.dumps(filtered_data, indent=4, ensure_ascii=False)
        file.write(pretty_json)

    documents = []

    for k, v in filtered_data.items():
        spec = {
            "_id": ("id", T, int),
            "id": ("id", T, int),
            "amenities": "amenities",
            "sold_price": "soldPrice.raw",
            "street_address": "streetAddress",
            "sold_sqm_price": "soldSqmPrice.formatted",
            "sold_price_absolute_diff": Coalesce(
                "soldPriceAbsoluteDiff.formatted", default=None
            ),
            "sold_price_percentage_diff": Coalesce(
                "soldPricePercentageDiff.formatted", default=None
            ),
            "list_price": Coalesce("listPrice.formatted", default=None),
            "living_area": "livingArea.formatted",
            "rooms": "rooms.formatted",
            "floor": Coalesce(("floor.value", T, int), default=None),
            "area_name": "descriptiveAreaName",
            "days_active": "daysActive",
            "sold_date": "soldDate",
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

        if v["sold_date"] is not None:
            cleaned = datetime.strptime(v["sold_date"], "%Y-%m-%d").date()
            v["sold_date"] = cleaned

        detailed_info = json.loads(get_detailed_info(v["id"]))
        detailed_info = glom(
            detailed_info,
            {
                "agent": ("data.object.salesOfResidence", ["agent"]),
                "agency": ("data.object.salesOfResidence", ["agency"]),
            },
        )
        detailed_info["agent"] = [x for x in detailed_info["agent"] if x is not None]
        detailed_info["agency"] = [x for x in detailed_info["agency"] if x is not None]

        for a in detailed_info["agent"]:
            a["id"] = int(a["id"])

        for a in detailed_info["agency"]:
            a["id"] = int(a["id"])

        v.update(detailed_info)
        documents.append(v)

    pprint(documents)
else:
    log.error("no script tags found")

log.info("Saving to MongoDB")

# urls = glom(
#     data,
#     ("0.itemListElement.*.url", [lambda x: x if "annons" not in x else SKIP]),
# )
# ids = []
# for url in urls:
#     pprint(url)
#     id = re.search(r"(\d+)$", url)
#     if id is not None:
#         ids.append(id.group(1))

# pprint(ids)


# json_data: str = get_detailed_info(4043678)
# print_json(json_data)


# r = requests.get(
#     "https://www.booli.se/_next/data/OQnlAaj-p4PfyARRskqoK/sv/sok/slutpriser.json?rooms=3,2&page=1000&searchType=slutpriser",
#     headers=headers,
# )
# data = json.loads(r.text)
# apollo_state = glom(data, "pageProps.__APOLLO_STATE__")
# sold_properties = [
#     int(apollo_state[key]["id"]) for key in apollo_state if "SoldProperty" in key
# ]
# pprint(sold_properties)

# pprint(get_detailed_info(5282475))


# pprint(get_num_pages())

# graphql_payload = {
#     "operationName": "selectedListing",
#     "variables": {"id": 2399871},
#     "query": """query selectedListing($id: ID!) {
#       object: propertyByListingId(listingId: $id) {
#         __typename
#         ... on Property {
#           id
#           salesOfResidence {
#             id
#             booliId
#             agency {
#               id
#               name
#               url
#               thumbnail
#               __typename
#             }
#             agent {
#               id
#               name
#               image
#               premium
#               __typename
#             }
#             soldPrice {
#               formatted
#               raw
#               value
#               unit
#               __typename
#             }
#             streetAddress
#             soldSqmPrice {
#               formatted
#               __typename
#             }
#             soldPriceAbsoluteDiff {
#               formatted
#               __typename
#             }
#             soldPricePercentageDiff {
#               formatted
#               raw
#               __typename
#             }
#             listPrice {
#               formatted
#               __typename
#             }
#             livingArea {
#               formatted
#               __typename
#             }
#             rooms {
#               formatted
#               __typename
#             }
#             floor {
#               value
#               __typename
#             }
#             objectType
#             descriptiveAreaName
#             location {
#               region {
#                 municipalityName
#                 __typename
#               }
#               __typename
#             }
#             soldPriceType
#             daysActive
#             soldDate
#             latitude
#             longitude
#             url
#             __typename
#           }
#           __typename
#         }
#         ... on Listing {
#           id
#           booliId
#           blockedImages
#           descriptiveAreaName
#           location {
#             region {
#               municipalityName
#               __typename
#             }
#             __typename
#           }
#           daysActive
#           published
#           livingArea {
#             formatted
#             __typename
#           }
#           listPrice {
#             formatted
#             value
#             unit
#             raw
#             __typename
#           }
#           listSqmPrice {
#             formatted
#             __typename
#           }
#           latitude
#           longitude
#           daysActive
#           primaryImage {
#             id
#             alt
#             __typename
#           }
#           objectType
#           rent {
#             formatted
#             raw
#             __typename
#           }
#           operatingCost {
#             raw
#             __typename
#           }
#           estimate {
#             price {
#               value
#               unit
#               raw
#               formatted
#               __typename
#             }
#             __typename
#           }
#           rooms {
#             formatted
#             __typename
#           }
#           floor {
#             value
#             __typename
#           }
#           streetAddress
#           url
#           isNewConstruction
#           biddingOpen
#           upcomingSale
#           mortgageDeed
#           tenureForm
#           plotArea {
#             formatted
#             __typename
#           }
#           amenities {
#             key
#             label
#             __typename
#           }
#           nextShowing {
#             startTime
#             __typename
#           }
#           __typename
#         }
#         ... on SoldProperty {
#           id
#           booliId
#           amenities {
#             key
#             label
#             __typename
#           }
#           agency {
#             id
#             name
#             url
#             thumbnail
#             __typename
#           }
#           agent {
#             id
#             name
#             image
#             premium
#             __typename
#           }
#           soldPrice {
#             formatted
#             raw
#             value
#             unit
#             __typename
#           }
#           streetAddress
#           soldSqmPrice {
#             formatted
#             __typename
#           }
#           soldPriceAbsoluteDiff {
#             formatted
#             __typename
#           }
#           soldPricePercentageDiff {
#             formatted
#             raw
#             __typename
#           }
#           listPrice {
#             formatted
#             __typename
#           }
#           livingArea {
#             formatted
#             __typename
#           }
#           rooms {
#             formatted
#             __typename
#           }
#           floor {
#             value
#             __typename
#           }
#           objectType
#           descriptiveAreaName
#           soldPriceType
#           daysActive
#           soldDate
#           latitude
#           longitude
#           url
#           location {
#             region {
#               municipalityName
#               __typename
#             }
#             __typename
#           }
#           __typename
#         }
#         ... on ResidenceWithSoldProperty {
#           id
#           booliId
#           agency {
#             id
#             name
#             url
#             thumbnail
#             __typename
#           }
#           agent {
#             id
#             name
#             image
#             premium
#             __typename
#           }
#           soldPrice {
#             formatted
#             raw
#             value
#             unit
#             __typename
#           }
#           streetAddress
#           soldSqmPrice {
#             formatted
#             __typename
#           }
#           soldPriceAbsoluteDiff {
#             formatted
#             __typename
#           }
#           soldPricePercentageDiff {
#             formatted
#             raw
#             __typename
#           }
#           listPrice {
#             formatted
#             __typename
#           }
#           livingArea {
#             formatted
#             __typename
#           }
#           rooms {
#             formatted
#             __typename
#           }
#           floor {
#             value
#             __typename
#           }
#           objectType
#           descriptiveAreaName
#           soldPriceType
#           daysActive
#           soldDate
#           latitude
#           longitude
#           url
#           location {
#             region {
#               municipalityName
#               __typename
#             }
#             __typename
#           }
#           __typename
#         }
#       }
#     }""",
# }
