import requests
import pandas as pd
import datetime
import re
import logging
import time
import json
from tokpedscraper.utils import (
   standarized_columns, 
   save_df_to_csv
)

# Setup the logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.DEBUG
)

def request_shop_products(shop_id, page, per_page):
    endpoint = "https://gql.tokopedia.com/graphql/ShopProducts"

    payload = [
        {
            "operationName": "ShopProducts",
            "variables": {
                "source": "shop",
                "sid": shop_id,
                "page": page,
                "perPage": per_page,
                "etalaseId": "etalase",
                "sort": 1,
                "user_districtId": "2274",
                "user_cityId": "176",
                "user_lat": "0",
                "user_long": "0"
            },
            "query": """query ShopProducts(
                          $sid: String!,
                          $source: String,
                          $page: Int,
                          $perPage: Int,
                          $keyword: String,
                          $etalaseId: String,
                          $sort: Int,
                          $user_districtId: String,
                          $user_cityId: String,
                          $user_lat: String,
                          $user_long: String
                        ) {
                          GetShopProduct(
                            shopID: $sid,
                            source: $source,
                            filter: {
                              page: $page,
                              perPage: $perPage,
                              fkeyword: $keyword,
                              fmenu: $etalaseId,
                              sort: $sort,
                              user_districtId: $user_districtId,
                              user_cityId: $user_cityId,
                              user_lat: $user_lat,
                              user_long: $user_long
                            }
                          ) {
                            status
                            errors
                            links {
                              prev
                              next
                              __typename
                            }
                            data {
                              name
                              product_url
                              product_id
                              price {
                                text_idr
                                __typename
                              }
                              primary_image {
                                original
                                thumbnail
                                resize300
                                __typename
                              }
                              flags {
                                isSold
                                isPreorder
                                isWholesale
                                isWishlist
                                __typename
                              }
                              campaign {
                                discounted_percentage
                                original_price_fmt
                                start_date
                                end_date
                                __typename
                              }
                              label {
                                color_hex
                                content
                                __typename
                              }
                              label_groups {
                                position
                                title
                                type
                                url
                                __typename
                              }
                              badge {
                                title
                                image_url
                                __typename
                              }
                              stats {
                                reviewCount
                                rating
                                averageRating
                                __typename
                              }
                              category {
                                id
                                __typename
                              }
                              __typename
                            }
                            __typename
                          }
                        }"""
        }
    ]

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Referer": "https://www.tokopedia.com",
    }

    return requests.request("POST", url=endpoint, json=payload, headers=headers)

def get_shop_products(shop_id, page, per_page):
    response = request_shop_products(shop_id, page, per_page)
    data = response.json()
    products = data[0]['data']["GetShopProduct"]["data"]
    # with open('response.json', 'w') as f:
    #   json.dump(products, f, indent=4)
    df = pd.json_normalize(products)
    return pd.DataFrame(list(map(standarized_columns, [df]))[0])
    # return print(df)

def get_tokped_shop_products_data(shop_id, pages, per_page, directory, shop_name):
    for page in range(1, pages+1):
        try:
            df = get_shop_products(shop_id, page, per_page)
            # print(df)
            logging.info(f"Retrieved data from page {page}")

            time.sleep(5)  # Rate limiting
            if page == 1:
                save_df_to_csv(
                    df=df,
                    dir=directory,
                    file_name=shop_name, 
                    header=True
                )
            else:
                save_df_to_csv(
                    df=df,
                    dir=directory,
                    file_name=shop_name, 
                    header=False
                )

            # return df
            
            logging.info(f"Saved data from page {page} to CSV")

        except Exception as e:
            logging.error(e)
            pass