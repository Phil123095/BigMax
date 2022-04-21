####### READ ME: ######
# This is the "backend script" that the recurring script calls upon. It has several functions. #

# Local Packages
import datetime
import time
import pandas as pd
from pangres import upsert
import numpy as np


"""
    Pull all the cmc coin IDs, and add the ones not yet in the DB to the DB.
"""
def get_all_coins_cmc(cmc_api, con):
    coins = cmc_api.cryptocurrency_map()
    with con.connect() as cur:
        coins_final = coins.data
        df = pd.DataFrame(coins_final)

        df_final = df[
            ["id", "name", "symbol", "slug", "rank", "is_active", "first_historical_data", "last_historical_data"]]
        df_final.columns = ['ID', 'name', 'symbol', 'slug', 'mkt_rank', 'is_active', 'first_historical_data',
                            'last_historical_data']

        df_final['first_historical_data'] = pd.to_datetime(df_final['first_historical_data'],
                                                           format='%Y-%m-%dT%H:%M:%S.%fZ')
        df_final['last_historical_data'] = pd.to_datetime(df_final['last_historical_data'],
                                                          format='%Y-%m-%dT%H:%M:%S.%fZ')
        df_to_use = df_final.set_index('ID')
        upsert(engine=cur,
               df=df_to_use,
               table_name='cmc_main',
               if_row_exists='update')
        coin_ids = list(df_final["ID"])
    return coin_ids


"""
    This is the function that is actually called by the recurring script.
    It converts the full coin_id_list into batches (as CMC limits the number of coin_ids per call)
    And then loops through the batches runs the get_coin_info_cmc function (see below)
"""
def get_coin_info_cmc(coin_id_list, cmc_api, con):
    batch_list = convert_list_to_batch_cmc(coin_id_list)
    df_info = pd.DataFrame(columns=['ID', 'date_added_cmc', 'name', 'symbol', 'description', 'url',
                                    'category'])
    df_tags = pd.DataFrame(columns=['ID', 'tag', 'tag_groups'])
    df_platforms = pd.DataFrame(columns=['ID', 'plat_ID', 'plat_name', 'symbol', 'token_address'])
    df_urls = pd.DataFrame(columns=['ID', 'url_cat', 'unique_url_cat', 'url'])
    df_contract_info = pd.DataFrame(columns=['ID', 'plat_ID', 'plat_name', 'contract_address'])
    for batch in batch_list:
        info, tags, platforms, urls, contracts = batch_pull_info_cmc(batch, cmc_api)
        df_info = df_info.append(info, ignore_index=True)
        df_tags = df_tags.append(tags, ignore_index=True)
        df_platforms = df_platforms.append(platforms, ignore_index=True)
        df_urls = df_urls.append(urls, ignore_index=True)
        df_contract_info = df_contract_info.append(contracts, ignore_index=True)
        time.sleep(3)
        print("Batch done")
    print("Dataframes created, now uploading to DB")
    upload_coin_info(con, df_info, df_tags, df_platforms, df_urls, df_contract_info)
    return 0


# This is run by the upload_coin_info_cmc function.
def batch_pull_info_cmc(coin_id_list, cmc_api):
    # Transform the coin_id list into a string of all ids separated by commas.
    final_coins = ','.join(coin_id_list)
    information = cmc_api.cryptocurrency_info(id=final_coins)
    info_list = []
    platform_list = []
    tag_list_full = []
    url_list_full = []
    contract_list_full = []
    for crypto in coin_id_list:
        info = information.data[crypto]
        info_row = [
            info['id'],
            datetime.datetime.strptime(info['date_added'], '%Y-%m-%dT%H:%M:%S.%fZ'),
            info['name'],
            info['symbol'],
            str(info['description']),
            str(info['urls']['website']),
            info['category']
        ]
        info_list.append(info_row)

        tag_list = info['tags']
        tag_groups = info['tag-groups']
        urls = info['urls']
        contracts = info['contract_address']
        if contracts is not None:
            df_contract = convert_contract_address(contracts, crypto)
            for row in df_contract:
                contract_list_full.append(row)

        if tag_list is not None:
            for tag in range(len(tag_list)):
                tag_info = [
                    info['id'],
                    tag_list[tag],
                    tag_groups[tag]
                ]
                tag_list_full.append(tag_info)

        platform = info['platform']
        if platform is not None:
            plat_info = [
                info['id'],
                platform['id'],
                platform['name'],
                platform['symbol'],
                platform['token_address']
            ]
            platform_list.append(plat_info)

        if urls is not None:
            df_url = convert_urls_to_table(urls, crypto)
            for row in df_url:
                url_list_full.append(row)

    df_info = pd.DataFrame(info_list, columns=['ID', 'date_added_cmc', 'name', 'symbol', 'description', 'url',
                                               'category'])
    df_tags = pd.DataFrame(tag_list_full, columns=['ID', 'tag', 'tag_groups'])
    df_platforms = pd.DataFrame(platform_list, columns=['ID', 'plat_ID', 'plat_name', 'symbol', 'token_address'])
    df_urls = pd.DataFrame(url_list_full, columns=['ID', 'url_cat', 'unique_url_cat', 'url'])
    df_contract_info = pd.DataFrame(contract_list_full, columns=['ID', 'plat_ID', 'plat_name', 'contract_address'])

    return df_info, df_tags, df_platforms, df_urls, df_contract_info


def upload_coin_info(con, info_table, tags_table, platform_table, url_table, contracts_table):
    with con.connect() as cur:
        # Upsert (Update and Insert) the coin info data.
        info_table = info_table.set_index(['ID'])
        tags_table = tags_table.set_index(['ID', 'tag'])
        platform_table = platform_table.set_index(['ID', 'plat_ID'])
        url_table = url_table.set_index(['ID', 'unique_url_cat'])
        # contracts_table = contracts_table.set_index(['ID', 'plat_ID'])
        # contracts_table = contracts_table.reindex[contracts_table.index.duplicated(keep=False)]

        print("0")
        upsert(engine=cur,
               df=info_table,
               table_name='cmc_info',
               if_row_exists='update')

        print("1")
        upsert(engine=cur,
               df=tags_table,
               table_name='cmc_tags',
               if_row_exists='update')

        print("2")
        upsert(engine=cur,
               df=platform_table,
               table_name='cmc_platforms',
               if_row_exists='update')

        print("3")
        upsert(engine=cur,
               df=url_table,
               table_name='cmc_websites',
               if_row_exists='update')

        # print("4")
        # upsert(engine=cur,
        #      df=contracts_table,
        #      table_name='cmc_contracts',
        #      if_row_exists='update')


def convert_urls_to_table(url_dict, coin_id):
    full_url_list = []
    for url_type in url_dict.keys():
        if len(url_dict[url_type]) == 0:
            continue
        else:
            count = 0
            for link in url_dict[url_type]:
                url_list = [
                    coin_id,
                    url_type,
                    url_type + str(count),
                    link
                ]
                full_url_list.append(url_list)
                count += 1
    return full_url_list


def convert_contract_address(contract_list, coin_id):
    contract_new_list = []
    for contract in contract_list:
        ind_contract = [
            coin_id,
            contract['platform']['coin']['id'],
            contract['platform']['coin']['name'],
            contract["contract_address"]
        ]
        contract_new_list.append(ind_contract)
    return contract_new_list


def convert_list_to_batch_cmc(coin_id_list):
    y = 0
    all_batches = []
    batch = []
    for i in range(len(coin_id_list)):
        if y < 199:
            batch.append(str(coin_id_list[i]))
        elif y == 200:
            batch.append(str(coin_id_list[i]))
            all_batches.append(batch)
            y = 0
            batch = []
        y += 1
    return all_batches


def check_already_in(coin_id, cur):
    all_ids = cur.execute("SELECT ID, max(timestamp) FROM hist_prices WHERE ID = ? GROUP BY ID", (coin_id,))
    result = all_ids.fetchone()
    if result is None:
        return 0
    else:
        sql_date = datetime.datetime.strptime(result[1], '%Y-%m-%d %H:%M:%S').date()
        today = datetime.date.today()
        if sql_date == today:
            return 1
        elif sql_date != today:
            return 0
