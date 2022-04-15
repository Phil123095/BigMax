import os
import pandas as pd
import datetime
import numpy as np
from pangres import upsert

try:
    from cg_db_utils.cg_db_utils.pycoingecko_dev import CoinGeckoAPI
    import cg_db_utils.cg_db_utils.DB_connector.get_db_connection as connector
except ModuleNotFoundError:
    from cg_db_utils.pycoingecko_dev.api import CoinGeckoAPI
    import cg_db_utils.DB_connector.get_db_connection as connector


def get_coin_list_cg(cg_api):
    info = cg_api.get_coins_list()
    coins_to_pull = [item['id'] for item in info]
    print(len(coins_to_pull))
    return coins_to_pull


def check_non_active(connection, cg_coin_list):
    query = "Select distinct(ID) from coingeckodb.cg_general_info"
    db_data = pd.read_sql(query, con=connection)
    if len(db_data) > len(cg_coin_list):
        out_result_df = pd.DataFrame({'ID':
                                          np.setdiff1d(db_data['ID'].to_numpy(),
                                                       np.array(cg_coin_list))})

        out_result_df['is_active'] = False
        out_result_df = out_result_df.set_index(['ID'])

        upsert(con=connection,
               df=out_result_df,
               table_name='cg_general_info',
               if_row_exists='update')

    else:
        print("No dead coins")


def update_prices_daily_cg(con, cg_api):
    id_list = pd.read_sql_query(
        "select distinct(ID) from coingeckodb.cg_general_info where is_active = 1",
        con=con)
    cg_list = id_list["ID"].tolist()

    batch_list = convert_list_to_batch(cg_list)
    count = 0
    for batch in batch_list:
        count += 1
        print("Doing batch number: " + str(count))
        output = cg_api.get_price(ids=batch.tolist(), vs_currencies='usd',
                                  include_market_cap='true', include_24hr_vol='true',
                                  include_last_updated_at='true')
        write_daily_price_cg_to_db(output, con)


def write_daily_price_cg_to_db(api_result, con):
    df = pd.DataFrame(columns=['timestamp', 'clean_date', 'ID', 'price', 'volume', 'market_cap'])
    timestamps = []
    clean_days = []
    IDs = []
    prices = []
    volumes = []
    market_caps = []

    for coin in api_result:
        coin_info = api_result[coin]
        if coin_info.get('usd') is not None:
            timestamps.append(datetime.datetime.fromtimestamp(coin_info['last_updated_at']))
            clean_days.append(datetime.date.fromtimestamp(coin_info['last_updated_at']))
            IDs.append(coin)
            prices.append(coin_info['usd'])
            volumes.append(coin_info['usd_24h_vol'])
            market_caps.append(coin_info['usd_market_cap'])
        else:
            continue

    df['timestamp'] = timestamps
    df['clean_date'] = clean_days
    df['ID'] = IDs
    df['price'] = prices
    df['volume'] = volumes
    df['market_cap'] = market_caps
    print(df)

    df.to_sql('cg_hist_prices', con=con, if_exists='append', index=False)


# Supporting function for CMC coin pulls. Takes a full list of coins and returns a list of lists of 200 ids.
# Ex: A list of 1000 coins would be converted into a list containing 5 sub-lists of 200 coins.
def convert_list_to_batch(coin_id_list):
    batches = np.array_split(np.array(coin_id_list), round(len(coin_id_list) / 200))
    return batches


def lambda_handler_daily_prices(event=None, context=None):
    if os.environ.get("AWS_EXECUTION_ENV") is None:
        local = True
    else:
        local = False

    cg_api = CoinGeckoAPI(local=local)
    conn = connector.get_db_connections(local=local)
    coin_list = get_coin_list_cg(cg_api)
    check_non_active(conn, coin_list)
    update_prices_daily_cg(conn, cg_api)
    cg_api.gateway_close()

    return {'Status': '200'}
