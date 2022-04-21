import pandas as pd
import datetime

try:
    from cg_db_utils.cg_db_utils.pycoingecko_dev.api import CoinGeckoAPI
    import cg_db_utils.cg_db_utils.DB_connector.get_db_connection as connector
    import cg_db_utils.cg_db_utils.consumer_system.coin_cleaner as cc
    import cg_db_utils.cg_db_utils.consumer_system.database_processor as db
except ModuleNotFoundError:
    from cg_db_utils.pycoingecko_dev.api import CoinGeckoAPI
    import cg_db_utils.DB_connector.get_db_connection as connector
    import cg_db_utils.consumer_system.coin_cleaner as cc
    import cg_db_utils.consumer_system.database_processor as db


def prep_tuple_list(crypto_id, start_date, end_date):
    if start_date == '1970-01-01' or end_date == '1970-01-01':
        return
    else:
        format_sdate = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        format_edate = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        raw_list = pd.date_range(format_sdate, format_edate - datetime.timedelta(days=1), freq='d')

        hist_query_list = [(element.strftime('%d-%m-%Y'), crypto_id) for element in raw_list]

        return hist_query_list, len(hist_query_list)


def tuple_list_v2(crypto_id, date):
    if date == '1970-01-01' or crypto_id == 'ID':
        return 0, 0
    else:
        format_sdate = datetime.datetime.strptime(date, '%Y-%m-%d')
        return format_sdate.strftime('%d-%m-%Y'), crypto_id


def get_coin_list_cg(cg_api):
    info = cg_api.get_coins_list()
    coins_to_pull = [item['id'] for item in info]
    print(len(coins_to_pull))
    return coins_to_pull