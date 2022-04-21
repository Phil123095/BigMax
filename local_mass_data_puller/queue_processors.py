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

import json

def all_hist_queue_processor(queue, process_count):
    print("In All Hist cleaner")
    #database = db.FullHistoryDatabaseProcessor(total_length=start_length, threshold=4000)
    #database.receive_connection(connector.get_db_connections(local=True))
    print("DB Connection Done")
    stop_count = 0
    coin_count = 0
    while stop_count < process_count:
        # if coin_count == 0:
            # database.receive_start_time(time.perf_counter())
        item = queue.get()
        if item == "STOP":
            stop_count += 1
        else:
            coin_id = list(item.keys())[0]
            coin_count += 1
            coin_cleaned = cc.HistCoinToProcess(item, coin_id)
            coin_cleaned.clean_all_data()
            print(f"{coin_cleaned.id} is done, {coin_count} coins done.")
            #database.data_aggregate(coin_cleaned)
    #database.data_aggregate(None)
    print("All Consumption is Done")
    return


def day_data_queue_processor(queue, process_count):
    print("In Day Info cleaner")
    database = db.DayInfoDatabaseProcessor(threshold=2000)
    database.receive_connection(connector.get_db_connections())
    # database.check_non_active(cg_coin_list=input_list)
    print("DB Connection Done")
    stop_count = 0
    coin_count = 0
    while stop_count < process_count:
        item = queue.get()
        if item == "STOP":
            stop_count += 1
        else:
            coin_id = list(item.keys())[0]
            coin_cleaned = cc.DailyCoinProcess(item, coin_id)
            coin_cleaned.clean_all_data()
            database.data_aggregate(coin_cleaned)
    database.data_aggregate(None)
    print("All Consumption is Done")
    return


def hist_price_queue_processor(queue, process_count):
    print("In Hist Price cleaner")
    database = db.HistPriceDatabaseProcessor(threshold=100)
    database.receive_connection(connector.get_db_connections())
    print("DB Connection Done")
    stop_count = 0
    coin_count = 0
    end_list = []
    while stop_count < process_count:
        item = queue.get()
        if item == "STOP":
            stop_count += 1
        else:
            coin_count += 1
            if coin_count % 1000 == 0:
                print(f"{coin_count} coins processed by queue.")
            end_list.append(item)
            coin_id = list(item.keys())[0]
            coin_cleaned = cc.HistPriceCoinProcess(item, coin_id)
            coin_cleaned.clean_all_data()
            database.data_aggregate(coin_cleaned)
    database.data_aggregate(None)
    with open("Testing/price_output.json", "w") as file:
        json.dump(end_list, file, ensure_ascii=False, indent=4)
    print("All Consumption is Done")
    return