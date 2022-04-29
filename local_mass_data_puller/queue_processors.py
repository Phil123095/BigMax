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


def general_db_uploading(queue, process_count, database_class, local):
    database_class.receive_connection(connector.get_db_connections(local=local))
    print("DB Connection Done")
    stop_count = 0
    coin_count = 0
    while stop_count < process_count:
        item = queue.get()
        if item == "STOP":
            stop_count += 1
        else:
            coin_id = list(item.keys())[0]
            coin_count += 1
            coin_cleaned = cc.HistCoinToProcess(item, coin_id)
            coin_cleaned.clean_all_data()
            if coin_count % 1000 == 0:
                print(f"{coin_count} coins processed by queue.")
            database_class.data_aggregate(coin_cleaned)
    database_class.data_aggregate(None)
    print("All Consumption is Done")
    return


def all_hist_queue_processor(queue, process_count, local):
    print("In All Hist cleaner")
    database = db.FullHistoryDatabaseProcessor(threshold=4000)
    general_db_uploading(queue=queue, process_count=process_count, database_class=database, local=local)
    return


def day_data_queue_processor(queue, process_count, local):
    print("In Day Info cleaner")
    database = db.DayInfoDatabaseProcessor(threshold=2000)
    general_db_uploading(queue=queue, process_count=process_count, database_class=database, local=local)
    return


def hist_price_queue_processor(queue, process_count, local):
    print("In Hist Price cleaner")
    database = db.HistPriceDatabaseProcessor(threshold=100)
    general_db_uploading(queue=queue, process_count=process_count, database_class=database, local=local)
    return