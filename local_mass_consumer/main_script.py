import time
import pandas as pd
import datetime
import multiprocessing
import producer.mass_producer as mp

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


def all_hist_queue_processor(queue, process_count, start_length):
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
    database = db.DayInfoDatabaseProcessor(total_length=None, threshold=2000)
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


def lambda_runner_function(in_list, process_to_run, nr_of_processes):
    out_queue = multiprocessing.Queue()

    if in_list is None:
        cg_api = CoinGeckoAPI(local=True)
        input_list = get_coin_list_cg(cg_api)
        cg_api.gateway_close()

    else:
        input_list = in_list

    if process_to_run == 'HISTORICAL_PRICES':
        cons_process = multiprocessing.Process(target=hist_price_queue_processor,
                                               args=(out_queue, nr_of_processes))

    elif process_to_run == 'ALL_HISTORICAL':
        cons_process = multiprocessing.Process(target=all_hist_queue_processor,
                                               args=(out_queue, nr_of_processes, len(input_list)))

    elif process_to_run == 'DAILY_INFO':
        cons_process = multiprocessing.Process(target=day_data_queue_processor,
                                               args=(out_queue, nr_of_processes))

    else:
        raise Exception("You need to provide a process, either: HISTORICAL_PRICES, ALL_HISTORICAL, DAILY_INFO")

    producer = mp.PullProcessManager(process_type=process_to_run, input_list=input_list,
                                     nr_of_processes=nr_of_processes, queue=out_queue)

    try:
        cons_process.start()
        tic = time.perf_counter()
        producer.start_pull_processes()
        toc = time.perf_counter()
        cons_process.join()
        producer.close_apis()
        print(f"{len(input_list)} calls processed in {(toc - tic)} seconds.")
    except KeyboardInterrupt:
        print("Interrupted by Keyboard")
        producer.close_apis()


def cg_daily_pull_lambda(in_list=None):
    tic = time.perf_counter()
    nr_of_processes = 8
    process_to_run = 'ALL_HISTORICAL'
    lambda_runner_function(in_list=in_list, process_to_run=process_to_run, nr_of_processes=nr_of_processes)
    toc = time.perf_counter()
    print(f"Full time taken {toc - tic} seconds")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

if __name__ == '__main__':
    total_api_calls = 0
    row_count = 0

    conn = connector.get_db_connections(local=True)
    sql_query = "select ID, clean_date from cg_hist_prices where clean_date >'2014-04-01'"
    all_dates = pd.read_sql(sql=sql_query, con=conn)
    print(all_dates.shape)

    records = all_dates.to_records(index=False)
    tuples_final = list(records)
    print(len(tuples_final))

    tuples_to_use = [(element[1].strftime('%d-%m-%Y'), element[0]) for element in tuples_final]
    tuples_to_use_short = tuples_to_use[:100000]

    print(len(tuples_to_use_short))
    print(f"{len(tuples_to_use_short)} calls to make in total")

    response = cg_daily_pull_lambda(in_list=tuples_to_use_short)
    print(response)
