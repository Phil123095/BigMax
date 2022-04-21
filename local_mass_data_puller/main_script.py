import time
import pandas as pd
import multiprocessing
from queue_processors import all_hist_queue_processor, day_data_queue_processor, hist_price_queue_processor
from utils import get_coin_list_cg

try:
    from cg_db_utils.cg_db_utils.pycoingecko_dev.api import CoinGeckoAPI
    import cg_db_utils.cg_db_utils.DB_connector.get_db_connection as connector
    import cg_db_utils.cg_db_utils.consumer_system.coin_cleaner as cc
    import cg_db_utils.cg_db_utils.consumer_system.database_processor as db
    import cg_db_utils.cg_db_utils.producer_system.mass_producer as mp

except ModuleNotFoundError:
    from cg_db_utils.pycoingecko_dev.api import CoinGeckoAPI
    import cg_db_utils.DB_connector.get_db_connection as connector
    import cg_db_utils.consumer_system.coin_cleaner as cc
    import cg_db_utils.consumer_system.database_processor as db
    import cg_db_utils.producer_system.mass_producer as mp

import json


def lambda_runner_function(in_list, process_to_run, nr_of_processes, local):
    out_queue = multiprocessing.Queue()

    if in_list is None:
        cg_api = CoinGeckoAPI(local=local)
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
    local = True

    conn = connector.get_db_connections(local=local)
    sql_query = "select ID, clean_date from cg_hist_prices where clean_date >'2014-04-01'"
    all_dates = pd.read_sql(sql=sql_query, con=conn)
    print(all_dates.shape)

    records = all_dates.to_records(index=False)
    tuples_final = list(records)
    print(len(tuples_final))

    tuples_to_use = [(element[1].strftime('%d-%m-%Y'), element[0]) for element in tuples_final]
    tuples_to_use_short = tuples_to_use[:10000]

    print(len(tuples_to_use_short))
    print(f"{len(tuples_to_use_short)} calls to make in total")

    tic = time.perf_counter()
    lambda_runner_function(in_list=tuples_to_use_short, process_to_run='ALL_HISTORICAL', nr_of_processes=8, local=local)
    toc = time.perf_counter()
    print(f"Full time taken {toc - tic} seconds")

