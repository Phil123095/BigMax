import time
import os
import mass_producer as mp
try:
    from cg_db_utils.cg_db_utils.pycoingecko_dev import CoinGeckoAPI
except ModuleNotFoundError:
    from cg_db_utils.pycoingecko_dev.api import CoinGeckoAPI

import json


def get_coin_list_cg(cg_api):
    info = cg_api.get_coins_list()
    coins_to_pull = [item['id'] for item in info]
    print(len(coins_to_pull))
    return coins_to_pull


def lambda_runner_function(process_to_run, nr_of_processes, local):
    cg_api = CoinGeckoAPI(local=local)
    input_list = get_coin_list_cg(cg_api)
    cg_api.gateway_close()

    producer = mp.PullProcessManager(process_type=process_to_run, input_list=input_list,
                                     nr_of_processes=nr_of_processes, local=local)

    try:
        producer.start_pull_processes()
        tic = time.perf_counter()
        producer.close_apis()
        toc = time.perf_counter()
        print(f"{len(input_list)} calls processed in {(toc - tic)} seconds.")
    except KeyboardInterrupt:
        print("Interrupted by Keyboard")
        producer.close_apis()


def cg_daily_pull_lambda(event, context):
    if os.environ.get("AWS_EXECUTION_ENV") is None:
        local = True
    else:
        local = False

    nr_of_processes = 4
    process_to_run = 'DAILY_INFO'
    lambda_runner_function(process_to_run=process_to_run, nr_of_processes=nr_of_processes, local=local)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }