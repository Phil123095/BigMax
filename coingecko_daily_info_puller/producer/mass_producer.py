import concurrent.futures as cf
import requests
import dotenv
import os
from pycoingecko_dev.pycoingecko_dev.api import CoinGeckoAPI
import multiprocessing
import numpy as np
import datetime
import boto3
import json


def send_message(message, sqs_key, sqs_secret, sqs_url):
    sqs_client = boto3.client("sqs", aws_access_key_id=sqs_key,
                                   aws_secret_access_key=sqs_secret,
                                   region_name="eu-central-1")
    sqs_queue_url = sqs_url
    response = sqs_client.send_message(
        QueueUrl=sqs_queue_url,
        MessageBody=json.dumps(message),
        MessageGroupId='trial',
    )
    if response:
        print("Submitted to SQS")
    else:
        print("Error")
    return


class PullProcessManager:
    def __init__(self, process_type, input_list, nr_of_processes, local):
        self.__DEFAULT_PULLS = ['ALL_HISTORICAL', 'HISTORICAL_PRICES', 'DAILY_INFO']
        assert process_type in self.__DEFAULT_PULLS, print(f"Please select one of {self.__DEFAULT_PULLS} as an option.")
        self.process_type = process_type
        self.aws_sqs_key, self.aws_sqs_secret, self.aws_sqs_url = self.__load_SQS_creds()
        self.process_count = nr_of_processes
        self.split_pull_list = np.array_split(input_list, nr_of_processes)
        self.local = local
        self.class_and_process = {}
        self.api_list = self.__load_apis()

    def __load_SQS_creds(self):
        if self.local:
            dotenv.load_dotenv()

        aws_sqs_key = os.environ['AWS_SQS_KEY']
        aws_sqs_secret = os.environ['AWS_SQS_SECRET']
        aws_sqs_url = os.environ['AWS_SQS_URL']
        return aws_sqs_key, aws_sqs_secret, aws_sqs_url


    def start_pull_processes(self):
        self.__run_processes()

    def __load_apis(self):
        api_list = []
        for i in range(self.process_count):
            cg_api = CoinGeckoAPI(self.local)
            api_list.append(cg_api)
        return api_list

    def __run_processes(self):
        for i in range(self.process_count):
            process_name = "Process-" + str(i + 1) + "-" + str(multiprocessing.current_process())
            list_to_use = list(self.split_pull_list[i])
            process_obj = SingleProcessor(process_type=self.process_type, cg_api=self.api_list[i],
                                          proc_name=process_name, sqs_key=self.aws_sqs_key,
                                          sqs_secret=self.aws_sqs_secret, sqs_url=self.aws_sqs_url)
            process = multiprocessing.Process(target=process_obj.multithread_processor, args=(list_to_use, self.process_type))
            process.start()

            self.class_and_process[process_name] = {"object_processor": process_obj,
                                                    "process": process}

        for process_kill in self.class_and_process.keys():
            process_to_close = self.class_and_process[process_kill]["process"]
            process_to_close.join()
            process_to_close.close()

        print("All Producer Processes closed")

    def close_apis(self):
        for process_kill in self.class_and_process.keys():
            api_to_close = self.class_and_process[process_kill]["object_processor"]
            api_to_close.cg_api.gateway_close()


class SingleProcessor:
    def __init__(self, process_type, cg_api, proc_name, sqs_key, sqs_secret, sqs_url):
        self.process_type = process_type
        self.cg_api = cg_api
        self.process_name = proc_name
        self.sqs_key = sqs_key
        self.sqs_secret = sqs_secret
        self.sqs_url = sqs_url

    def multithread_processor(self, input_list, process_type):
        print(f"In {self.process_name}")

        if process_type == 'ALL_HISTORICAL':
            print(f"Here in {process_type}")
            with cf.ThreadPoolExecutor(max_workers=50) as executor:
                executor.map(self.historical_daily_data_processor, input_list)

        if process_type == 'HISTORICAL_PRICES':
            print(f"Here in {process_type}")
            with cf.ThreadPoolExecutor(max_workers=50) as executor:
                executor.map(self.historical_price_data_processor, input_list)

        if process_type == 'DAILY_INFO':
            print(f"Here in {process_type}")
            with cf.ThreadPoolExecutor(max_workers=50) as executor:
                executor.map(self.day_data_processor, input_list)

        print(f"Trying to close {self.process_name}")
        send_message("STOP", sqs_key=self.sqs_key, sqs_secret=self.sqs_secret, sqs_url=self.sqs_url)
        print(f"Done with {self.process_name}")
        return

    def historical_daily_data_processor(self, input):
        """
        Function to query cg_api for Full Historical Data (Developer, Community, Price, etc.)
        :param input: tuple of (date, coin_id) - date is in d-m-y format.
        :return: Nothing. Adds the result to the consumer queue.
        Result is in dict format: {"coin_id": {date: DATA}}
        """
        try:
            result = {input[1]: {input[0]: self.cg_api.get_coin_history_by_id(input[1], input[0])}}
        except requests.exceptions.HTTPError:
            print("Failed once, oops")
            result = {input[1]: {input[0]: self.cg_api.get_coin_history_by_id(input[1], input[0])}}

        send_message(result, sqs_key=self.sqs_key, sqs_secret=self.sqs_secret, sqs_url=self.sqs_url)

    def historical_price_data_processor(self, input):
        """
        Function to query cg_api for Historical Price Data (price, market_cap, volume)
        :param input: tuple of (coin_id, vs_currency, timeframe). Defaults for vs_currency is USD and timeframe is MAX.
        :return: Nothing. Adds result to consumer queue.
        Result is in dict format: {"coin_id":
                {"prices": LIST OF LISTS, "market_caps": LIST OF LISTS, "total_volumes": LIST OF LISTS}}
        """

        result = self.cg_api.get_coin_market_chart_by_id(input, vs_currency='usd', days='max')
        """
        try:
        except requests.exceptions.HTTPError:
            print("Failed once, oops")
            result = self.cg_api.get_coin_market_chart_by_id(input, vs_currency='usd', days='max')
            result2 = {input: result}
            print(result2)
        """

        send_message(result, sqs_key=self.sqs_key, sqs_secret=self.sqs_secret, sqs_url=self.sqs_url)

    def day_data_processor(self, input):
        """
        Function to query cg_api for Daily Coin Info (categories, description, websites, platforms, developer data,
        community_data)
        :param input: coin_id
        :return: Nothing. Adds result to consumer queue.
        Result is same as historical_daily_processor: {'coin_id': {'date': DATA}}
        """
        date = datetime.date.today().strftime('%d-%m-%Y')
        try:
            result = {input: {date: self.cg_api.get_coin_by_id(input)}}
        except requests.exceptions.HTTPError:
            print("Failed once, oops")
            result = {input: {date: self.cg_api.get_coin_by_id(input)}}
        send_message(result, sqs_key=self.sqs_key, sqs_secret=self.sqs_secret, sqs_url=self.sqs_url)