import concurrent.futures as cf
import time
import requests
import dotenv
import os
try:
    from cg_db_utils.cg_db_utils.pycoingecko_dev.api import CoinGeckoAPI
except ModuleNotFoundError:
    from cg_db_utils.pycoingecko_dev.api import CoinGeckoAPI
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
    def __init__(self, process_type, input_list, nr_of_processes, local, queue=None):
        self.__DEFAULT_PULLS = ['ALL_HISTORICAL', 'HISTORICAL_PRICES', 'DAILY_INFO']
        assert process_type in self.__DEFAULT_PULLS, print(f"Please select one of {self.__DEFAULT_PULLS} as an option.")
        self.process_type = process_type
        self.process_count = nr_of_processes
        self.split_pull_list = np.array_split(input_list, nr_of_processes)
        self.local = local
        if self.local:
            assert queue, print("If you're running this locally, use the Python queues.")
            self.out_queue = queue
        elif not self.local:
            self.aws_sqs_key, self.aws_sqs_secret, self.aws_sqs_url = self.__load_SQS_creds()
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
            cg_api = CoinGeckoAPI(local=self.local)
            api_list.append(cg_api)
        return api_list

    def __run_processes(self):
        for i in range(self.process_count):
            process_name = "Process-" + str(i + 1) + "-" + str(multiprocessing.current_process())
            list_to_use = list(self.split_pull_list[i])
            if self.local:
                process_obj = SingleProcessor(local=self.local, process_type=self.process_type, cg_api=self.api_list[i],
                                              proc_name=process_name, queue=self.out_queue)
            else:
                process_obj = SingleProcessor(local=self.local, process_type=self.process_type, cg_api=self.api_list[i],
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
    def __init__(self, local, process_type, cg_api, proc_name, queue=None, sqs_key=None, sqs_secret=None, sqs_url=None):
        self.local = local
        self.process_type = process_type
        self.cg_api = cg_api
        self.process_name = proc_name
        if queue:
            self.end_queue = queue
        else:
            self.sqs_key = sqs_key
            self.sqs_secret = sqs_secret
            self.sqs_url = sqs_url

    def message_sender(self, message):
        if self.local:
            self.end_queue.put(message)
        else:
            send_message(message=message, sqs_key=self.sqs_key, sqs_secret=self.sqs_secret, sqs_url=self.sqs_url)

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
        self.message_sender(message="STOP")
        if self.local:
            self.end_queue.close()
            self.end_queue.join_thread()
        print(f"Done with {self.process_name}")
        return

    def historical_daily_data_processor(self, input):
        """
        Function to query cg_api for Full Historical Data (Developer, Community, Price, etc.)
        :param input: tuple of (date, coin_id) - date is in d-m-y format.
        :return: Nothing. Adds the result to the consumer queue.
        Result is in dict format: {"coin_id": {date: DATA}}
        """
        if self.local:
            max_retries = 5
            retries = 0
            try:
                print(f"Working on {input[1]} - {input[0]}")
                result = {input[1]: {input[0]: self.cg_api.get_coin_history_by_id(input[1], input[0])}}
                self.message_sender(message=result)
                return
            except requests.exceptions.HTTPError as e:
                print(f"Failed once, trying again. Error: {e}")
                retry = True
                if '429' in str(e):
                    while retry and (retries < max_retries):
                        print(f"Retry attempt nr. {retries + 1}")
                        time.sleep(((2 * 0.001) ** retries) * 100)
                        try:
                            result = {input[1]: {input[0]: self.cg_api.get_coin_history_by_id(input[1], input[0])}}
                            self.message_sender(message=result)
                            return
                        except requests.exceptions.HTTPError as e:
                            if '429' in str(e):
                                retry = True
                                retry += 1
                            else:
                                raise Exception(f"There's an unknown error: {e}")
                    raise Exception("We've reached max nr of retries")

        try:
            result = {input[1]: {input[0]: self.cg_api.get_coin_history_by_id(input[1], input[0])}}
        except requests.exceptions.HTTPError:
            print("Failed once, oops")
            result = {input[1]: {input[0]: self.cg_api.get_coin_history_by_id(input[1], input[0])}}

        self.message_sender(message=result)

    def historical_price_data_processor(self, input):
        """
        Function to query cg_api for Historical Price Data (price, market_cap, volume)
        :param input: tuple of (coin_id, vs_currency, timeframe). Defaults for vs_currency is USD and timeframe is MAX.
        :return: Nothing. Adds result to consumer queue.
        Result is in dict format: {"coin_id":
                {"prices": LIST OF LISTS, "market_caps": LIST OF LISTS, "total_volumes": LIST OF LISTS}}
        """

        result = self.cg_api.get_coin_market_chart_by_id(input, vs_currency='usd', days='max', interval='daily')
        result2 = {input: result}

        """
        NOT SURE WHAT THE IMPACT OF USING RESULT VS RESULT2 IS IN DOWNSTREAM CODE. NEED TO CLEAN UP AND FIGUE THAT OUT
        """
        if self.local:
            self.message_sender(message=result2)
        else:
            self.message_sender(message=result2)

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
        self.message_sender(message=result)