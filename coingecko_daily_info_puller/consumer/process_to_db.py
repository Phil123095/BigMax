import datetime
import time
import pandas as pd
import threading
from pangres import upsert
import numpy as np


class BaseDatabaseProcessor:
    def __init__(self, threshold=100):
        self.coin_counter = 0
        self.all_coins_counter = 0
        self.threshold = threshold

    def receive_connection(self, connection):
        self.connection = connection

    def data_aggregate(self, coin_data):
        if coin_data:
            self.coin_counter += 1
            self.data_appending(coin_data)

            if self.coin_counter >= self.threshold:
                self.data_to_db()
                self.all_coins_counter += self.coin_counter
                self.coin_counter = 0
                print(f"{self.all_coins_counter} coins processed")

        else:
            print(self.all_coins_counter)
            self.data_to_db()
            self.all_coins_counter += self.coin_counter
            print(f"Processing of all {self.all_coins_counter} coins is done.")

    def data_appending(self, coin_data):
        pass

    def __dict_cleaner(self, list_of_dicts):
        pass

    def data_to_db(self):
        pass


class FullHistoryDatabaseProcessor(BaseDatabaseProcessor):
    def __init__(self, threshold=100):
        BaseDatabaseProcessor.__init__(self, threshold)
        self.community_data_list = []
        self.developer_data_list = []
        self.public_interest_list = []

    def data_appending(self, coin_data):
        if coin_data.community_data_cleaned:
            self.community_data_list.append(coin_data.community_data_cleaned)
        if coin_data.developer_data_cleaned:
            self.developer_data_list.append(coin_data.developer_data_cleaned)
        if coin_data.public_interest_cleaned:
            self.public_interest_list.append(coin_data.public_interest_cleaned)

    def __dict_cleaner(self, list_of_dicts):
        cleaned_list = []
        for element in list_of_dicts:
            data = element[list(element.keys())[0]]
            date = datetime.datetime.strptime(list(element.keys())[0], "%d-%m-%Y")
            data['date'] = datetime.datetime.strftime(date, "%Y-%m-%d")
            cleaned_list.append(data)
        return cleaned_list

    def data_to_db(self):
        print("Processing to DB")
        try:
            community_df = pd.DataFrame(self.__dict_cleaner(self.community_data_list))
            developer_df = pd.DataFrame(self.__dict_cleaner(self.developer_data_list))
            public_interest_df = pd.DataFrame(self.__dict_cleaner(self.public_interest_list))
        except ValueError:
            return

        try:
            community_df['ID'] = community_df['ID'].apply(lambda x: str(x))
            developer_df['ID'] = developer_df['ID'].apply(lambda x: str(x))
            public_interest_df['ID'] = public_interest_df['ID'].apply(lambda x: str(x))

        except KeyError:
            return

        community_df.to_sql('cg_community', self.connection, if_exists='append', index=False)
        developer_df.to_sql('cg_developer', self.connection, if_exists='append', index=False)
        public_interest_df.to_sql('cg_public_ranking', self.connection, if_exists='append', index=False)

        print("Uploaded to DB")

        self.community_data_list, self.developer_data_list, self.public_interest_list = [], [], []


class DayInfoDatabaseProcessor(FullHistoryDatabaseProcessor):
    def __init__(self, threshold=100):
        FullHistoryDatabaseProcessor.__init__(self, threshold)
        self.general_data_list = []
        self.categories_data_list = []
        self.platforms_data_list = []
        self.scoring_list = []

    def check_non_active(self, cg_coin_list):
        query = "Select distinct(ID) from coingeckodb.cg_general_info"
        db_data = pd.read_sql(query, con=self.connection)
        if len(db_data) > len(cg_coin_list):
            out_result_df = pd.DataFrame({'ID':
                                              np.setdiff1d(db_data['ID'].to_numpy(),
                                                           np.array(cg_coin_list))})

            out_result_df['is_active'] = False
            out_result_df = out_result_df.set_index(['ID'])

            upsert(con=self.connection,
                   df=out_result_df,
                   table_name='cg_general_info',
                   if_row_exists='update')

            print(out_result_df)

        else:
            print("No dead coins")

    def data_appending(self, coin_data):
        FullHistoryDatabaseProcessor.data_appending(self, coin_data)

        if coin_data.categories_cleaned is not None:
            self.categories_data_list.append(pd.DataFrame(coin_data.categories_cleaned))
        if coin_data.platforms_cleaned is not None:
            self.platforms_data_list.append(pd.DataFrame(coin_data.platforms_cleaned))
        if coin_data.info_table_data_cleaned is not None:
            self.general_data_list.append(pd.DataFrame(coin_data.info_table_data_cleaned))
        if coin_data.scoring_data_cleaned is not None:
            self.scoring_list.append(pd.DataFrame(coin_data.scoring_data_cleaned))

    def data_to_db(self):
        """
        Uploading data to database. Some data needs to be upserted (Updated/Inserted), other data needs to be appended.
        Upserting:
            - general_df --> Info Table
            - categories_df --> Categories Table
            - platforms_df --> Platforms Table
        Appending:
            - scoring_df --> Scoring Table
        :return:
        """
        FullHistoryDatabaseProcessor.data_to_db(self)
        try:
            general_df = pd.concat(self.general_data_list, ignore_index=True)
            categories_df = pd.concat(self.categories_data_list, ignore_index=True)
            platforms_df = pd.concat(self.platforms_data_list, ignore_index=True)
            scoring_df = pd.concat(self.scoring_list, ignore_index=True)
        except ValueError:
            return

        try:
            general_df['ID'] = general_df['ID'].apply(lambda x: str(x))
            categories_df['ID'] = categories_df['ID'].apply(lambda x: str(x))
            platforms_df['ID'] = platforms_df['ID'].apply(lambda x: str(x))
            scoring_df['ID'] = scoring_df['ID'].apply(lambda x: str(x))
        except KeyError:
            return

        """UPSERT TABLES"""
        with self.connection.connect() as cur:
            # Upsert (Update and Insert) the coin info data.
            general_df = general_df.set_index(['ID'])
            categories_df = categories_df.set_index(['ID', 'category'])
            platforms_df = platforms_df.set_index(['ID', 'asset_platform_id'])

            upsert(con=cur,
                   df=general_df,
                   table_name='cg_general_info',
                   if_row_exists='update')

            upsert(con=cur,
                   df=categories_df,
                   table_name='cg_categories',
                   if_row_exists='update')

            upsert(con=cur,
                   df=platforms_df,
                   table_name='cg_platforms',
                   if_row_exists='update')

        """APPEND TABLES"""
        scoring_df.to_sql('cg_scoring_table', self.connection, if_exists='append', index=False)

        self.general_data_list, self.categories_data_list, self.platforms_data_list, self.scoring_list = [], [], [], []


class HistPriceDatabaseProcessor:
    def __init__(self, threshold):
        self.coin_counter = 0
        self.all_coins_counter = 0
        self.threshold = threshold
        self.market_data_list = []

    def receive_connection(self, connection):
        self.connection = connection

    def data_aggregate(self, coin_data):
        if coin_data:
            self.coin_counter += 1
            self.data_appending(coin_data)

            if self.coin_counter >= self.threshold:
                self.data_to_db()
                self.all_coins_counter += self.coin_counter
                self.coin_counter = 0
                print(f"{self.all_coins_counter} coins processed")

        else:
            print(self.all_coins_counter)
            self.data_to_db()
            self.all_coins_counter += self.coin_counter
            print(f"Processing of all {self.all_coins_counter} coins is done.")

    def data_appending(self, coin_data):
        clean_df = pd.DataFrame(coin_data.cleaned_hist_price)
        self.market_data_list.append(clean_df)

    def data_to_db(self):
        market_df = pd.concat(self.market_data_list, ignore_index=True)

        market_df = market_df.astype({"ID": str})

        market_df['timestamp'] = market_df['timestamp'].apply(lambda x: datetime.datetime.strftime(x, "%Y-%m-%d %H:%M:%S"))
        market_df['clean_date'] = market_df['clean_date'].apply(
            lambda x: datetime.date.strftime(x, "%Y-%m-%d"))

        market_df['ID'] = market_df['ID'].apply(lambda x: str(x))
        market_df = market_df.astype({"clean_date": str, "ID": str, "price": float, "volume": float, "market_cap": float})

        print(f"Adding data to SQL Table for {len(market_df)} rows")
        tic = time.perf_counter()

        self.multi_thread_db(market_df, table_name='cg_hist_prices')
        toc = time.perf_counter()
        print(f"Data upload done in {toc - tic} seconds")

        self.market_data_list = []

    def multi_thread_db(self, data, table_name):
        CHUNKSIZE = 1000
        df = data
        workers = []
        for x in range(len(df) // CHUNKSIZE):
            t = threading.Thread(
                target=lambda: df.iloc[x * CHUNKSIZE: (x + 1) * CHUNKSIZE, :].to_sql(
                    table_name,
                    self.connection,
                    if_exists='append',
                    index=False,
                    method='multi'))
            t.start()
            workers.append(t)
        print('total number of threads:', len(workers))
        [t.join() for t in workers]

