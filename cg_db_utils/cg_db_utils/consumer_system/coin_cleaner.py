import datetime


class HistCoinToProcess:
    """
    Takes as input an individual coin's data.
    This is the base class, and will be used for common data points between daily info + coin historical info
    """

    def __init__(self, data, crypto_id):
        self.id = crypto_id
        self.all_daily_data = data[crypto_id]
        self.developer_data_to_pop = ['code_additions_deletions_4_weeks']

        try:
            self.community_data = {i: self.all_daily_data[i]['community_data'] for i in self.all_daily_data.keys()}
        except KeyError:
            self.community_data = None

        try:
            self.developer_data = {i: self.all_daily_data[i]['developer_data'] for i in self.all_daily_data.keys()}
        except KeyError:
            self.developer_data = None

        try:
            self.public_interest = {i: self.all_daily_data[i]['public_interest_stats'] for i in
                                    self.all_daily_data.keys()}
        except KeyError:
            self.public_interest = None

        self.community_data_cleaned = {}
        self.developer_data_cleaned = {}
        self.public_interest_cleaned = {}

    def clean_all_data(self):
        self.__process_community()
        self.__process_developer()
        self.__process_interest()

    def __process_community(self):
        if self.community_data:
            self.community_data_cleaned = self.community_data
            for element in self.community_data_cleaned.keys():
                self.community_data_cleaned[element]["ID"] = str(self.id)
        else:
            self.community_data_cleaned = None

    def __process_developer(self):
        if self.developer_data:
            data_clean = {}
            for element in self.developer_data.keys():
                process = self.developer_data[element]
                if process['forks'] is None or process['stars'] is None or process['subscribers'] is None:
                    continue
                else:
                    process["ID"] = str(self.id)
                    process['code_additions_4w'] = process['code_additions_deletions_4_weeks']['additions']
                    process['code_deletions_4w'] = process['code_additions_deletions_4_weeks']['deletions']
                    for popper in self.developer_data_to_pop:
                        process.pop(popper)
                    data_clean[element] = process

            if data_clean:
                self.developer_data_cleaned = data_clean
            else:
                self.developer_data_cleaned = None
        else:
            self.developer_data_cleaned = None

    def __process_interest(self):
        if self.public_interest:
            for element in self.public_interest.keys():
                single_element = self.public_interest[element]
                single_element["ID"] = str(self.id)
                self.public_interest_cleaned[element] = single_element
        else:
            self.public_interest_cleaned = None


class DailyCoinProcess(HistCoinToProcess):
    def __init__(self, data, crypto_id):
        super().__init__(data, crypto_id)
        self.developer_data_to_pop = ['code_additions_deletions_4_weeks', 'last_4_weeks_commit_activity_series']
        self.day_data = self.all_daily_data[list(self.all_daily_data.keys())[0]]
        self.info_table_data_cleaned = {'ID': [self.day_data['id']],
                                        'symbol': [self.day_data['symbol']],
                                        'name': [self.day_data['name']],
                                        'block_time_in_minutes': [self.day_data['block_time_in_minutes']],
                                        'hashing_algorithm': [self.day_data['hashing_algorithm']],
                                        'homepage': [self.day_data["links"]["homepage"][0]],
                                        'is_active': [True]
                                        }

        try:
            self.scoring_data_cleaned = {'date_added': [datetime.date.strftime(datetime.date.today(), "%Y-%m-%d")],
                                         'ID': [self.day_data["id"]],
                                         'sentiment_votes_up_percentage': [self.day_data["sentiment_votes_up_percentage"]],
                                         'sentiment_votes_down_percentage': [self.day_data[
                                            "sentiment_votes_down_percentage"]],
                                         'coingecko_score': [self.day_data["coingecko_score"] if self.day_data["coingecko_score"] else None],
                                         'developer_score': [self.day_data["developer_score"]],
                                         'community_score': [self.day_data['community_score']],
                                         'liquidity_score': [self.day_data['liquidity_score']],
                                         'public_interest_score': [self.day_data['public_interest_score']]
                                         }
        except KeyError:
            self.scoring_data_cleaned = {'date_added': [datetime.date.strftime(datetime.date.today(), "%Y-%m-%d")],
                                         'ID': [self.day_data["id"]],
                                         'sentiment_votes_up_percentage': [
                                             self.day_data["sentiment_votes_up_percentage"]],
                                         'sentiment_votes_down_percentage': [self.day_data[
                                                                                 "sentiment_votes_down_percentage"]],
                                         'coingecko_score': [None],
                                         'developer_score': [self.day_data["developer_score"]],
                                         'community_score': [self.day_data['community_score']],
                                         'liquidity_score': [self.day_data['liquidity_score']],
                                         'public_interest_score': [self.day_data['public_interest_score']]
                                         }

        self.platforms = self.day_data['platforms'] if self.day_data['platforms'] is not None else None
        self.categories = self.day_data['categories'] if self.day_data['categories'] is not None else None

        self.platforms_cleaned = {}
        self.categories_cleaned = {}

    def clean_all_data(self):
        super().clean_all_data()
        self.__process_platforms()
        self.__process_categories()

    def __process_platforms(self):
        if self.platforms is None:
            return

        elif len(self.platforms.keys()) == 1 and self.platforms.pop(list(self.platforms.keys())[0]) == '':
            self.platforms_cleaned = None
            return
        else:
            platforms_dict = {'ID': [self.id for _ in range(len(self.platforms.keys()))],
                              'asset_platform_id': [plat_id for plat_id in self.platforms.keys()],
                              'platform_address': [address for address in self.platforms.values()]}
            self.platforms_cleaned = platforms_dict

    def __process_categories(self):
        if self.categories:
            cleaned_categories = []
            for category in self.categories:
                if category is not None:
                    cleaned_categories.append(category)
            dict_test = {'ID': [self.id for _ in range(len(cleaned_categories))],
                         'category': cleaned_categories}
            self.categories_cleaned = dict_test

        else:
            return


class HistPriceCoinProcess:
    def __init__(self, data, crypto_id):
        self.crypto_id = crypto_id
        self.all_prices = data[crypto_id]['prices']
        self.all_volumes = data[crypto_id]['total_volumes']
        self.all_market_caps = data[crypto_id]['market_caps']

        self.cleaned_hist_price = {}

    def clean_all_data(self):
        self.__process_hist_prices()

    def __process_hist_prices(self):
        clean_dict = {'timestamp': [datetime.datetime.fromtimestamp(day[0] / 1000) for day in self.all_prices],
                      'clean_date': [datetime.date.fromtimestamp(day[0] / 1000) for day in self.all_prices],
                      'ID': self.crypto_id, 'price': [price[1] for price in self.all_prices],
                      'volume': [volume[1] for volume in self.all_volumes],
                      'market_cap': [market_cap[1] for market_cap in self.all_market_caps]}

        self.cleaned_hist_price = clean_dict
        return self.cleaned_hist_price
