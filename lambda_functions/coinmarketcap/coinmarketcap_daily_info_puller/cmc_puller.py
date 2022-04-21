import json
import os
import dotenv
from coinmarketcapapi import CoinMarketCapAPI
import supporting_functions as cg_script
try:
    import cg_db_utils.cg_db_utils.DB_connector.get_db_connection as connector
except ModuleNotFoundError:
    import cg_db_utils.DB_connector.get_db_connection as connector


def cmc_lambda_handler(event, context):
    if os.environ.get("AWS_EXECUTION_ENV") is None:
        local = True
        dotenv.load_dotenv()
    else:
        local = False

    engine = connector.get_db_connections(local=local)
    cmc_api = CoinMarketCapAPI(os.environ['CMC_API_KEY'])

    list_of_coins = cg_script.get_all_coins_cmc(cmc_api, engine)
    print("Coin list retrieval done")

    cg_script.get_coin_info_cmc(list_of_coins, cmc_api, engine)
    print("DB upload done")
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
