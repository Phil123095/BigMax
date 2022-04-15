import json
import os
import process_to_db as db

try:
    import cg_db_utils.cg_db_utils.DB_connector.get_db_connection as connector
except ModuleNotFoundError:
    import cg_db_utils.DB_connector.get_db_connection as connector

import coin_cleaner as cc


def lambda_handler(event, context):
    if os.environ.get("AWS_EXECUTION_ENV") is None:
        local = True
    else:
        local = False
    for message in event['Messages']:
        message_body = json.loads(message['Body'])
        print(message_body)
        coin_id = list(message_body.keys())[0]
        database = db.DayInfoDatabaseProcessor(threshold=1)
        coin_cleaned = cc.DailyCoinProcess(message_body, coin_id)
        database.receive_connection(connector.get_db_connections(local=local))

        coin_cleaned.clean_all_data()
        database.data_aggregate(coin_cleaned)
    return {'Status': '200'}
