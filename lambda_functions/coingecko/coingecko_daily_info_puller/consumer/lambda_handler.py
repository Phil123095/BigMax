import json
import os

try:
    import cg_db_utils.cg_db_utils.DB_connector.get_db_connection as connector
    import cg_db_utils.cg_db_utils.consumer_system.coin_cleaner as cc
    import cg_db_utils.cg_db_utils.consumer_system.database_processor as db
except ModuleNotFoundError:
    import cg_db_utils.DB_connector.get_db_connection as connector
    import cg_db_utils.consumer_system.coin_cleaner as cc
    import cg_db_utils.consumer_system.database_processor as db


def lambda_handler(event, context):
    if os.environ.get("AWS_EXECUTION_ENV") is None:
        local = True
    else:
        local = False

    database = db.DayInfoDatabaseProcessor(threshold=len(event['Records']))
    database.receive_connection(connector.get_db_connections(local=local))
    for message in event['Records']:
        message_body = json.loads(message['body'])
        print(message_body)
        try:
            coin_id = list(message_body.keys())[0]
        except AttributeError:
            return
        coin_cleaned = cc.DailyCoinProcess(message_body, coin_id)

        coin_cleaned.clean_all_data()
        database.data_aggregate(coin_cleaned)
    return {'Status': '200'}
