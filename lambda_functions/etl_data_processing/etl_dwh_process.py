import json
import os
from etl_dwh_tables import etl_categories
try:
    import cg_db_utils.cg_db_utils.DB_connector.get_db_connection as connector
except ModuleNotFoundError:
    import cg_db_utils.DB_connector.get_db_connection as connector


def etl_db_table_create(event, context):
    if os.environ.get("AWS_EXECUTION_ENV") is None:
        local = True
    else:
        local = False
    engine = connector.get_db_connections(local=local)
    etl_categories(engine)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }