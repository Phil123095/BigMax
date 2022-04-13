from sqlalchemy import create_engine, engine
import os
import dotenv


def get_db_connections(local):
    if local:
        dotenv.load_dotenv()

    db_user = os.environ['DB_USER']
    password = os.environ['DB_PASS']
    db_host = os.environ['DB_HOST']
    df_name = os.environ['DB_NAME']

    engine_final = create_engine(
        engine.url.URL.create(
            drivername="mysql+mysqlconnector",
            username=db_user,  # e.g. "my-database-user"
            password=password,  # e.g. "my-database-password"
            host=db_host,  # e.g. "127.0.0.1"
            port=3306,  # e.g. 3306
            database=df_name  # e.g. "my-database-name",
        )
    )
    print("Connection established.")
    return engine_final