from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.logging import get_run_logger
from prefect_sqlalchemy import SqlAlchemyConnector
import requests
import pandas as pd
from sqlalchemy.types import JSON


@task()
def read_api(path):
    logger = get_run_logger()
    try:
        response = requests.get(f"https://fakestoreapi.com/{path}")
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)

        # Drop columns
        logger.info(f"Fetched {len(df)} records")
        return df
    except Exception as e:
        logger.error(f"Error: {str(e)}")

@task
def store_users_in_database(df):
    logger = get_run_logger()
    with SqlAlchemyConnector.load("postgres-credentials") as connector:
        engine = connector.get_engine()
        df.to_sql(
            name="bronze_users",
            con=engine,
            if_exists="append",
            index=False,
            dtype={'address': JSON, 'name': JSON}
        )
        logger.info(f"Stored {len(df)} users to database")


@task
def store_products_in_database(df):
    logger = get_run_logger()
    with SqlAlchemyConnector.load("postgres-credentials") as connector:
        engine = connector.get_engine()
        df.to_sql(
            name="bronze_products",
            con=engine,
            if_exists="append",
            index=False,
            dtype={'rating': JSON}
        )
        logger.info(f"Stored {len(df)} products to database")


@task
def store_carts_in_database(df):
    logger = get_run_logger()
    with SqlAlchemyConnector.load("postgres-credentials") as connector:
        engine = connector.get_engine()
        df.to_sql(
            name="bronze_carts",
            con=engine,
            if_exists="append",
            index=False,
            dtype={'products': JSON}
        )
        logger.info(f"Stored {len(df)} carts to database")
# Bronze tables
@flow
def bronze_users():
    # Read users from API
    df = read_api("/users")

    # Store bronze_users
    store_users_in_database(df)

@flow
def bronze_products():
    df = read_api("/products")

    store_products_in_database(df)


@flow
def bronze_carts():
    df = read_api("/carts")

    store_carts_in_database(df)

@flow
def main():
    # Brone tables
    bronze_users()
    bronze_products()
    bronze_carts()


#if __name__ == "__main__":
#    hello_world.serve(name="my-first-deployment", cron="* * * * *")
main()
