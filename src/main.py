from prefect import flow, task
from prefect.logging import get_run_logger
from prefect_sqlalchemy import SqlAlchemyConnector, ConnectionComponents, SyncDriver
import requests
import pandas as pd
from sqlalchemy.types import JSON
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import inspect
from datetime import datetime
import os

@task()
def read_api(path) -> pd.DataFrame:
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
def read_table_into_df(tablename):
    with SqlAlchemyConnector.load("postgres-credentials") as connector:
        engine = connector.get_engine()
        df = pd.read_sql_table(
            tablename,
            con=engine
        )
        return df

# Bronze tables
@flow
def bronze_users():
    # Read users from API
    df = read_api("/users")

    # Store bronze_users
    logger = get_run_logger()
    with SqlAlchemyConnector.load("postgres-credentials") as connector:
        engine = connector.get_engine()
        insp = inspect(engine)
        check = insp.has_table("bronze_users")
        if check:
            existing_ids = pd.read_sql_query("SELECT id FROM bronze_users", con=engine)
            df_filtered = df[~df['id'].isin(existing_ids['id'])]
            df_filtered.to_sql(
                name="bronze_users",
                con=engine,
                if_exists="append",
                index=False,
                dtype={'address': JSON, 'name': JSON}
            )
            logger.info(f"Stored {len(df_filtered)} users to database")
        else:
            df.to_sql(
                name="bronze_users",
                con=engine,
                if_exists="append",
                index=False,
                dtype={'address': JSON, 'name': JSON}
            )
            logger.info(f"Stored {len(df)} users to database")

@flow
def bronze_products():
    df = read_api("/products")

    logger = get_run_logger()
    with SqlAlchemyConnector.load("postgres-credentials") as connector:
        engine = connector.get_engine()
        insp = inspect(engine)
        check = insp.has_table("bronze_products")
        if check:
            existing_ids = pd.read_sql_query("SELECT id FROM bronze_products", con=engine)
            df_filtered = df[~df['id'].isin(existing_ids['id'])]
            df_filtered.to_sql(
                name="bronze_products",
                con=engine,
                if_exists="append",
                index=False,
                dtype={'rating': JSON}
            )
            logger.info(f"Stored {len(df_filtered)} products to database")
        else:
            df.to_sql(
                name="bronze_products",
                con=engine,
                if_exists="append",
                index=False,
                dtype={'rating': JSON}
            )
            logger.info(f"Stored {len(df)} products to database")

@flow
def bronze_carts():
    df = read_api("/carts")

    logger = get_run_logger()
    with SqlAlchemyConnector.load("postgres-credentials") as connector:
        engine = connector.get_engine()
        insp = inspect(engine)
        check = insp.has_table("bronze_carts")
        if check:
            existing_ids = pd.read_sql_query("SELECT id FROM bronze_carts", con=engine)
            df_filtered = df[~df['id'].isin(existing_ids['id'])]
            df_filtered.to_sql(
                name="bronze_carts",
                con=engine,
                if_exists="append",
                index=False,
                dtype={'products': JSON}
            )
            logger.info(f"Stored {len(df_filtered)} carts to database")
        else:
            df.to_sql(
                name="bronze_carts",
                con=engine,
                if_exists="append",
                index=False,
                dtype={'products': JSON}
            )
            logger.info(f"Stored {len(df)} carts to database")

# Silver tables
@flow
def silver_users():
    logger = get_run_logger()
    df = read_table_into_df("bronze_users")
    name = df["name"].apply(pd.Series)

    df2 = df[["id", "email", "username", "phone"]]
    df2["first_name"] = name["firstname"]
    df2["last_name"] = name["lastname"]
    df2["ingestion_date"] = datetime.today()

    with SqlAlchemyConnector.load("postgres-credentials") as connector:
        engine = connector.get_engine()
        insp = inspect(engine)
        check = insp.has_table("silver_users")
        if check:
            existing_ids = pd.read_sql_query("SELECT id FROM silver_users", con=engine)
            df2_filtered = df2[~df2['id'].isin(existing_ids['id'])]
            df2_filtered.to_sql(
                name="silver_users",
                con=engine,
                if_exists="append",
                index=False
            )
            logger.info(f"Stored {len(df2_filtered)} users to database")
        else:
            df2.to_sql(
                name="silver_users",
                con=engine,
                if_exists="append",
                index=False
            )
            logger.info(f"Stored {len(df2)} users to database")
@flow
def silver_geolocation():
    logger = get_run_logger()
    df = read_table_into_df("bronze_users")
    address = pd.json_normalize(df["address"], sep="_")

    df2 = df[["id"]]
    df2["city"] = address["city"]
    df2["lat"] = address["geolocation_lat"]
    df2["long"] = address["geolocation_long"]
    df2["street"] = address["street"]
    df2["number"] = address["number"]
    df2["zip_code"] = address["zipcode"]
    df2["ingestion_date"] = datetime.today()

    df2.rename(columns={"id": "user_id"}, inplace=True)

    with SqlAlchemyConnector.load("postgres-credentials") as connector:
        engine = connector.get_engine()
        insp = inspect(engine)
        check = insp.has_table("silver_geolocation")
        if check:
            existing_ids = pd.read_sql_query("SELECT user_id FROM silver_geolocation", con=engine)
            df2_filtered = df2[~df2['user_id'].isin(existing_ids['user_id'])]
            df2_filtered.to_sql(
                name="silver_geolocation",
                con=engine,
                if_exists="append",
                index=False
            )
            logger.info(f"Stored {len(df2_filtered)} users to database")
        else:
            df2.to_sql(
                name="silver_geolocation",
                con=engine,
                if_exists="append",
                index=False
            )
            logger.info(f"Stored {len(df2)} users to database")

@flow
def silver_products():
    logger = get_run_logger()
    df = read_table_into_df("bronze_products")
    rating = pd.json_normalize(df["rating"], sep="_")

    df2 = df[["id","title", "price", "description", "category", "image"]]
    df2["rating_rate"] = rating["rate"]
    df2["rating_count"] = rating["count"]
    df2["ingestion_date"] = datetime.today()

    df2.rename(columns={"id": "user_id"})

    with SqlAlchemyConnector.load("postgres-credentials") as connector:
        engine = connector.get_engine()
        insp = inspect(engine)
        check = insp.has_table("silver_products")
        if check:
            existing_ids = pd.read_sql_query("SELECT id FROM silver_products", con=engine)
            df2_filtered = df2[~df2['id'].isin(existing_ids['id'])]
            df2_filtered.to_sql(
                name="silver_products",
                con=engine,
                if_exists="append",
                index=False
            )
            logger.info(f"Stored {len(df2_filtered)} users to database")
        else:
            df2.to_sql(
                name="silver_products",
                con=engine,
                if_exists="append",
                index=False
            )
            logger.info(f"Stored {len(df2)} users to database")

@flow
def silver_carts():
    logger = get_run_logger()
    df = read_table_into_df("bronze_carts")

    df2 = df.explode("products")
    products = pd.json_normalize(df2["products"])
    df2["product_id"] = products["productId"]
    df2["product_quantity"] = products["quantity"]
    df2["ingestion_date"] = datetime.today()
    df2["date"] = df2["date"].apply(lambda x: datetime.fromisoformat(x.replace('Z', '+00:00')))
    df2.drop(["products", "__v"], axis=1, inplace=True)

    df2.rename(columns={"userId": "user_id"}, inplace=True)

    with SqlAlchemyConnector.load("postgres-credentials") as connector:
        engine = connector.get_engine()
        insp = inspect(engine)
        check = insp.has_table("silver_carts")
        if check:
            existing_ids = pd.read_sql_query("SELECT id FROM silver_carts", con=engine)
            df2_filtered = df2[~df2['id'].isin(existing_ids['id'])]
            df2_filtered.to_sql(
                name="silver_carts",
                con=engine,
                if_exists="append",
                index=False
            )
            logger.info(f"Stored {len(df2_filtered)} users to database")
        else:
            df2.to_sql(
                name="silver_carts",
                con=engine,
                if_exists="append",
                index=False
            )
            logger.info(f"Stored {len(df2)} users to database")

# Gold tables
@flow
def dim_users():
    logger = get_run_logger()
    df = read_table_into_df("silver_users")

    df2 = df[["id", "email", "phone", "ingestion_date"]]

    with SqlAlchemyConnector.load("postgres-credentials") as connector:
        engine = connector.get_engine()
        insp = inspect(engine)
        check = insp.has_table("dim_users")
        if check:
            existing_ids = pd.read_sql_query("SELECT id FROM dim_users", con=engine)
            df2_filtered = df2[~df2['id'].isin(existing_ids['id'])]
            df2_filtered.to_sql(
                name="dim_users",
                con=engine,
                if_exists="append",
                index=False
            )
            logger.info(f"Stored {len(df2_filtered)} users to database")
        else:
            df2.to_sql(
                name="dim_users",
                con=engine,
                if_exists="append",
                index=False
            )
            logger.info(f"Stored {len(df2)} users to database")

@flow
def dim_geolocation():
    logger = get_run_logger()
    df = read_table_into_df("silver_geolocation")

    df2 = df[["user_id", "city", "lat", "long", "ingestion_date"]]
    df2["id"] = df2.index + 1
    df3 = df2[["id", "user_id", "city", "lat", "long"]]

    with SqlAlchemyConnector.load("postgres-credentials") as connector:
        engine = connector.get_engine()
        insp = inspect(engine)
        check = insp.has_table("dim_geolocation")
        if check:
            existing_ids = pd.read_sql_query("SELECT user_id FROM dim_geolocation", con=engine)
            df3_filtered = df3[~df3['user_id'].isin(existing_ids['user_id'])]
            df3_filtered.to_sql(
                name="dim_geolocation",
                con=engine,
                if_exists="append",
                index=False
            )
            logger.info(f"Stored {len(df3_filtered)} geolocations to database")
        else:
            df3.to_sql(
                name="dim_geolocation",
                con=engine,
                if_exists="append",
                index=False
            )
            logger.info(f"Stored {len(df3)} geolocations to database")

@flow
def dim_products():
    logger = get_run_logger()
    df = read_table_into_df("silver_products")

    df2 = df[["id", "title", "price", "category", "rating_rate", "rating_count", "ingestion_date"]]

    with SqlAlchemyConnector.load("postgres-credentials") as connector:
        engine = connector.get_engine()
        insp = inspect(engine)
        check = insp.has_table("dim_products")
        if check:
            existing_ids = pd.read_sql_query("SELECT id FROM dim_products", con=engine)
            df2_filtered = df2[~df2['id'].isin(existing_ids['id'])]
            df2_filtered.to_sql(
                name="dim_products",
                con=engine,
                if_exists="append",
                index=False
            )
            logger.info(f"Stored {len(df2_filtered)} products to database")
        else:
            df2.to_sql(
                name="dim_products",
                con=engine,
                if_exists="append",
                index=False
            )
            logger.info(f"Stored {len(df2)} products to database")

@flow
def dim_carts():
    logger = get_run_logger()
    df = read_table_into_df("silver_carts")

    df2 = df[["id", "product_id", "user_id", "product_quantity", "date", "ingestion_date"]]

    with SqlAlchemyConnector.load("postgres-credentials") as connector:
        engine = connector.get_engine()
        insp = inspect(engine)
        check = insp.has_table("dim_carts")
        if check:
            existing_ids = pd.read_sql_query("SELECT id FROM dim_carts", con=engine)
            df2_filtered = df2[~df2['id'].isin(existing_ids['id'])]
            df2_filtered.to_sql(
                name="dim_carts",
                con=engine,
                if_exists="append",
                index=False
            )
            logger.info(f"Stored {len(df2_filtered)} carts to database")
        else:
            df2.to_sql(
                name="dim_carts",
                con=engine,
                if_exists="append",
                index=False
            )
            logger.info(f"Stored {len(df2)} carts to database")

@flow
def fact_products_in_cart_by_day():

    logger = get_run_logger()
    with SqlAlchemyConnector.load("postgres-credentials") as connector:
        engine = connector.get_engine()
        df = pd.read_sql_query("""
            select count(distinct dc.product_id) as instances, du.id as user_id, dg.id as geolocation_id, dp.id as product_id , date_trunc('day', dc.date) as day
            from dim_carts dc
            join dim_products dp on dp.id = dc.product_id
            join dim_users du on du.id = dc.user_id
            join dim_geolocation dg on du.id = dg.user_id
            group by date_trunc('day', dc.date), du.id, dp.id, dg.id;
        """, con=engine)

        df.to_sql(
            name="fact_product_in_cart_by_day",
            con=engine,
            if_exists="replace",
            index=False
        )
        logger.info(f"Stored {len(df)} instances to database")

@flow
def main():

    # Register the block
    connector = SqlAlchemyConnector(
        connection_info=ConnectionComponents(
            driver=SyncDriver.POSTGRESQL_PSYCOPG2,
            username=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=5432,
            database="prefect",
        )
    )
    connector.save("postgres-credentials", overwrite=True)

    # Bronze tables
    bronze_users()
    bronze_products()
    bronze_carts()

    # Silver tables
    silver_users()
    silver_geolocation()
    silver_products()
    silver_carts()

    # Data warehouse (gold tables)
    dim_users()
    dim_geolocation()
    dim_products()
    dim_carts()
    fact_products_in_cart_by_day()


if __name__ == "__main__":
    main.serve()
