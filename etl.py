import configparser
from sqlalchemy import create_engine
import pandas as pd
from pathlib import Path

from sql_queries import drop_table_queries, create_table_queries, insert_table_queries


def stage_orders(conn):
    """
    Stages scraped orders from a S3 Bucket into a PostgreSQL database.

    Args:
        conn (engine):  an database engine object that can execute sql statements.

    Returns:
        None
    """

    s3_bucket = 's3://rebuy-momox-arbitrage'
    s3_files = [
        f'{s3_bucket}/orders/ordered_books.csv',
        f'{s3_bucket}/orders/order_overview.csv'
    ]

    table_names = [
        'staging_ordered_books',
        'stagingn_order_overview'
    ]

    for s3_file, table_name in zip(s3_files, table_names):

        print(f"Loading {s3_file}...\n")
        staging_table = pd.read_csv(s3_file)

        print(f"Stagig {table_name}\n")
        staging_table.to_sql(
            table_name,
            con=conn,
            index=False,
            method='multi',
            if_exists='replace'
        )

    print("Finished staginging orders...\n")


def stage_scraped_books(conn):
    """
    Stages scraped books from a local SQLlite3 Database into a PostgreSQL database.

    Args:
        conn (engine):  an database engine object that can execute sql statements.

    Returns:
        None
    """

    local_database_filepath = Path().cwd() / 'rebuy_momox.db'

    old_table_name = 'rebuy_momox'
    new_table_name = 'staging_scraped_books'

    sql = f"""
        SELECT *
        FROM {old_table_name}
    """
    engine = create_engine("sqlite:///" + local_database_filepath.as_posix())

    print(f"Loading {old_table_name}...\n")
    staging_table = pd.read_sql(
        sql=sql,
        con=engine,
        parse_dates=["timestamp"],
        coerce_float=True)

    for column in [
        "rebuy_price",
        "momox_price",
        "rebuy_publication_year",
        "momox_publication_year"
    ]:

        staging_table[column] = pd.to_numeric(staging_table[column])

    staging_table['date'] = staging_table['timestamp'].dt.date

    print(f"Stagig {new_table_name}...\n")
    staging_table.to_sql(
        new_table_name,
        con=conn,
        index=False,
        if_exists='replace'
    )

    print("Finished staginging scraped books...\n")


def check_row_count(conn):
    """
    Checks wether all fact and dimension tables have been properly built and have more than 0 rows.

    Args:
        conn (engine):  an database engine object that can execute sql statements.

    Returns:
        None
    """

    table_names = [
        "fact_scraped_books",
        "fact_rebuy_sales",
        "dim_book",
        "dim_order",
        "dim_time"
    ]

    for table_name in table_names:

        df = pd.read_sql(
            sql=f"SELECT COUNT(*) FROM {table_name}",
            con=conn
        )

        row_count = df['count'].loc[0]

        if row_count > 0:
            print(f"{table_name} passed test and has {row_count} rows\n")

        else:
            raise Exception("{table_name} has {row_count} rows\n")


def main():
    """
    Executes the whole ETL Pipelinne.

    """

    # reading in credentials
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    DWH_DB = config.get("DWH", "DWH_DB")
    DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
    DWH_PORT = config.get("DWH", "DWH_PORT")
    DWH_ENDPOINT = config.get("DWH", "DWH_ENDPOINT")
    AWS_KEY = config.get("AWS", "KEY")
    AWS_SECRET = config.get("AWS", "SECRET")

    conn_string = "postgresql://{}:{}@{}:{}/{}".format(
        DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB)

    conn = create_engine(conn_string)

    print("Staging Data...\n")
    stage_orders(conn)

    # Legacy SQLlite3 DB stored locally and to be migrated to S3
    # since it is not shared on GitHub and only locally available this function is commented out
    # stage_scraped_books(conn)

    with conn.connect() as connection:

        print("Droping Tables...\n")
        for sql in drop_table_queries:
            connection.execute(sql)

        print("Creating Tables...\n")
        for sql in create_table_queries:
            connection.execute(sql)

        print("Creating Fact and Dimension Tables...\n")
        for sql in insert_table_queries:
            connection.execute(sql)

        print("Running tests...")

    # Data quality checks
    check_row_count(conn)


if __name__ == "__main__":
    main()
