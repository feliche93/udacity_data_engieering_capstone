import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
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

    conn = psycopg2.connect(conn_string)

    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
