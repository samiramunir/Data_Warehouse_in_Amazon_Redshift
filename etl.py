import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        Function to copy data from S3 bucket to staging tables.
            Args:
                cur  : cusor object that connects to the redshift databaseself.
                conn : connection object to the server
            Returns:
                NONE
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
        Function to insert data into the final tables from staging tables.
            Args:
                cur  : cusor object that connects to the redshift databaseself.
                conn : connection object to the server
            Returns:
                NONE
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        Function to call on the load_staging_tables and insert_tables functions
        for all load and insert queries from sql_queries.py

            Args:
                NONE
            Returns:
                NONE
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    host        = config.get("CLUSTER","HOST")
    dbname      = config.get("CLUSTER","DB_NAME")
    user        = config.get("CLUSTER","DB_USER")
    password    = config.get("CLUSTER","DB_PASSWORD")
    port        = config.get("CLUSTER","DB_PORT")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(host,
                                                                                   dbname,
                                                                                   user,
                                                                                   password,
                                                                                   port))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
