import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
        Function to drop tables in the database if it exists before re-creating the tableself.
            Args:
                cur  : cusor object that connects to the redshift databaseself.
                conn : connection object to the server
            Returns:
                NONE
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        Function to crate tables in the database.
            Args:
                cur  : cusor object that connects to the redshift databaseself.
                conn : connection object to the server.
            Returns:
                NONE
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        Function to execute all drop table and create table queries imported from sql_queries.py
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

    conn    = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(host,
                                                                                      dbname,
                                                                                        user,
                                                                                      password,
                                                                                      port))
    cur     = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
