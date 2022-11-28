"""
Create schema and drops and (re)create tables.

Run this file to reset tables before each time the ETL scripts are run.
"""
import configparser
import psycopg2
from sql_queries import create_table_queries, \
    drop_table_queries, create_schema


def drop_tables(cur, conn):
    """
    Drop each table using the queries in `drop_table_queries` list.

    :param cur: cursor object
    :param conn: connection object
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print(query)


def create_tables(cur, conn):
    """
    Create each table using the queries in `create_table_queries` list.

    :param cur: cursor object
    :param conn: connection object
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        print(query)


def main():
    """Drop (if exists) and Create the sparkify database."""
    # get AWS configs
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # establishe connection with db and get cursor
    con_meta = "host={} dbname={} user={} password={} port={}"
    conn = psycopg2.connect(con_meta.format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    # create schema
    cur.execute(create_schema)
    conn.commit()
    # perform operations
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
