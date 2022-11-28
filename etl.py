"""
File to read and process data and load them in tables.

The file reads and processes data from the `song_data`
and `log_data` folders and load them into tables.
"""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data to staging tables from s3.

    The function copies data from s3 to
    `stagging_log_data` and `stagging_song_data` tables.
    :param cur: cursor object
    :param filepath: str file path
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print(query)


def insert_tables(cur, conn):
    """
    Insert data to fact and dimension tables.

    The function executes an insert statement to add data
    from `stagging_log_data` and `stagging_song_data` tables
    to songplays, users, artists, songs, time
    :param cur: cursor object
    :param filepath: str file path
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print(query)


def main():
    """Load data to staging tables and insert to fact and dimension."""
    # get AWS configs
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # establishe connection with db and get cursor
    con_meta = "host={} dbname={} user={} password={} port={}"
    conn = psycopg2.connect(con_meta.format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    # perform operation
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
