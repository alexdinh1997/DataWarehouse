import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop existing tables by accessing to sql_queries.py
    parameter:
        - cur: for connection cursor with inserting the data in DB
        - conn: make connection to DB
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    create new tables by accessing to sql_queries.py
    parameter:
        - cur: for connection cursor with inserting the data in DB
        - conn: make connection to DB
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    #read file dwh.cfg to get the cluster descriptions
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()