import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Copies staging tables as defined in sql_queries.py.
            Parameters:
                    cur (cursor): cursor connection to the Redshift DB
                    filepath (string): AWS DB details and credentials
            Returns:
                    None.
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Inserts data from staging tables using queries defined in sql_queries.py.
            Parameters:
                    cur (cursor): cursor connection to the Redshift DB
                    conn (string): AWS DB details and credentials 
            Returns:
                    None.
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Main function of this script. Loads data from S3 into staging tables on Redshift and then processes that data into the analytics tables on Redshift.
            Parameters:
                    None.
            Returns:
                    None.
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()