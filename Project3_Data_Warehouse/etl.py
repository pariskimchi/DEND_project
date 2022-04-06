import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        Copy staging tables from S3 to redshift 
            using queries in `copy_table_queries` list     
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
        Insert staging tables data into eac Fact-Dimension tables 
            using queries in `insert_table_queries` list
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():

    """
        - Read AWS-configuration file through configparser
            and set config from `dwh.cfg` file

        - Creates connection using config

        - Copy staging table from s3 to redshift 
            using `load_staging_tables` function

        - Insert staging table data into Fact-Dimension tables 
            using `insert_tables` function
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()