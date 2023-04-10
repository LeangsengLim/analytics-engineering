#!/usr/bin/env python
# coding: utf-8


# import argparse to create arguments
import argparse
import os
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine



def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    data_directory = r'data/'
    csv_name = 'green_tripdata_2019-01.parquet'

    # Download the csv
    os.system(f"wget {url} -O {data_directory + csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    ##trips = pd.read_parquet('data/green_tripdata_2019-01.parquet', engine='pyarrow')

    ##trips = pd.read_parquet(csv_name, engine='pyarrow', compression='gzip')
    trips = pd.read_parquet(data_directory + csv_name, engine='pyarrow')

    trips['lpep_pickup_datetime'] = pd.to_datetime(trips['lpep_pickup_datetime'])
    trips['lpep_dropoff_datetime'] = pd.to_datetime(trips['lpep_dropoff_datetime'])


    # to create table schema
    trips.head(n=0).to_sql(name='green_taxi_data', con=engine, if_exists='replace')

    trips.to_sql(name='green_taxi_data', con=engine, if_exists='append')

    # print schema of the data
    print(pd.io.sql.get_schema(trips, name='green_tripdata'))



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')


    # user, password, host, port, database name, table name
    # url of the csv


    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of table where we insert data to postgres')
    parser.add_argument('--url', help='url of data to postgres')

    args = parser.parse_args()

    main(args)









