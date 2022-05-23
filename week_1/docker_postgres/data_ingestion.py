import argparse
import time
import requests
import pandas as pd
from sqlalchemy import create_engine


def args_init():
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()
    return args

def main(args):
    user = args.user
    password = args.password
    host = args.host
    port = args.port
    db = args.db
    table_name = args.table_name
    url = args.url
    csv_name = "output.csv.gz"
    
    res = requests.get(url)
    if res.status_code != 200:
        raise Exception("Something went wrong downloading csv file!")
    with open(csv_name, "wb") as f:
        f.write(res.content)
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
            
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100_000, compression="gzip")
    for df in df_iter:
        t_start = time.time()
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time.time()
        print('inserted another chunk, took %.3f second' % (t_end - t_start))
    

if __name__ == "__main__":
    args = args_init()
    main(args)