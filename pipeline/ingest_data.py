#!/usr/bin/env python
# coding: utf-8

# In[2]:


import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm



dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

@click.command()
@click.option("--pg-user", default="root", show_default=True, help="Postgres user")
@click.option("--pg-password", default="root", show_default=True, help="Postgres password")
@click.option("--pg-host", default="localhost", show_default=True, help="Postgres host")
@click.option("--pg-port", default=5432, show_default=True, help="Postgres port")
@click.option("--pg-db", default="ny_taxi", show_default=True, help="Postgres database name")
@click.option("--target-table", default="yellow_taxi_data", show_default=True, help="Target table name")
@click.option("--year", default=2021, show_default=True, type=int, help="Year of data")
@click.option("--month", default=1, show_default=True, type=int, help="Month of data (1-12)")
@click.option("--chunksize", default=10000, show_default=True, type=int, help="Pandas read_csv chunksize")
@click.option("--url-prefix", default="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/", show_default=True, help="URL prefix for data files")
def main(pg_user, pg_password, pg_host, pg_port, pg_db, target_table, year, month, chunksize, url_prefix):
    """
    Ingest yellow taxi data for a given year/month into Postgres.
    """
    url = f"{url_prefix}yellow_tripdata_{year}-{month:02d}.csv.gz"
    engine = create_engine(f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}")

    first = True
    try:
        df_iter = pd.read_csv(url, dtype=dtype, parse_dates=parse_dates, iterator=True, chunksize=chunksize)
    except Exception as e:
        click.echo(f"Failed to open URL {url}: {e}", err=True)
        raise

    for df_chunk in tqdm(df_iter, desc="ingesting", unit="chunk"):
        if first:
            # create table schema (empty) replacing any existing table
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists="replace", index=False)
            click.echo(f"Table '{target_table}' created or replaced.")
            first = False

        # append chunk
        df_chunk.to_sql(name=target_table, con=engine, if_exists="append", index=False)

    click.echo("Ingestion finished.")

if __name__ == "__main__":
    main()

