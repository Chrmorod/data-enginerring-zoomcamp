"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: VendorID
    type: integer
  - name: tpep_pickup_datetime
    type: timestamp
  - name: tpep_dropoff_datetime
    type: timestamp
  - name: passenger_count
    type: integer
  - name: trip_distance
    type: double
  - name: RatecodeID
    type: integer
  - name: store_and_fwd_flag
    type: string
  - name: PULocationID
    type: integer
  - name: DOLocationID
    type: integer
  - name: payment_type
    type: integer
  - name: fare_amount
    type: double
  - name: extra
    type: double
  - name: mta_tax
    type: double
  - name: tip_amount
    type: double
  - name: tolls_amount
    type: double
  - name: improvement_surcharge
    type: double
  - name: total_amount
    type: double
  - name: congestion_surcharge
    type: double
  - name: airport_fee
    type: double
  - name: taxi_type
    type: string

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python

import os
import json
import pandas as pd
from datetime import datetime, date
from typing import List, Tuple
from dateutil.relativedelta import relativedelta

#NYC Taxi TLC data endpoint
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def _parse_bruin_datetime(s: str) -> datetime:
    if not s:
        raise ValueError("Empty datetime string")

    s = s.strip()

    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return datetime.strptime(s, "%Y-%m-%d")
      
    # ISO con 'Z' -> convert a +00:00 to fromisoformat
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    return datetime.fromisoformat(s)


def generate_monthly_to_ingest(start_date: str, end_date: str) -> List[Tuple[int, int]]:
    start_dt = _parse_bruin_datetime(start_date)
    end_dt_excl = _parse_bruin_datetime(end_date)

    # Normalizamos a month start (día 1 a las 00:00)
    start = start_dt.date().replace(day=1)

    # end es EXCLUSIVO: consideramos el mes del día anterior a end
    end_date_excl = end_dt_excl.date()
    if end_date_excl <= start:
        return []

    last_included_day = date.fromordinal(end_date_excl.toordinal() - 1)
    last_month = last_included_day.replace(day=1)

    months: List[Tuple[int, int]] = []
    cur = start
    while cur <= last_month:
        months.append((cur.year, cur.month))
        # sumar 1 mes
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)

    return months
def build_parquet_url(taxi_type: str, year: int, month: int) -> str:
    """
    Build the URL for the Parquet file based on taxi type, year, and month.

    Args:
        taxi_type (str): Type of taxi (e.g., 'yellow', 'green').
        year (int): Year of the data.
        month (int): Month of the data.  
    Returns:
        str: URL to the Parquet file.
    """
    return f"{BASE_URL}/{taxi_type}_tripdata_{year}-{month:02d}.parquet"  

def fetch_trip_data(taxi_type: str, year: int, month: int) -> pd.DataFrame:
    """
    Fetch trip data from the Parquet file for the given taxi type, year, and month.

    Args:
        taxi_type (str): Type of taxi (e.g., 'yellow', 'green').
        year (int): Year of the data.
        month (int): Month of the data.  
    Returns:
        pd.DataFrame: DataFrame containing the trip data.
    """
    url = build_parquet_url(taxi_type, year, month)
    try:
        df = pd.read_parquet(url)
        df['taxi_type'] = taxi_type  # Add a column to identify taxi type
        return df
    except Exception as e:
        print(f"Error fetching data from {url}: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error

# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize() -> pd.DataFrame:
    """
    Materialize the trip data by fetching it from the Parquet files and returning a DataFrame.

    Returns:
        pd.DataFrame: DataFrame containing the ingested trip data.
    """       
    # Get environment variables
    start_date = os.environ.get('BRUIN_START_DATE')
    end_date = os.environ.get('BRUIN_END_DATE')
    vars_json = os.environ.get('BRUIN_VARS', '{}')
    vars_dict = json.loads(vars_json)
    taxi_types = vars_dict.get('taxi_types', ['yellow'])
    
    monthly_periods = generate_monthly_to_ingest(start_date, end_date)
    
    all_data = []
    for taxi_type in taxi_types:
        for year, month in monthly_periods:
            df = fetch_trip_data(taxi_type, year, month)
            if not df.empty:
                all_data.append(df)
    
    if all_data:
        return pd.concat(all_data, ignore_index=True)

    raise RuntimeError(
        f"No data ingested. Check date range and file availability. "
        f"start_date={start_date}, end_date={end_date}, taxi_types={taxi_types}"
    )


