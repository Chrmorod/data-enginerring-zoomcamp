# Module 4 dbt
---

## Commands

```bash
dbt --version                                                             
dbt init taxi_rides_ny
dbt debug
code ~/.dbt/profiles.yml
python3 ingest.py
duckdb -ui taxi_rides_ny.duckdb
dbt seed
dbt docs generate #(catalog json)
dbt docs serve --host 0.0.0.0 --port 8099

dbt snapshot
dbt source freshness
dbt docs serve #no use in cloud, (automatic)
dbt clean
dbt deps
dbt run 
dbt test
dbt compile


dbt  build (dbt run + dbt test + dbt seed + dbt snapshot)

dbt retry (whatever fail)

dbt --help
dbt --version
dbt run --full-refresh (drop data and load data)
dbt run --fail-fast
dbt run -target
dbt test -t prod
dbt run --select stg_green_tripdata
dbt run --select +int_trips_unioned
dbt run --select int_trips_unioned+
dbt run --select +int_trips_unioned+ (upstream & downstream)
dbt run --select state:modified --state ./target (only run files updated)
```
## analyses
- A place for SQL files that you don't want to expose
- I generally use it for data quality reports
- Lots of people don't use it 
## dbt_project.yml
- The most important file in dbt
- Tell dbt some defaults
- You need it to run dbt commands
- For dbt core, your profile should match the one in the `.dbt/profile.yaml`
## macros
- They behave like Python functions (Reusable logic)
- They help you encapsulate logic (in one place)
## README.md
- The documentation of your project
- Installation/setup guides
- Contact information

## seeds
- A space to upload csv and flat files (to add them to dbt later)
- Quick and dirty approach (better to fix at source)

## snapshots
- Take a picture of a table at a moment in time
- Useful to track the history of a column that overwrites itself

## tests
- A place to put assertions in SQL format
- A place for singular tests 
- If this SQL command returns more than 0 rows, the dbt build fails

## models
- The most important directory
- dbt suggests 3 subfolders:
### staging
- Sources (so raw table from database)
- Staging files are 1 to 1 copy of your data with minimal cleaning steps:
  - Data types
  - Renaming columns
### intermediate
- Anything that is not raw nor you want to expose
- No guidelines, just nice for heavy duty cleaning or complex logic
### marts
- If it is in marts, it is ready for consumption
- Tables ready for dashboards
- Properly modeled, clean tables

## example code utils (dbt_packages)
```bash
    with unioned as(
        select * from {{ ref('int_trips_unioned') }}
    ),
    payment_type_lookup as (
        select * from {{ ref('payment_type_lookup') }}
    ),
    cleaned_and_enriched_trips AS (
        SELECT
        -- Generate a unique trip identifier (surrogate key pattern)
            {{ dbt_utils.generate_surrogate_key([
                'vendor_id',
                'pickup_datetime',
                'pickup_location_id',
                'service_type'
            ]) }} as trip_id,
            -- Identifiers
            u.vendor_id,
            u.service_type,
            u.rate_code_id,
            -- Location IDs
            u.pickup_location_id,
            u.dropoff_location_id,
            --Timestamps
            u.pickup_datetime,
            u.dropoff_datetime,
            -- Trip details
            u.store_and_fwd_flag,
            u.passenger_count,
            u.trip_distance,
            u.fare_amount,
            -- Payment breakdown
            u.fare_amount,
            u.extra,
            u.mta_tax,
            u.tip_amount,
            u.tolls_amount,
            u.improvement_surcharge,
            u.total_amount,
            from payment_type_lookup p
        JOIN unioned u
            ON u.payment_type = p.payment_type  

    )
    select * from cleaned_and_enriched_trips
```
