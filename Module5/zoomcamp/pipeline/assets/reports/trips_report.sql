/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# TODO: Set the asset name (recommended: reports.trips_report).
name: reports.trips_report

# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependency on the staging asset(s) this report reads from.
depends:
  - staging.trips

# TODO: Choose materialization strategy.
# For reports, `time_interval` is a good choice to rebuild only the relevant time window.
# Important: Use the same `incremental_key` as staging (e.g., pickup_datetime) for consistency.
materialization:
  type: table
# TODO: Define report columns + primary key(s) at your chosen level of aggregation.
columns:
  - name: trip_date
    type: date

  - name: taxi_type
    type: string

  - name: payment_type
    type: string

  - name: trip_count
    type: integer

  - name: total_passengers
    type: double

  - name: total_distance
    type: double

  - name: total_fare
    type: double

  - name: total_tips
    type: double

  - name: total_revenue
    type: double

  - name: avg_fare
    type: double

  - name: avg_trip_distance
    type: double

  - name: avg_passengers
    type: double

custom_checks:
  - name: row_count_positive
    description: Ensure aggregation returns rows
    query: |
      SELECT COUNT(*) > 0 from reports.trips_report
    value: 1
@bruin */

-- Aggregate trips by date, taxi type, and payment type

SELECT
    CAST(pickup_datetime AS DATE) AS trip_date,
    taxi_type,
    payment_type,

    -- Count metrics
    COUNT(*) AS trip_count,
    SUM(COALESCE(passenger_count, 0)) AS total_passengers,

    -- Distance metrics
    SUM(COALESCE(trip_distance, 0)) AS total_distance,

    -- Revenue metrics
    SUM(COALESCE(fare_amount, 0)) AS total_fare,
    SUM(COALESCE(tip_amount, 0)) AS total_tips,
    SUM(COALESCE(total_amount, 0)) AS total_revenue,

    -- Average metrics
    AVG(COALESCE(fare_amount, 0)) AS avg_fare,
    AVG(COALESCE(trip_distance, 0)) AS avg_trip_distance,
    AVG(COALESCE(passenger_count, 0)) AS avg_passengers

FROM staging.trips
WHERE pickup_datetime >= TIMESTAMP '{{ start_datetime }}'
  AND pickup_datetime <  TIMESTAMP '{{ end_datetime }}'

GROUP BY
    CAST(pickup_datetime AS DATE),
    taxi_type,
    payment_type;
