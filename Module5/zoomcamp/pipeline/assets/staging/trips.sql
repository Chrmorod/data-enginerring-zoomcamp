/* @bruin
name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table

columns:
  - name: trip_id
    type: string
    description: "Composite primary key for deduplication"
    primary_key: true
    nullable: false
    checks:
      - name: not_null

  - name: vendor_id
    type: integer
    description: "TLC vendor identifier"
    checks:
      - name: not_null

  - name: pickup_datetime
    type: timestamp
    description: "Trip start time"
    checks:
      - name: not_null

  - name: dropoff_datetime
    type: timestamp
    description: "Trip end time"
    checks:
      - name: not_null

  - name: passenger_count
    type: integer
    description: "Number of passengers"
    checks:
      - name: not_null
      - name: non_negative

  - name: trip_distance
    type: double
    description: "Trip distance in miles"
    checks:
      - name: not_null
      - name: non_negative

  - name: ratecode_id
    type: integer
    description: "Rate code identifier"
    checks:
      - name: not_null

  - name: store_and_fwd_flag
    type: string
    description: "Store and forward flag"

  - name: pu_location_id
    type: integer
    description: "Pickup location ID"
    checks:
      - name: not_null

  - name: do_location_id
    type: integer
    description: "Dropoff location ID"
    checks:
      - name: not_null

  - name: payment_type
    type: string
    description: "Payment type name from lookup"
    checks:
      - name: not_null

  - name: fare_amount
    type: double
    description: "Base fare amount"
    checks:
      - name: not_null
      - name: non_negative

  - name: extra
    type: double
    description: "Extra charges"
    checks:
      - name: non_negative

  - name: mta_tax
    type: double
    description: "MTA tax"
    checks:
      - name: non_negative

  - name: tip_amount
    type: double
    description: "Tip amount"
    checks:
      - name: non_negative

  - name: tolls_amount
    type: double
    description: "Tolls amount"
    checks:
      - name: non_negative

  - name: improvement_surcharge
    type: double
    description: "Improvement surcharge"
    checks:
      - name: non_negative

  - name: total_amount
    type: double
    description: "Total amount"
    checks:
      - name: not_null
      - name: non_negative

  - name: congestion_surcharge
    type: double
    description: "Congestion surcharge"
    checks:
      - name: non_negative

  - name: airport_fee
    type: double
    description: "Airport fee"
    checks:
      - name: non_negative

  - name: taxi_type
    type: string
    description: "Type of taxi (yellow)"
    checks:
      - name: not_null

custom_checks:
  - name: row_count_positive
    description: Ensure the table is not empty in the processed window
    query: |
      SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END AS ok
      FROM staging.trips
      WHERE pickup_datetime >= TIMESTAMP '{{ start_datetime }}'
        AND pickup_datetime <  TIMESTAMP '{{ end_datetime }}'
    value: 1
@bruin */

WITH src AS (
  SELECT
    CAST(vendor_id AS INTEGER) AS vendor_id,
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

    -- Rellenamos nulos para pasar not_null
    COALESCE(CAST(passenger_count AS INTEGER), 0) AS passenger_count,
    CAST(trip_distance AS DOUBLE) AS trip_distance,
    COALESCE(CAST(ratecode_id AS INTEGER), 1) AS ratecode_id,

    CAST(store_and_fwd_flag AS VARCHAR) AS store_and_fwd_flag,
    CAST(pu_location_id AS INTEGER) AS pu_location_id,
    CAST(do_location_id AS INTEGER) AS do_location_id,
    CAST(payment_type AS INTEGER) AS payment_type_id,

    CAST(fare_amount AS DOUBLE) AS fare_amount,
    CAST(extra AS DOUBLE) AS extra,
    CAST(mta_tax AS DOUBLE) AS mta_tax,
    CAST(tip_amount AS DOUBLE) AS tip_amount,
    CAST(tolls_amount AS DOUBLE) AS tolls_amount,
    CAST(improvement_surcharge AS DOUBLE) AS improvement_surcharge,
    CAST(total_amount AS DOUBLE) AS total_amount,
    CAST(congestion_surcharge AS DOUBLE) AS congestion_surcharge,
    CAST(airport_fee AS DOUBLE) AS airport_fee,
    CAST(taxi_type AS VARCHAR) AS taxi_type
  FROM ingestion.trips
  WHERE
    tpep_pickup_datetime >= TIMESTAMP '{{ start_datetime }}'
    AND tpep_pickup_datetime <  TIMESTAMP '{{ end_datetime }}'
    AND taxi_type = 'yellow'

    -- Filtramos valores negativos para pasar non_negative
    AND trip_distance >= 0
    AND fare_amount >= 0
    AND extra >= 0
    AND mta_tax >= 0
    AND tip_amount >= 0
    AND tolls_amount >= 0
    AND improvement_surcharge >= 0
    AND total_amount >= 0
    AND congestion_surcharge >= 0
    AND airport_fee >= 0
),
enriched AS (
  SELECT
    md5(
      concat_ws(
        '|',
        CAST(pickup_datetime AS VARCHAR),
        CAST(dropoff_datetime AS VARCHAR),
        CAST(pu_location_id AS VARCHAR),
        CAST(do_location_id AS VARCHAR),
        CAST(COALESCE(vendor_id, -1) AS VARCHAR),
        CAST(COALESCE(fare_amount, -1) AS VARCHAR),
        taxi_type
      )
    ) AS trip_id,

    s.vendor_id,
    s.pickup_datetime,
    s.dropoff_datetime,
    s.passenger_count,
    s.trip_distance,
    s.ratecode_id,
    s.store_and_fwd_flag,
    s.pu_location_id,
    s.do_location_id,

    COALESCE(pl.payment_type_name, CAST(s.payment_type_id AS VARCHAR)) AS payment_type,

    s.fare_amount,
    s.extra,
    s.mta_tax,
    s.tip_amount,
    s.tolls_amount,
    s.improvement_surcharge,
    s.total_amount,
    s.congestion_surcharge,
    s.airport_fee,
    s.taxi_type
  FROM src s
  LEFT JOIN ingestion.payment_lookup pl
    ON pl.payment_type_id = s.payment_type_id
),
dedup AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY pickup_datetime DESC) AS rn
  FROM enriched
  WHERE pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
)
SELECT
  trip_id,
  vendor_id,
  pickup_datetime,
  dropoff_datetime,
  passenger_count,
  trip_distance,
  ratecode_id,
  store_and_fwd_flag,
  pu_location_id,
  do_location_id,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  airport_fee,
  taxi_type
FROM dedup
WHERE rn = 1;