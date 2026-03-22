from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source_ddl = """
    CREATE TABLE green_trips (
        lpep_pickup_datetime STRING,
        lpep_dropoff_datetime STRING,
        PULocationID INT,
        DOLocationID INT,
        passenger_count INT,
        trip_distance DOUBLE,
        tip_amount DOUBLE,
        total_amount DOUBLE,
        event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
        WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'green-trips',
        'properties.bootstrap.servers' = 'redpanda:29092',
        'properties.group.id' = 'hw-group-q4',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.ignore-parse-errors' = 'true'
    )
    """

    sink_ddl = """
    CREATE TABLE hw_window_pu (
        window_start TIMESTAMP(3),
        PULocationID INT,
        num_trips BIGINT
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/postgres',
        'table-name' = 'hw_window_pu',
        'username' = 'postgres',
        'password' = 'postgres',
        'driver' = 'org.postgresql.Driver'
    )
    """

    insert_sql = """
    INSERT INTO hw_window_pu
    SELECT
        window_start,
        PULocationID,
        COUNT(*) AS num_trips
    FROM TABLE(
        TUMBLE(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTES)
    )
    GROUP BY window_start, PULocationID
    """

    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)

    result = t_env.execute_sql(insert_sql)
    job_client = result.get_job_client()

    if job_client:
        print(f"Submitted job: {job_client.get_job_id()}")


if __name__ == "__main__":
    main()