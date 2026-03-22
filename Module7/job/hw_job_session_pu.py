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
        'properties.group.id' = 'hw-group-session',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.ignore-parse-errors' = 'true'
    )
    """

    sink_ddl = """
    CREATE TABLE hw_session_pu (
        session_start TIMESTAMP(3),
        session_end TIMESTAMP(3),
        PULocationID INT,
        num_trips BIGINT,
        PRIMARY KEY (session_start, session_end, PULocationID) NOT ENFORCED
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://postgres:5432/postgres',
        'table-name' = 'hw_session_pu',
        'username' = 'postgres',
        'password' = 'postgres',
        'driver' = 'org.postgresql.Driver'
    )
    """

    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)

    insert_sql = """
    INSERT INTO hw_session_pu
    SELECT
        window_start,
        window_end,
        PULocationID,
        COUNT(*) AS num_trips
    FROM TABLE(
        SESSION(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTES)
    )
    GROUP BY window_start, window_end, PULocationID
    """

    t_env.execute_sql(insert_sql).wait()


if __name__ == "__main__":
    main()