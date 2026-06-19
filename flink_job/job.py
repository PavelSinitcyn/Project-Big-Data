"""
Flink-задача (PyFlink), работа в event time.

Конвейер:
    src: kafka  -- события IoT (Table API / SQL)
    src: pg     -- справочник типов устройств (Table API / SQL, JDBC)
    join        -- обогащаем события из Kafka наименованием типа из Postgres
    window      -- окно в 1 минуту: средняя температура и медиана влажности (DataStream)
    sink: kafka -- результат пишем в топик Kafka (Table API / SQL)

В задаче показан переход между Table/SQL API и DataStream API в обе стороны:
    Table -> DataStream (для оконной агрегации с медианой)
    DataStream -> Table (для записи в sink)
"""

import os
import glob
from datetime import datetime

from pyflink.common import Duration, Row, Time, Types, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.table import StreamTableEnvironment

KAFKA_BOOTSTRAP = "localhost:9092"
SOURCE_TOPIC = "iot-events"
SINK_TOPIC = "iot-minute-stats"

PG_URL = "jdbc:postgresql://localhost:5432/iot"
PG_USER = "postgres"
PG_PASSWORD = "postgres"


def find_jars():
    """Собрать file://-ссылки на все коннекторные jar из папки jars."""
    jars_dir = os.path.join(os.path.dirname(__file__), os.pardir, "jars")
    jars_dir = os.path.abspath(jars_dir)
    jars = glob.glob(os.path.join(jars_dir, "*.jar"))
    if not jars:
        raise RuntimeError(
            f"Не найдены jar-коннекторы в {jars_dir}. Запустите ./download_jars.sh"
        )
    return [f"file://{j}" for j in jars]


def median(values):
    """Медиана списка чисел."""
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return float(s[mid])
    return (s[mid - 1] + s[mid]) / 2.0


class EventTimeAssigner(TimestampAssigner):
    """Достаём время события из поля event_time (локальное время) в мс."""

    def extract_timestamp(self, value, record_timestamp):
        dt = value[0]  # datetime из TIMESTAMP(3)
        return int(dt.timestamp() * 1000)


class MinuteStats(ProcessWindowFunction):
    """
    Оконная функция: для каждого типа устройства в минутном окне
    считает среднюю температуру и медиану влажности.
    """

    def process(self, key, context, elements):
        temps = []
        hums = []
        for e in elements:
            temps.append(e[1])  # temperature
            hums.append(e[2])   # humidity

        avg_temp = sum(temps) / len(temps)
        med_hum = median(hums)

        # начало окна -> время в формате hh:mm (локальная зона)
        window_start_ms = context.window().start
        time_str = datetime.fromtimestamp(window_start_ms / 1000).strftime("%H:%M")

        # key -> наименование типа устройства (из Postgres)
        yield Row(time_str, key, round(avg_temp, 2), round(med_hum, 2))


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars(*find_jars())
    env.set_parallelism(1)

    t_env = StreamTableEnvironment.create(env)

    # --- src: kafka (Table API / SQL), работа в event time ---
    t_env.execute_sql(f"""
        CREATE TABLE iot_events (
            device_type_id INT,
            event_time     TIMESTAMP(3),
            temperature    DOUBLE,
            humidity       DOUBLE,
            proc_time      AS PROCTIME(),                              -- для lookup join
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{SOURCE_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id' = 'flink-iot',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    # --- src: pg (Table API / SQL, JDBC) -- статичный справочник типов ---
    t_env.execute_sql(f"""
        CREATE TABLE device_types (
            id        INT,
            type_name STRING,
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{PG_URL}',
            'table-name' = 'device_types',
            'username' = '{PG_USER}',
            'password' = '{PG_PASSWORD}'
        )
    """)

    # --- join: события Kafka + справочник Postgres (lookup join сохраняет event time) ---
    joined = t_env.sql_query("""
        SELECT
            e.event_time   AS event_time,
            d.type_name    AS type_name,
            e.temperature  AS temperature,
            e.humidity     AS humidity
        FROM iot_events AS e
        JOIN device_types FOR SYSTEM_TIME AS OF e.proc_time AS d
          ON e.device_type_id = d.id
    """)

    # --- переход Table -> DataStream ---
    ds = t_env.to_data_stream(joined)

    # назначаем event time и watermark в DataStream
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(5))
        .with_timestamp_assigner(EventTimeAssigner())
    )
    ds = ds.assign_timestamps_and_watermarks(watermark_strategy)

    # отбрасываем event_time (TIMESTAMP не поддерживается в состоянии окна),
    # время события уже зафиксировано как метка записи -> оставляем тип, температуру, влажность
    ds = ds.map(
        lambda r: Row(r[1], r[2], r[3]),
        output_type=Types.ROW_NAMED(
            ["type_name", "temperature", "humidity"],
            [Types.STRING(), Types.DOUBLE(), Types.DOUBLE()],
        ),
    )

    # --- window: минутное окно по типу устройства, средняя температура и медиана влажности ---
    result_type = Types.ROW_NAMED(
        ["event_time_str", "device_type", "avg_temperature", "median_humidity"],
        [Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.DOUBLE()],
    )

    result_ds = (
        ds.key_by(lambda r: r[0])  # type_name
        .window(TumblingEventTimeWindows.of(Time.minutes(1)))
        .process(MinuteStats(), output_type=result_type)
    )

    # --- переход DataStream -> Table ---
    result_table = t_env.from_data_stream(result_ds)
    t_env.create_temporary_view("result_view", result_table)

    # --- sink: kafka (Table API / SQL) ---
    t_env.execute_sql(f"""
        CREATE TABLE iot_minute_stats (
            `time`          STRING,
            device_type     STRING,
            avg_temperature DOUBLE,
            median_humidity DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{SINK_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'format' = 'json'
        )
    """)

    t_env.execute_sql("""
        INSERT INTO iot_minute_stats
        SELECT event_time_str, device_type, avg_temperature, median_humidity
        FROM result_view
    """).wait()


if __name__ == "__main__":
    main()
