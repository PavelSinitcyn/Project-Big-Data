# Big Data 2026, Синицын Павел
# Заключительный проект: IoT + Kafka + Flink + Postgres

Потоковая обработка событий от IoT-устройств на PyFlink в режиме **event time**.

## Архитектура

```
Генератор (раз в сек) ──> Kafka (iot-events)
                                  │
                                  ▼
        ┌─────────────────── Flink (event time) ───────────────────┐
        │  src: kafka ─┐                                            │
        │              ├─ join ─> window (1 мин) ─> sink: kafka ────┼──> Kafka (iot-minute-stats)
        │  src: pg ────┘   (avg temp, median humidity)              │
        └───────────────────────────────────────────────────────────┘
                          ▲
                   Postgres (справочник device_types: ddl.sql / dml.sql)
```

## Что реализовано

- **Генератор сообщений** раз в секунду от IoT-устройств (тип, время события, температура, влажность) → публикация в топик Kafka - [generator/iot_generator.py](generator/iot_generator.py).
- **DDL/DML скрипты** для справочника типов устройств (`id`, `type_name`) — [sql/ddl.sql](sql/ddl.sql), [sql/dml.sql](sql/dml.sql).
- **Flink, event time** — [flink_job/job.py](flink_job/job.py):
  - источник **kafka** и источник **pg** на **SQL/Table API**;
  - **join** событий Kafka со статичным справочником из Postgres (lookup join, сохраняет event time);
  - **window** в 1 минуту: средняя температура и медиана влажности по каждому типу устройства;
  - **sink в kafka** на **SQL/Table API** в формате: время (`hh:mm`), тип устройства (из pg), средняя температура, медиана влажности.
- **Источник и получатель на SQL/Table API** — таблицы `iot_events`, `device_types`, `iot_minute_stats` объявлены через DDL Table API.
- **Переход между DataStream и SQL/Table API** в обе стороны: `to_data_stream` (для оконной агрегации с медианой) и `from_data_stream` (для записи в sink).

## Структура

```
project_big_data/
├── docker-compose.yml          # Kafka + Postgres
├── download_jars.sh            # скачивание коннекторов Flink
├── requirements.txt
├── sql/
│   ├── ddl.sql                 # создание справочника типов
│   └── dml.sql                 # наполнение справочника
├── generator/
│   ├── iot_generator.py        # генератор событий -> Kafka
│   └── view_results.py         # просмотр итогового топика
└── flink_job/
    └── job.py                  # Flink-конвейер
```

## Запуск

Нужны: Docker, Python 3.8–3.10, Java 11.

1. Поднять инфраструктуру (Postgres сам выполнит `ddl.sql` и `dml.sql`):
   ```bash
   docker-compose up -d
   ```

2. Установить зависимости и скачать коннекторы:
   ```bash
   python3 -m venv venv && source venv/bin/activate
   pip install -r requirements.txt
   ./download_jars.sh
   ```

3. Запустить генератор (в отдельном терминале):
   ```bash
   python generator/iot_generator.py
   ```

4. Запустить Flink-задачу:
   ```bash
   python flink_job/job.py
   ```

5. Посмотреть результат (в отдельном терминале):
   ```bash
   python generator/view_results.py
   ```

Через ~1 минуту в топике `iot-minute-stats` появятся записи вида:
```json
{"time": "12:05", "device_type": "Кондиционер", "avg_temperature": 21.34, "median_humidity": 55.1}
```

## Остановка

```bash
docker-compose down -v
```

> Время в результате (`hh:mm`) — в локальной зоне; генератор и оконная функция используют одну и ту же зону.
