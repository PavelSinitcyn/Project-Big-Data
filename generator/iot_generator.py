"""
Генератор сообщений от IoT устройств.
Раз в секунду формирует событие и публикует его в топик Kafka.

Формат события (JSON):
    {
        "device_type_id": 2,                       # тип устройства (id из справочника)
        "event_time": "2026-06-19T12:00:01.000",   # время события
        "temperature": 23.5,                       # температура
        "humidity": 45.2                           # влажность
    }
"""

import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "iot-events"

# Идентификаторы типов устройств совпадают со справочником в Postgres (dml.sql)
DEVICE_TYPE_IDS = [1, 2, 3, 4]


def make_event():
    """Сформировать одно случайное событие от устройства."""
    return {
        "device_type_id": random.choice(DEVICE_TYPE_IDS),
        # время в локальной зоне без таймзоны (ISO-8601, мс) — под TIMESTAMP(3) во Flink
        "event_time": datetime.now().isoformat(timespec="milliseconds"),
        "temperature": round(random.uniform(-10.0, 35.0), 2),
        "humidity": round(random.uniform(20.0, 90.0), 2),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"Генератор запущен. Публикуем в топик '{TOPIC}' (Ctrl+C для остановки)...")
    try:
        while True:
            event = make_event()
            producer.send(TOPIC, event)
            print("ОТПРАВЛЕНО:", event)
            time.sleep(1)  # раз в секунду
    except KeyboardInterrupt:
        print("\nОстановка генератора...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
