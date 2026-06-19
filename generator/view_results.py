"""
Вспомогательный скрипт: читает и печатает результат из топика iot-minute-stats.
Удобно для проверки работы конвейера.
"""

import json

from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "iot-minute-stats"


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    print(f"Читаем топик '{TOPIC}' (Ctrl+C для остановки)...")
    try:
        for msg in consumer:
            print("РЕЗУЛЬТАТ:", msg.value)
    except KeyboardInterrupt:
        print("\nОстановка...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
