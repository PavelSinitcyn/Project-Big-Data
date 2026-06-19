#!/usr/bin/env bash
# Скачивание jar-коннекторов для Flink 1.18 (Kafka + JDBC + драйвер Postgres).
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/jars"
mkdir -p "$DIR"

BASE="https://repo1.maven.org/maven2"
JARS=(
  "org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar"
  "org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar"
  "org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar"
)

for path in "${JARS[@]}"; do
  name="$(basename "$path")"
  if [ -f "$DIR/$name" ]; then
    echo "Уже есть: $name"
  else
    echo "Скачиваем: $name"
    curl -fSL "$BASE/$path" -o "$DIR/$name"
  fi
done

echo "Готово. Jar-файлы в $DIR"
