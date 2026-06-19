-- Справочник типов IoT устройств
-- DDL: создание таблицы

CREATE TABLE IF NOT EXISTS device_types (
    id        INTEGER PRIMARY KEY,   -- идентификатор типа
    type_name VARCHAR(100) NOT NULL  -- наименование типа
);
