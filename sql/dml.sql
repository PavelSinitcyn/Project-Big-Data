-- Справочник типов IoT устройств
-- DML: наполнение таблицы

INSERT INTO device_types (id, type_name) VALUES
    (1, 'Холодильник'),
    (2, 'Кондиционер'),
    (3, 'Котёл'),
    (4, 'Датчик улицы')
ON CONFLICT (id) DO UPDATE SET type_name = EXCLUDED.type_name;
