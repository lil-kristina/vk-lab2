# Практическая работа №2: CDC → SCD → Lakehouse (Вариант 1)

**Автор:** Седова Кристина Александровна

## Обзор работы
Задача: операционная база данных (PostgreSQL) постоянно меняется (переезды, смена владельцев, изменение статусов). Аналитикам нужна история на дату («кто был владельцем счёта на 1 марта»), но нельзя нагружать источник тяжёлыми `SELECT`-запросами. 

**Решение:** реализована архитектура **CDC → SCD Type 2 → Lakehouse**.
*   **CDC:** Debezium читает журнал транзакций PostgreSQL и отправляет события в Kafka.
*   **Обработка:** Spark Structured Streaming применяет логику SCD Type 2 (закрывает старые версии, вставляет новые).
*   **Хранение:** данные пишутся в таблицы Iceberg (формат), физически лежат в MinIO (S3), метаданные хранятся в Lakekeeper (REST Catalog).
*   **Доступ:** SQL-запросы выполняются через Trino (и DBeaver).

## Стек технологий
| Компонент | Технология | Порт (хост) | Роль |
|---|---|---|---|
| Источник | PostgreSQL 16 | 5433 | Операционная БД (OLTP) |
| CDC | Debezium 2.6 | 8083 | Захват изменений из WAL |
| Буфер | Kafka 3.7 (KRaft) | 9092 | Очередь событий |
| Обработка | Spark 3.5 (PySpark) | 7077, 8081 (UI) | SCD Type 2 |
| Хранилище | MinIO | 9000, 9001 | Объектное хранилище (S3) |
| Каталог | Lakekeeper | 8181 | Iceberg REST Catalog |
| SQL-движок | Trino | 8080 | Аналитические запросы |
| BI-клиент | DBeaver | — | Визуализация данных |

## Структура репозитория
```text
p2-cdc-scd-lakehouse/
├── README.md                  — этот файл: обзор работы и стек
├── docker-compose.yml         — описание всех сервисов (стек)
├── отчет.md                   — заполненная форма отчёта по практике
├── architecture.drawio        — исходник схемы архитектуры (draw.io)
│
├── init/                      — скрипты инициализации БД (Postgres)
│   └── 01_schema.sql          — создание таблицы accounts и триггера updated_at
│
├── debezium/                  — настройки CDC
│   └── connector.json         — конфиг коннектора Debezium (Postgres → Kafka)
│
├── lakekeeper/                — настройки каталога Iceberg
│   └── wh-lakehouse.json      — конфиг создания warehouse в Lakekeeper
│
├── spark/                     — обработка потока
│   └── scd_type2.py           — Spark-джоба: чтение Kafka, SCD Type 2, запись в Iceberg
│
├── trino/                     — конфигурация Trino
│   └── catalog/
│       └── lakehouse.properties — настройка каталога Iceberg (Lakekeeper + MinIO)
│
└── screenshots/               — скриншоты для отчёта
    ├── DockerDesktop.png      — запущенные контейнеры
    ├── MinIO.png              — бакет warehouse в MinIO
    ├── DBeaver.png            — таблица accounts_history в DBeaver
    └── architecture.png       — экспорт схемы для отчёта
```

### Быстрый старт

1. Поднять базовый стек: `docker compose up -d`.
2. Инициализировать Lakekeeper и MinIO. 
3. Зарегистрировать Debezium-коннектор.
4. Запустить обработку (Spark).
5. Проверить результат: подключитесь к Trino (через DBeaver или консоль).
