from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, coalesce, expr, row_number
from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyspark.sql.window import Window

TABLE = "lakehouse.default.accounts_history"
TOPIC = "dbserver.public.accounts"
CHECKPOINT = "/checkpoints/scd_type2_v5"

# Spark + Iceberg
spark = (
    SparkSession.builder.appName("SCD_Type2_CDC")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "rest")
    .config("spark.sql.catalog.lakehouse.uri", "http://lakekeeper:8181/catalog")
    .config("spark.sql.catalog.lakehouse.warehouse", "lakehouse")
    .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.lakehouse.s3.endpoint", "http://minio:9000")
    .config("spark.sql.catalog.lakehouse.s3.access-key-id", "minioadmin")
    .config("spark.sql.catalog.lakehouse.s3.secret-access-key", "minioadmin")
    .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
    .config("spark.sql.catalog.lakehouse.client.region", "us-east-1")
    .config("spark.sql.defaultCatalog", "lakehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Таблица истории
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.default")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        account_id BIGINT,
        owner      STRING,
        city       STRING,
        status     STRING,
        updated_at TIMESTAMP,   -- время изменения в источнике
        valid_from TIMESTAMP,   -- с какого момента версия действует
        valid_to   TIMESTAMP,   -- до какого (NULL = действует сейчас)
        is_current BOOLEAN
    ) USING iceberg
""")

# 1. Kafka
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")  # действует только при первом запуске, дальше - checkpoint
    .load()
)

# 2. Разбор Debezium-конверта
row_schema = StructType([
    StructField("account_id", LongType()),
    StructField("owner", StringType()),
    StructField("city", StringType()),
    StructField("status", StringType()),
    StructField("updated_at", LongType()),   # TIMESTAMP в Debezium = микросекунды от эпохи
])
envelope = StructType([
    StructField("before", row_schema),
    StructField("after", row_schema),
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),         # время события в Debezium, миллисекунды
])

events = (
    raw.select(
        from_json(col("value").cast("string"), envelope).alias("d"),
        col("offset").alias("kafka_offset"),
    )
    # отбрасываем tombstone-сообщения (value = null) и неизвестные операции
    .where(col("d.op").isin("c", "r", "u", "d"))
    .select(
        coalesce(col("d.after.account_id"), col("d.before.account_id")).alias("account_id"),
        col("d.after.owner").alias("owner"),
        col("d.after.city").alias("city"),
        col("d.after.status").alias("status"),
        col("d.op").alias("op"),
        # время события: для delete - ts_ms, иначе updated_at из источника
        when(col("d.op") == "d", expr("timestamp_millis(d.ts_ms)"))
        .otherwise(expr("timestamp_micros(d.after.updated_at)"))
        .alias("event_time"),
        "kafka_offset",
    )
)

# 3. Микробатч
MERGE_SQL = f"""
    MERGE INTO {TABLE} t
    USING batch_updates u
    ON t.account_id = u.account_id AND t.is_current = true
    -- удаление в источнике: закрываем текущую версию
    WHEN MATCHED AND u.op = 'd' AND u.event_time >= t.valid_from THEN
        UPDATE SET t.valid_to = u.event_time, t.is_current = false
    -- изменение отслеживаемых полей: закрываем текущую версию
    WHEN MATCHED AND u.op <> 'd' AND u.event_time > t.valid_from
         AND NOT (t.owner <=> u.owner AND t.city <=> u.city AND t.status <=> u.status) THEN
        UPDATE SET t.valid_to = u.event_time, t.is_current = false
"""

INSERT_SQL = f"""
    INSERT INTO {TABLE}
    SELECT u.account_id, u.owner, u.city, u.status,
           u.event_time               AS updated_at,
           u.event_time               AS valid_from,
           CAST(NULL AS TIMESTAMP)    AS valid_to,
           true                       AS is_current
    FROM batch_updates u
    WHERE u.op <> 'd'
      -- вставляем, только если нет действующей версии и событие не старее уже записанных
      AND NOT EXISTS (
          SELECT 1 FROM {TABLE} c
          WHERE c.account_id = u.account_id
            AND (c.is_current = true OR c.valid_from >= u.event_time)
      )
"""


def process_batch(batch_df, epoch_id):
    # одно (последнее) событие на счёт в рамках батча
    w = Window.partitionBy("account_id").orderBy(col("event_time").desc(), col("kafka_offset").desc())
    latest = (
        batch_df.withColumn("rn", row_number().over(w))
        .where("rn = 1")
        .drop("rn")
        .persist()
    )
    try:
        if latest.isEmpty():
            return
        latest.createOrReplaceTempView("batch_updates")
        s = latest.sparkSession
        s.sql(MERGE_SQL)    # шаг 1: закрыть устаревшие версии
        s.sql(INSERT_SQL)   # шаг 2: вставить новые версии
    finally:
        latest.unpersist()


# 4. Запуск потока
query = (
    events.writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .start()
)
query.awaitTermination()