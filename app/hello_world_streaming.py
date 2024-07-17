from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("KafkaStreamingExample") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("action", StringType(), True)
])

static_df = spark.createDataFrame(
    [(0, "User A"), (1, "User B"), (2, "User C"), (3, "User D"), (4, "User E")],
    ["id", "name"]
)

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "netology-spark") \
    .option("startingOffsets", "earliest") \
    .option("group.id", "spark-kafka-group") \
    .load()

value_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("event_timestamp")
)

parsed_df = value_df.select("data.*", "event_timestamp")

joined_df = parsed_df.join(static_df, "id")

query_join = joined_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

windowed_counts = parsed_df \
    .groupBy(
        window(col("event_timestamp"), "1 minute"),
        "id"
    ) \
    .agg(count("action").alias("action_count"))

query_agg = windowed_counts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

spark.streams.awaitAnyTermination()