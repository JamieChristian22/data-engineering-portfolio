from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum as _sum, count as _count, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Delta configuration
builder = (
    SparkSession.builder.appName("ClickstreamStreaming")
    .master("spark://spark:7077")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("ts_utc", StringType(), False),
    StructField("user_id", IntegerType(), False),
    StructField("session_id", StringType(), False),
    StructField("page", StringType(), False),
    StructField("country", StringType(), False),
    StructField("device", StringType(), False),
    StructField("referrer", StringType(), False),
    StructField("purchase_usd", DoubleType(), False),
])

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "clickstream")
    .option("startingOffsets", "latest")
    .load()
)

json_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("e")).select("e.*")

events = json_df.withColumn("event_ts", to_timestamp(col("ts_utc")))

agg = (
    events
    .withWatermark("event_ts", "2 minutes")
    .groupBy(window(col("event_ts"), "1 minute"), col("page"), col("country"), col("device"))
    .agg(
        _count("*").alias("events"),
        _sum("purchase_usd").alias("revenue_usd")
    )
)

output_path = "/opt/lakehouse/delta/clickstream_agg"
checkpoint = "/opt/lakehouse/checkpoints/clickstream_agg"

query = (
    agg.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .start(output_path)
)

print("Streaming job started. Writing Delta table to:", output_path)
query.awaitTermination()
