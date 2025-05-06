from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("TrafficMonitoring") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Schema for incoming JSON data
schema = StructType([
    StructField("sensor_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("vehicle_count", IntegerType(), False),
    StructField("average_speed", DoubleType(), False),
    StructField("congestion_level", StringType(), False)
])

# Read from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic_data") \
    .load()

# Parse JSON and apply schema
parsed_stream = raw_stream \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

# Data Quality Checks
clean_stream = parsed_stream \
    .filter(
        col("sensor_id").isNotNull() &
        col("timestamp").isNotNull() &
        (col("vehicle_count") >= 0) &
        (col("average_speed") > 0)
    ) \
    .dropDuplicates(["sensor_id", "timestamp"])

# Analysis 1: Traffic Volume per Sensor (5-minute windows)
traffic_volume = clean_stream \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(window("timestamp", "5 minutes"), "sensor_id") \
    .agg(sum("vehicle_count").alias("total_vehicles")) \
    .select(
        col("window.start").alias("window_start"),
        col("sensor_id"),
        col("total_vehicles")
    )

# Analysis 2: Congestion Hotspots (3 consecutive HIGH in 5-minute windows)
congestion_hotspots = clean_stream \
    .filter(col("congestion_level") == "HIGH") \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(window("timestamp", "15 minutes", "5 minutes"), "sensor_id") \
    .agg(count("*").alias("high_count")) \
    .filter(col("high_count") >= 3) \
    .select(col("sensor_id"), col("window.start").alias("window_start"))

# Analysis 3: Average Speed per Sensor (10-minute rolling window)
avg_speed = clean_stream \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(window("timestamp", "10 minutes"), "sensor_id") \
    .agg(avg("average_speed").alias("avg_speed")) \
    .select(col("window.start").alias("window_start"), col("sensor_id"), col("avg_speed"))

# Analysis 4: Sudden Speed Drops (>50% drop in 2 minutes)
window_spec = Window.partitionBy("sensor_id").orderBy("timestamp").rowsBetween(-1, 0)
speed_drops = clean_stream \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy("sensor_id", window("timestamp", "2 minutes")) \
    .agg(avg("average_speed").alias("current_speed")) \
    .withColumn("prev_speed", lag("current_speed", 1).over(window_spec)) \
    .filter(col("current_speed") < 0.5 * col("prev_speed")) \
    .select(col("sensor_id"), col("window.start").alias("window_start"))

# Analysis 5: Busiest Sensors (Last 30 minutes)
busiest_sensors = clean_stream \
    .withWatermark("timestamp", "30 minutes") \
    .groupBy(window("timestamp", "30 minutes"), "sensor_id") \
    .agg(sum("vehicle_count").alias("total_vehicles")) \
    .select(col("window.start").alias("window_start"), col("sensor_id"), col("total_vehicles")) \
    .orderBy(col("total_vehicles").desc()) \
    .limit(3)

# Write results to Kafka
def write_to_kafka(df, topic):
    return df.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", topic) \
        .option("checkpointLocation", f"/tmp/checkpoint_{topic}") \
        .outputMode("append") \
        .start()

# Start all streams
write_to_kafka(traffic_volume, "traffic_analysis").awaitTermination()