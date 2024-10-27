from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, sum, window, to_timestamp, concat, lit

# Create Spark Session
spark = SparkSession.builder \
    .appName("VWAP Calculation") \
    .master("local") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Define schema for streaming stock data
schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", DoubleType(), True)
])

# Create streaming DataFrame from CSV source
stock_stream_df = spark \
    .readStream \
    .schema(schema) \
    .option("header", True) \
    .csv("/presentation_demo/data/stream/output_sink/")  # Folder path for incoming CSV data

# Combine Date and Time into a single Timestamp column
stock_stream_df = stock_stream_df \
    .withColumn("Timestamp", to_timestamp(concat(col("Date"), lit(" "), col("Time")), "yyyy-MM-dd HH:mm:ss.SSSSSS"))

# Calculate VWAP (Volume Weighted Average Price) every 5 seconds
vwap_df = stock_stream_df \
    .withWatermark("Timestamp", "5 seconds") \
    .groupBy("Name", window("Timestamp", "5 seconds")) \
    .agg(
        (sum(col("Close") * col("Volume")) / sum(col("Volume"))).alias("VWAP"),
        (sum(col("Volume"))).alias("Volume"),
    ) \
    .select("Name", "window.start", "window.end", "VWAP", 'Volume')


def count_rows_and_print(batch_df, batch_id):
    row_count = batch_df.count()  # Count the number of rows in the batch
    print(f"Batch ID: {batch_id}, New Rows: {row_count}")

    # Print the actual values to the console, sorted by Timestamp
    if row_count > 0:
        batch_df.show(truncate=False)
    else:
        print("No new rows to display.")

# Write output to console and optionally to a CSV file
query = vwap_df \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(lambda batch_df, batch_id: (
        count_rows_and_print(batch_df, batch_id)
        # Optionally, write to CSV
        # batch_df.write.mode("append").option("header", True).csv("/path/to/output/vwap_data")
    )) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
