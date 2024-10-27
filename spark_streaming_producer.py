import random
import time
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import Row

stock_base_prices = {"AAPL": 500, "GOOGL": 166, "MSFT": 428, "AMZN": 187, "TSLA": 269}

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Generate Stock Data") \
    .master("local") \
    .getOrCreate()

# Function to generate random stock data
def generate_stock_data(num_records):
    stock_data = []
    stock_names = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]  # Example stock names

    for i in range(num_records):
        date_time = datetime.now()
        date_str = date_time.strftime("%Y-%m-%d")
        time_str = date_time.strftime("%H:%M:%S.%f")
        name = random.choice(stock_names)
        open_price = round(random.uniform(stock_base_prices[name], stock_base_prices[name] + 50), 2)
        high_price = round(open_price + random.uniform(0, 10), 2)
        low_price = round(open_price - random.uniform(0, 10), 2)
        close_price = round(random.uniform(low_price, high_price), 2)
        volume = random.randint(500, 10000)

        stock_data.append(Row(Date=date_str, Time=time_str, Name=name, Open=open_price,
                              High=high_price, Low=low_price, Close=close_price, Volume=volume))
        time.sleep(1)

    return stock_data

# Directory to write to HDFS
output_path = "/presentation_demo/data/stream/output_sink/"  # Change this to your HDFS output path

# Generate stock data for a duration (e.g., 10 minutes)


while True:
    start_time = datetime.now()
    num_records = random.randint(10, 100)  # Number of records to generate (20 records for 10 minutes with 30 sec intervals)

    # Generate the stock data
    stock_data = generate_stock_data(num_records)

    # Create a DataFrame from the generated stock data
    df = spark.createDataFrame(stock_data)

    df = df.orderBy("Name", "Date", "Time")
    #file_name = f"stock_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"  # Specify the file name

    # Write DataFrame to HDFS in CSV format
    df.write \
        .mode("append") \
        .option("header", True) \
        .csv(output_path)

    print(f"Written {num_records} records to HDFS between time interval {start_time} {datetime.now()}")

# Stop the Spark session when done (this line will never be reached in this loop)
spark.stop()
