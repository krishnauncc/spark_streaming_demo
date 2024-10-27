import csv
import random
from datetime import datetime, timedelta

# Set trading day start and end times
start_time = datetime.strptime("09:30:00", "%H:%M:%S")
end_time = datetime.strptime("16:00:00", "%H:%M:%S")

# Get current date
current_date = datetime.now().date()

# Define the output CSV file
output_file = "data/stream/stock_data.csv"

# Initialize CSV file with headers
with open(output_file, mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["Date", "Time", "Name", "Open", "High", "Low", "Close", "Volume"])

    # Generate data every 5 seconds between 9:30 AM and 4:00 PM
    current_time = datetime.combine(current_date, start_time.time())
    end_of_day = datetime.combine(current_date, end_time.time())
    
    while current_time <= end_of_day:
        # Generate random Open, High, Low, Close values
        open_price = round(random.uniform(500, 600), 2)
        high_price = round(open_price + random.uniform(0.5, 5.0), 2)
        low_price = round(open_price - random.uniform(0.5, 5.0), 2)
        close_price = round(random.uniform(low_price, high_price), 2)
        volume = random.randint(1000, 10000)
        
        # Write row with generated data
        writer.writerow([
            current_time.strftime("%Y-%m-%d"),  # Current date
            current_time.strftime("%H:%M:%S"),  # Current time
            "AAPL",  # Example stock ticker
            open_price,
            high_price,
            low_price,
            close_price,
            volume
        ])
        
        # Increment time by a random duration (up to 5 seconds)
        current_time += timedelta(seconds=random.uniform(0.01, 4.0))

print(f"Stock data has been generated and saved to {output_file}")
