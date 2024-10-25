import os
import pandas as pd
from meteostat import Point, Hourly
from datetime import datetime
import time

# Load stations from the teammate's CSV
csv_file_path = "./seeds/current_water_level_res.csv"  # Update with actual path
stations_df = pd.read_csv(csv_file_path)

# Create directory to store weather data
if not os.path.exists("weather_data"):
    os.makedirs("weather_data")

# Set the time period (past 40 years)
start_date = datetime(1984, 1, 1)
end_date = datetime(2024, 10, 24, 23, 59)  # Set to the end of the last day

# Loop through each station and fetch the hourly weather data
for index, row in stations_df.iterrows():
    station_name = row["station_name"]
    more_link = row["link"]
    latitude = row["latitude"]
    longitude = row["longitude"]

    if pd.notna(latitude) and pd.notna(longitude):
        # Fetch hourly weather data
        print(
            f"Fetching hourly weather data for {station_name} ({latitude}, {longitude})..."
        )
        location = Point(latitude, longitude)
        data = Hourly(location, start=start_date, end=end_date)
        data = data.fetch()

        # Save data to CSV
        file_name = (
            f"weather_data/{station_name.replace(' ', '_')}_hourly_weather_data.csv"
        )
        data.to_csv(file_name)
        print(f"Hourly data saved for {station_name} in {file_name}")
    else:
        print(f"Skipping {station_name} due to missing coordinates.")

    # Sleep to avoid hitting API rate limits
    time.sleep(1)
