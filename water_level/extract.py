import requests
from bs4 import BeautifulSoup
import json
import csv
from datetime import datetime
import os
from time import sleep
from tqdm import tqdm
import pandas as pd
import sys
from datetime import timedelta

# List of stations and their "more" links from the previously parsed data
stations_df = pd.read_csv(sys.argv[1])
stations = stations_df.to_dict(orient='records')

base_url = "https://www.danubehis.org"

def get_csv_filename(station_name, time_from_str, time_to_str):
    csv_filename = f"{station_name.replace('/', '_').replace(' ', '_').replace('(', '').replace(')', '')}_data_{time_from_str}_{time_to_str}.csv"
    csv_filepath = os.path.join('./data', csv_filename)
    return csv_filename, csv_filepath


def fetch_and_parse_station_data(station, time_from_str, time_to_str):
    station_name = station['Station']
    more_link = station['More Link']

    # Get the full URL for the station's page
    url = base_url + more_link + f"&time_from={time_from_str}&time_to={time_to_str}"

    # Fetch the page content
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to retrieve data for {station_name}.")
        sleep(10)
        return None

    # Parse the HTML content
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the relevant div that contains the chart data
    chart_data_div = soup.find('div', class_='charts-highchart chart')
    if not chart_data_div:
        print(f"No chart data found for {station_name}.")
        sleep(10)
        return None

    # Extract the 'data-chart' attribute
    data_chart = chart_data_div.get('data-chart')
    if not data_chart:
        print(f"No data-chart attribute found for {station_name}.")
        sleep(10)
        return None

    # Parse the chart data (it's in JSON format)
    chart_json = json.loads(data_chart)

    # Extract the series data (timestamps and values)
    series_data = chart_json['series'][0]['data']

    # Convert Unix timestamps to a readable date format and prepare CSV rows
    csv_data = [["Timestamp", "Value"]]
    for point in series_data:
        timestamp = datetime.utcfromtimestamp(point[0] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        value = point[1]
        csv_data.append([timestamp, value])

    # Save the data to a CSV file
    csv_filename, csv_filepath = get_csv_filename(
        station_name, 
        time_from_str, 
        time_to_str
    )

    with open(csv_filepath, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(csv_data)

    sleep(5)
    return csv_filepath

# Initialize start date and current date
history_start_date = datetime(2020, 1, 1)
current_date = datetime.now()
end_date = current_date

# Loop through each station and fetch the data
while end_date > history_start_date:

    start_date = end_date - timedelta(days=90)

    print(start_date.date(), end_date.date())

    for station in tqdm(stations):
        time_from_str = start_date.strftime('%d.%m.%Y')
        time_to_str = end_date.strftime('%d.%m.%Y')

        _, csv_filepath = get_csv_filename(
            station['Station'], 
            time_from_str, 
            time_to_str
        )

        if os.path.exists(csv_filepath):
            continue
        
        fetch_and_parse_station_data(station, time_from_str, time_to_str)

    end_date = start_date
