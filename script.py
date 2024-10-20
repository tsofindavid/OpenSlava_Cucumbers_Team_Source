import requests
from bs4 import BeautifulSoup
import json
import csv
from datetime import datetime
import os
from time import sleep
from tqdm import tqdm
import pandas as pd

# List of stations and their "more" links from the previously parsed data
stations_df = pd.read_csv("Station_List_with_More_Links.csv")
stations = stations_df.to_dict(orient='records')

base_url = "https://www.danubehis.org"

def fetch_and_parse_station_data(station):
    station_name = station['Station']
    more_link = station['More Link']

    # Get the full URL for the station's page
    url = base_url + more_link

    # Fetch the page content
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to retrieve data for {station_name}.")
        return None

    # Parse the HTML content
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the relevant div that contains the chart data
    chart_data_div = soup.find('div', class_='charts-highchart chart')
    if not chart_data_div:
        print(f"No chart data found for {station_name}.")
        return None

    # Extract the 'data-chart' attribute
    data_chart = chart_data_div.get('data-chart')
    if not data_chart:
        print(f"No data-chart attribute found for {station_name}.")
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
    csv_filename = f"{station_name.replace(' ', '_').replace('(', '').replace(')', '')}_data.csv"
    csv_filepath = os.path.join('./data', csv_filename)
    with open(csv_filepath, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(csv_data)

    sleep(5)
    return csv_filepath

# Loop through each station and fetch the data
for station in tqdm(stations):
    fetch_and_parse_station_data(station)
