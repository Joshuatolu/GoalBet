#!/usr/bin/env python
# coding: utf-8

# Import Libraries
import pandas as pd
from datetime import datetime
import os
from sqlalchemy import create_engine, VARCHAR
from dotenv import load_dotenv
import requests as rq
import logging

# Configure logging
logging.basicConfig(
    filename='pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Get the current season
current_date = datetime.now()
formated_date = int(current_date.strftime('%Y'))
season = str(formated_date)[2:] + str(formated_date + 1)[2:]

# Create the url formatting strings for the 3 leagues
data_url = {
    'Premiership': f'https://www.football-data.co.uk/mmz4281/{season}/E0.csv',
    'Championship': f'https://www.football-data.co.uk/mmz4281/{season}/E1.csv',
    'League_1': f'https://www.football-data.co.uk/mmz4281/{season}/E2.csv'
}
schemaa = 'GoalBet'

# Credentials
load_dotenv(override=True)
hostname = os.getenv('hostname')
username = os.getenv('username')
password = os.getenv('password')
port = os.getenv('port')
db_name = os.getenv('db_name')

# Create the DB engine
db_url = f'postgresql+psycopg2://{username}:{password}@{hostname}:{port}/{db_name}'
engine = create_engine(db_url)

# Define fields to extract
fields_to_extract = ['Div', 'Date', 'Time', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG']
directories = ['Premiership', 'Championship', 'League_1']

# Function to get maximum date from the database
def get_max_date(league):
    query = f'SELECT MAX("Date") FROM "{schemaa}"."{league}"'
    max_date_df = pd.read_sql_query(query, engine)
    return max_date_df.iloc[0, 0]

def read_and_load_football(url, league, season):
    # Get the maximum date from the database
    max_date = get_max_date(league)

    # Read the CSV file into a DataFrame
    response = rq.get(url, timeout=10)
    if response.status_code == 200:
        try:
            season_data = pd.read_csv(url, usecols=fields_to_extract)
            season_data.insert(1, "season", season)

            # Filter for dates greater than the maximum date
            season_data['Date'] = pd.to_datetime(season_data['Date'], dayfirst=True)
            filtered_data = season_data[season_data['Date'] > max_date]
            #filtered_data.rename(columns={'season': 'season'}, inplace=True)

            if not filtered_data.empty:
                # Load the filtered data into the database
                filtered_data.to_sql(league, schema='GoalBet', con=engine, if_exists='append', index=False)#, dtype={"season": VARCHAR})
                logging.info(f'Loaded new data for {season} into {league}.')
            else:
                logging.info(f'No new data to load for season {season} in {league}.')

        except (ValueError, UnicodeDecodeError) as e:
            logging.error(f"Error reading data for {league}: {e}")
    else:
        logging.error(f'File not found for the season {season} in {league}.')

for league in directories:
    read_and_load_football(data_url[league], league, season)

logging.info('Pipeline execution completed.')
