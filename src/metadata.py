# -*- coding: utf-8 -*-
""" 
Created by saul.ramirez at 2/15/2022

"""
import requests
import psycopg2
import pandas as pd
import os
from geopy.geocoders import Nominatim
import yaml
import geopy
import logging
from datetime import datetime, timedelta, timezone
import json

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


class Metadata:
    """"Class Metadata  to get data from openweather by APi
and store in postgresSql as data base.
    """

    _yaml_config = None  # type: dict
    _locations = None  # type: list
    _row = None  # type: list
    _coon = None
    _row_df = None  # type: pd.DataFrame
    _query = None   # type: list

    def __init__(self,):
        """Init method to execute when the class is Initialize
        """
        logger.info("Initialize the class metadata")
        self.process_metadata_weather()

    def process_metadata_weather(self) -> None:
        """"public method to execute all the process

        Returns:
            None
        """
        self._read_yaml_file()
        self._db_connection()
        self._create_tables()
        self._get_date_window()
        self._get_geocoding_by_country()
        self._get_response_data()
        self._get_historical_response_data()
        self._process_row_data()
        self._process_highest_temperatures()
        self._process_avg_temperatures()

    def _read_yaml_file(self) -> None:
        """Read the Yaml file configuration and set into the class variable
        _yaml_config
        Returns:
            None
        """
        logger.info('Reading config yaml file ')
        path = os.path.dirname(os.path.abspath(__file__))
        yaml_file = f"{path}/config/config.yml"
        with open(yaml_file) as f:
            data = yaml.safe_load(f)

        if data:
            self._yaml_config = data

    def _get_date_window(self) -> int:
        """Get the date from the last 5 days in unix timestamp

        Return:
            last_5_days: return the unix timestamp as integer
        """
        date_list = []
        days = self._yaml_config.get('days')
        for i in range(1, days + 1):
            day = (datetime.utcnow() - timedelta(days=i)).timestamp()
            date_list.append(int(day))

        return date_list

    def _get_geocoding_by_country(self) -> None:
        """get the latitude and longitude by city name and put in the class variable
        _locations

        Returns:
            None
        """
        tmp = []
        geolocator = Nominatim(user_agent=__name__)
        for place in self._yaml_config.get('locations'):
            city = geolocator.geocode(place)
            city_json = {'city': city.address,
                         'lat':  city.latitude,
                         'long': city.longitude}
            tmp.append(city_json)

        if tmp:
            self._locations = tmp

    def _call_api(self, days, lat, long, location):
        """"
        Call the open weather API and get the data from the las 5 days

         Args:
            days: days to get the historical data
            lat: Latitude for the location
            long: longitude for the location

            Return
        """
        openweather_endpoint = self._yaml_config.get('endpoint')
        unit = self._yaml_config.get('unit')
        api = self._yaml_config.get('api')
        endpoint = f"{openweather_endpoint}?lat={lat}&lon={long}&dt={days}&units={unit}&appid={api}"

        response = requests.get(endpoint)
        if response.status_code == 200:
            response = response.text
            row = json.loads(response)
            row['location'] = location

            return row
        else:
            logger.error(f"Error: {response.text}")
            raise Exception(f"Error: {response.text}")

    def _get_response_data(self) -> None:
        """"Iterate the location and get the response for the API and put this as list in the class
        variable _raw

        Returns:
            None
        """
        row = []
        last_5 = self._get_date_window()
        logger.info(f"Call the Open weather API from the last {self._yaml_config.get('days')} days")
        for city in self._locations:
            for day in last_5:
                weather = self._call_api(day, city.get('lat'), city.get('long'), city.get('city'))
                row.append(weather)

        if row:
            self._row = row

    def _insert_data(self, df, table) -> None:
        """"Insert data for table
        Args:
            df: dataframe with the data to insert
            table: table name to insert

        Return:
            None
        """
        data = list(set([tuple(x) for x in df.to_numpy()]))
        tmp = ', '.join("?" * len(df.columns))
        tmp = tmp.replace('?', '%s')
        query = f"INSERT INTO {table} VALUES({tmp})"
        self._execute_query(self._conn, query, data)

    def _process_row_data(self) -> None:
        """"process to insert the row data

        Returns:
            None
        """
        if not self._row_df.empty:
            logger.info("Process and insert the row data")
            table = "row_data"
            self._insert_data(self._row_df, table)

    def _process_highest_temperatures(self) -> None:
        """"Process the row data frame to get the highest temperature by location
        and insert into the table

        Returns:
            None
        """
        if not self._row_df.empty:
            logger.info("Process and insert the highest_temperatures by location ")
            table = "highest_temperatures"
            tmp_df = self._row_df[['row_id', 'temp', 'row_date', 'location']]
            idx = tmp_df.groupby(['location'])['temp'].transform(max) == tmp_df['temp']
            highest = tmp_df[idx]
            self._insert_data(highest, table)

    def _process_avg_temperatures(self) -> None:
        """"Process and transform to get the avg, max,min temp per day with the locations
        and insert into the table avg_temperatures
        Returns: None
        """
        if not self._row_df.empty:
            logger.info("Process and insert the avg_temperatures by day ")
            table = "avg_temperatures"
            tmp_df = self._row_df[['temp', 'row_date', 'location']]
            tmp_df['day'] = tmp_df['row_date'].dt.strftime('%Y/%m/%d')

            idx = tmp_df.groupby(['day'])['temp'].transform(max) == tmp_df['temp']
            tmp_max = tmp_df[idx]
            idx = tmp_df.groupby(['day'])['temp'].transform(min) == tmp_df['temp']
            tmp_min = tmp_df[idx]
            tmp_avg = tmp_df.groupby(['day'])['temp'].mean()
            tmp_avg = tmp_avg.reset_index()

            df_avg_tmp = pd.merge(tmp_min, tmp_max, on='day', how='inner', suffixes=('_min', '_max'))
            df_avg_tmp = pd.merge(df_avg_tmp, tmp_avg, on='day', how='inner')
            df_avg = df_avg_tmp[['day', 'temp', 'temp_min', 'temp_max',
                                 'location_min', 'location_max']]

            self._insert_data(df_avg, table)

    def _get_historical_response_data(self) -> None:
        """"get the historical data from the response and put in a data frame to parser the json and order the columns
        and the convert to a list for future process

        Returns:
            None
        """
        logger.info("get historical data from the las 5 days")
        df = pd.DataFrame()
        for location in self._row:
            tmp_id = ""
            for k, v in location.items():

                if k in ['lat', 'lon']:
                    tmp_id += str(v)

                if k == 'hourly':
                    tmp = pd.json_normalize(v)
                    tmp['id'] = tmp_id
                if k == 'location':
                    tmp['location'] = v

            df = pd.concat([df, tmp])
        row_df = df[['id', 'temp', 'feels_like', 'pressure', 'humidity', 'dew_point', 'uvi', 'clouds', 'visibility',
                     'wind_speed', 'wind_deg', 'wind_gust', 'dt', 'location']]

        row_df.insert(12, 'row_date', pd.to_datetime(row_df['dt'], unit='s'))
        row_df.insert(0, 'row_id', row_df['dt'].astype(str) + row_df['id'])
        row_df = row_df.drop(columns=['id', 'dt'])

        if not row_df.empty:
            self._row_df = row_df

    def _set_query(self) -> None:
        """"Set query value from query.sql file  in the class variable _query.
        Raise:
            Exception: if the query file is empty

        Returns:
            None
        """
        path = os.path.dirname(os.path.abspath(__file__))
        query_f = f"{path}/query/create_tables.sql"

        with open(query_f, 'r') as query_in:
            query = query_in.read()

        query = query.split(';')

        if not query:
            logger.error("The Sql query was not define in query.sql  file")
            raise Exception("The Sql query was not define query file")

        self._query = query

    def _create_tables(self) -> None:
        """"execute query to create tables to store weather Response for API

        Returns:
            None
        """
        logger.info("Creating tables")
        for query in self._query:
            self._execute_query(self._conn, query)

    @staticmethod
    def _execute_query(conn, query, parameter=None) -> tuple:
        """"Method to execute the query.
        Args:
            conn():database connection
            parameter(list): parameters for the query
            query(str): query to execute

        Returns:
            query_data(list): result of the query execution
        """
        with conn.cursor() as cursor:
            try:
                if not parameter:
                    cursor.execute(query)
                    conn.commit()

                if isinstance(parameter, list):
                    cursor.executemany(query, parameter)
                    conn.commit()
                    logger.info("Query execution succeeded ")

            except (Exception, psycopg2.DatabaseError) as error:
                logger.error(f"Error in execute the query: {error}")

    def _db_connection(self) -> None:
        """"Get the connection to Postgresql data base

        Returns:
            None
        """
        try:
            conn = psycopg2.connect(dbname=self._yaml_config['database'].get('db'),
                                    user=self._yaml_config['database'].get('user'),
                                    password=self._yaml_config['database'].get('pass'),
                                    host=self._yaml_config['database'].get('host'),
                                    port="54320")

            if conn:
                logger.info("Connected to PostgresSQL")
                self._conn = conn

        except (Exception, psycopg2.Error) as error:
            logger.error("Error while connecting to PostgresSQL", error)
