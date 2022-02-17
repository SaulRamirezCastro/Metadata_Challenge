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
    """"
    """

    _yaml_config = None  # type: dict
    _locations = None  # type: list
    _row = None        # type: list
    _coon = None

    def __init__(self, **kwargs):
        """
        """
        logger.info("Initialize the class metadata")

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
        for i in range(1, days):
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
        for city in self._locations:
            for day in last_5:
                weather = self._call_api(day, city.get('lat'), city.get('long'), city.get('city'))
                row.append(weather)

        if row:
            self._row = row

    def _process_response_data(self) -> None:
        """"
        """
        dt = pd.DataFrame()
        for location in self._row:
            for k, v in location.items():
                if k == 'hourly':
                    tmp = pd.json_normalize(v)

                if k == 'location':
                    tmp['location'] = v

            dt = pd.concat([dt, tmp])

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
        i = 0
        for query in self._query:
            self._execute_query(self._conn, query)
            if self._conn.notices:
                logger.info(f"Query Message: {self._conn.notices[i]}")
                i += 1
            else:
                logger.info("Create table Done")

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
                else:
                    cursor.execute(query, parameter)
                    conn.commit()

            except Exception as e:
                logger.error(e)

    def _db_connection(self) -> None:
        """"Get the connection to Postgresql data base

        Returns:
            None
        """

        try:
            conn = psycopg2.connect(dbname="metadata",
                                    user="sramirez",
                                    password="sramirez1234",
                                    host="localhost",
                                    port="54320")

            if conn:
                logger.info("Connected to PostgresSQL")
                self._conn = conn

        except (Exception, psycopg2.Error) as error:
            logger.error("Error while connecting to PostgresSQL", error)

