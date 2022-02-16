# -*- coding: utf-8 -*-
""" 
Created by saul.ramirez at 2/15/2022

"""
import requests
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
    _locations = None   # type: list

    def __init__(self, **kwargs):
        """
        """
        logger.info("Initialize the class metadata")

    def _read_yaml_file(self) -> None:
        """Read the Yaml file configuration and set into the class variable
        '_yaml_config'
        Returns:
            None
        """
        logger.info('Reading config yaml file ')
        path = os.path.dirname(os.path.abspath(__file__))
        yaml_file = f"{path}/config.yml"
        with open(yaml_file) as f:
            data = yaml.safe_load(f)

        if data:
            self._yaml_config = data

    def _get_date_window(self) -> int:
        """Get the date from the last 5 days in unix timestamp

        Return:
            last_5_days: return the unix timestamp as integer
        """
        last_5_days = (datetime.utcnow() - timedelta(days=self._yaml_config.get('days'))).timestamp()

        return int(last_5_days)


    def _get_geocoding_by_country(self) -> None:
        """

        :return:
        """

        geolocator = Nominatim(user_agent="test_home")
        city = geolocator.geocode("Tierra Blanca Veracruz")
        print(city.latitude, city.longitude)

    def _call_api(self):
        """"
        Call the open weather API and get the data from the las 5 days
        """
        last_5 = self._get_date_window()
        openweather_endpoint = self._yaml_config.get('endpoint')
        endpoint = f"{openweather_endpoint}?lat=18.5272475&lon=-96.21520077487331&dt={last_5}&units=metric&appid=a90258c97e50044075e289056fe1b24f"

        response = requests.get(endpoint)
        if response.status_code == 200:
            response = response.text
            tmp = json.loads(response)
            print(tmp)
        else:
            logger.error(f"Error: {response.text}")
            raise Exception(f"Error: {response.text}")



