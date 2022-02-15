# -*- coding: utf-8 -*-
""" 
Created by saul.ramirez at 2/15/2022

"""
import requests
import geopy
import logging
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


class Metadata:
    """"
    """
    _locations = None   # type: list

    def __init__(self, **kwargs):
        """
        """
        logger.info("Initialize the class metadata")

    @staticmethod
    def _get_date_window() -> int:
        """

        :return:
        """
        last_5_days = (datetime.utcnow() - timedelta(days=5)).timestamp()

        return int(last_5_days)

    def _get_geocoding_by_country(self) -> None:
        """"""
        pass

    def _call_api(self):
        """"
        """
        pass