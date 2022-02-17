# -*- coding: utf-8 -*-
""" 
Created by saul.ramirez at 2/15/2022

"""

import pytest
from metadata import Metadata


def test_init_class():
    process = Metadata()

    return process

# def test_get_geocoding_by_country():
#     process = Metadata()
#     process._read_yaml_file()
#     process._get_geocoding_by_country()
#
#
def test_get_date_window():
    process = test_init_class()
    process._read_yaml_file()
    process._get_date_window()
    process._get_geocoding_by_country()
    process._get_response_data()
    process._process_row_data()
    # process._get_historical_response_data()




# def test_db_connection():
#     process = Metadata()
#     process._db_connection()
#
#
# def test_create_table():
#     process = Metadata()
#     process._read_yaml_file()
#     process._db_connection()
#     process._set_query()
#     process._create_tables()
#
