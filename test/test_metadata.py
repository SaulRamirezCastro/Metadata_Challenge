# -*- coding: utf-8 -*-
""" 
Created by saul.ramirez at 2/15/2022

"""
import os

import pytest
from metadata import Metadata
import pandas as pd


def test_init_class():
    process = Metadata()

    return process


def test_read_data():
    path = os.path.dirname(os.path.abspath(__file__))
    data = f"{path}/sample_data/sample_data.csv"
    df = pd.read_csv(data)

    return df


# def test_get_geocoding_by_country():
#     process = Metadata()
#     process._read_yaml_file()
#     process._get_geocoding_by_country()
#
#
# def test_get_date_window():
#     process = test_init_class()
#     process._read_yaml_file()
#     # process._db_connection()
#     # process._get_date_window()
#     # process._get_geocoding_by_country()
#     # process._get_response_data()
#     df = test_read_data()
#     process._row_df = df
#     process._get_historical_response_data()
#     process._process_row_data()

def test_process_highest_temperatures():
    process = test_init_class()
    process._read_yaml_file()
    process._db_connection()
    process._set_query()
  #  process._create_tables()
    # process._get_date_window()
    # process._get_geocoding_by_country()
    # process._get_response_data()
    df = test_read_data()
    df['row_date'] = df['row_date'].astype('datetime64[ns]')
    process._row_df = df
    # process._get_historical_response_data()
    process._process_row_data()
    process._process_highest_temperatures()
    process._process_avg_temperatures()

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
