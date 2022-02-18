# -*- coding: utf-8 -*-
""" 
Created by saul.ramirez at 2/15/2022

"""
import os

from src.metadata import Metadata
import pandas as pd


def test_init_class():
    process = Metadata()

    return process


def test_read_yaml_file():
    process = test_init_class()
    process._read_yaml_file()
    assert process._yaml_config is not None


def test_set_query():
    process = test_init_class()
    process._set_query()
    assert process._query is not None


def test_db_connection():
    process = test_init_class()
    process._db_connection()
    assert process._conn is not None


def test_create_table():
    process = test_init_class()
    process._read_yaml_file()
    process._db_connection()
    process._set_query()
    process._create_tables()


def test_read_data():
    path = os.path.dirname(os.path.abspath(__file__))
    data = f"{path}/sample_data/sample_data.csv"
    df = pd.read_csv(data)

    return df


def test_get_geocoding_by_country():
    process = test_init_class()
    process._read_yaml_file()
    process._get_geocoding_by_country()
    assert process._locations is not None


def test_get_date_window():
    process = test_init_class()
    process._read_yaml_file()
    process._get_date_window()


def test_get_response():
    process = test_init_class()
    process._read_yaml_file()
    process._get_date_window()
    process._get_geocoding_by_country()
    process._get_response_data()
    assert process._row is not None


def test_get_historical_response_data():
    process = test_init_class()
    process._read_yaml_file()
    process._get_date_window()
    process._get_geocoding_by_country()
    process._get_response_data()
    process._get_historical_response_data()
    assert process._row_df is not None


def test_process_row_data():
    process = test_init_class()
    process._read_yaml_file()
    process._db_connection()
    process._set_query()
    df = test_read_data()
    df['row_date'] = df['row_date'].astype('datetime64[ns]')
    process._row_df = df
    process._process_row_data()


def test_process_highest_temperatures():
    process = test_init_class()
    process._read_yaml_file()
    process._db_connection()
    process._set_query()
    df = test_read_data()
    df['row_date'] = df['row_date'].astype('datetime64[ns]')
    process._row_df = df
    process._process_highest_temperatures()
    process._process_avg_temperatures()


def test_process_avg_temperatures():
    process = test_init_class()
    process._read_yaml_file()
    process._db_connection()
    process._set_query()
    df = test_read_data()
    df['row_date'] = df['row_date'].astype('datetime64[ns]')
    process._row_df = df
    process._process_highest_temperatures()
