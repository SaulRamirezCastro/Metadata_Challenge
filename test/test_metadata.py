# -*- coding: utf-8 -*-
""" 
Created by saul.ramirez at 2/15/2022

"""

import pytest
from metadata import Metadata


def test_init_class():
    process = Metadata()

    return process


def test_get_date_window():
    process = test_init_class()
    process._get_date_window()
