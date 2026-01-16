# Databricks notebook source
import sys
import os
from conftest import assertDataFrameEqualIgnoreColumnOrder
from pyspark.testing import assertDataFrameEqual

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from src.example_library.main import sample_function


def test_sample_function():
    assert 1 == 1


def test_count():
    df = sample_function()
    assert df.count() == 3


def test_columns():
    df = sample_function()
    assert df.columns == ["Name", "Age"]


def test_data(spark):
    df1 = sample_function()
    data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
    columns = ["Name", "Age"]
    df2 = spark.createDataFrame(data, columns)
    assertDataFrameEqualIgnoreColumnOrder(df1, df2)

