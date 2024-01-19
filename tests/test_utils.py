import pytest
from src import utils
import os


def test_configure_spark_session(mocker):
    _spark = mocker.patch('src.utils.SparkSession')
    utils.configure_spark_session()
    _spark.builder.appName.assert_called_once_with("Getting Started")


def test_get_superstore_sales_sdf(mocker):
    _configure_spark_session = mocker.patch('src.utils.configure_spark_session')
    _spark = _configure_spark_session
    _pd = mocker.patch('src.utils.pd')
    utils.get_superstore_sales_sdf(_spark)
    _pd.read_csv.assert_called_once_with('./data/train.csv')
    _spark.createDataFrame.assert_called_once_with(_pd.read_csv.return_value)


