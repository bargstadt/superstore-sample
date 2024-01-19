from pyspark.sql import SparkSession
import pandas as pd

def configure_spark_session():
    spark = SparkSession \
    .builder \
    .appName("Getting Started") \
    .getOrCreate()
    return spark

def get_superstore_sales_sdf(spark):
    df = pd.read_csv('./data/train.csv')
    sdf = spark.createDataFrame(df)
    return sdf

if __name__ == '__main__':
    spark = configure_spark_session()
    sdf = get_superstore_sales_sdf(spark)
    sdf.show(5)