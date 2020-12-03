import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import boto3
keyId = ''
keySec = ''
def main():
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    s3 = boto3.resource('s3', aws_access_key_id=keyId, aws_secret_access_key=keySec)
    bucket = s3.Bucket('scpro2-udacity-data-engineering')
    
if __name__ == "__main__":
    main()

