import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import boto3
import json
import configparser

def load_in_memory_json(json, spark):
    """load_in_memory_json
    This needs to exist as pyspark can't load in memory json directly
    it only supports reading files from disk. So here we write the json
    payload to a temp file on disc then read it back with spark.
    """
    file = "./temp"
    f = open(file, "w")
    f.write(json)
    f.close()
    return spark.read.json(file)
    
def process_song(file, spark, outbucket):
    """process_song
    Process a song file by inserting artist and song into s3
    """
    print(json.loads(file)['artist_id'])
    frame = load_in_memory_json(file, spark)
    frame.write.parquet("s3a://scpro2-udacity-data-engineering-output/", mode="overwrite")
    
    
def process_log(file, spark, outbucket):
    """process_log
    Process a log file by inserting the song play and user into s3
    """
    dayOfTheWeek = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
    jsonS = '[' + file.replace("}", "},")
    jsonS = jsonS[:-1] + ']'
    logs = json.loads(jsonS)
    print(logs)
    
    
def main():
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    file = open('./creds.json', "r")
    creds = json.loads(file.read())
    file.close()

    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", creds['keyId'])
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", creds['keySec'])
    
    s3 = boto3.resource('s3', aws_access_key_id=creds['keyId'], aws_secret_access_key=creds['keySec'])
    ibucket = s3.Bucket('scpro2-udacity-data-engineering')
    obucket = s3.Bucket('scpro2-udacity-data-engineering-output')
    
    for obj in ibucket.objects.all():
        if "song_data" in obj.key:
            process_song(obj.get()['Body'].read().decode("utf-8"), spark, obucket)

    #for obj in ibucket.objects.all():
        #if "log_data" in obj.key:
            #process_log(obj.get()['Body'].read().decode("utf-8"), spark, obucket)

    
if __name__ == "__main__":
    main()

