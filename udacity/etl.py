import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import StructType
import boto3
import json
import configparser

def frame_row_from_json(json, spark):
    """frame_row_from_json
    This needs to exist as pyspark can't load in memory json directly
    it only supports reading files from disk. So here we write the json
    payload to a temp file on disc then read it back with spark.
    """
    file = "./temp"
    f = open(file, "w")
    f.write(json)
    f.close()
    return spark.read.json(file)

def process_song_data(spark, ibucket):
    """process_song
    Process a song file by inserting artist and song into s3
    """
    
    artists = frame_row_from_json(json.dumps({
        'artist_id': 'S',
        'name': 'S',
        'location': 'S',
        'latitude': 1,
        'longitude': 1
    }), spark)
    
    songs = frame_row_from_json(json.dumps({
        'song_id': 'S',
        'title': 'S',
        'artist_id': 'S',
        'year': 1,
        'duration': 1
    }), spark)
    
    for obj in ibucket.objects.all():
        if "song_data" in obj.key:
            songRec = json.loads(obj.get()['Body'].read().decode("utf-8"))
            artists = artists.union(frame_row_from_json(json.dumps({
                "artist_id": songRec["artist_id"],
                "name": songRec["artist_name"],
                "location": songRec["artist_location"],
                "latitude": songRec["artist_latitude"],
                "longitude": songRec["artist_longitude"]
            }), spark))
            artists.show()
            print('a')
            songs = songs.union(frame_row_from_json(json.dumps({
                "song_id": songRec["song_id"],
                "title": songRec["title"],
                "artist_id": songRec["artist_id"],
                "year": songRec["year"],
                "duration": songRec["duration"]
            }), spark))
            songs.show()
            print('s')
            

    artists.write.parquet("s3a://scpro2-udacity-data-engineering-output/artists.parquet", mode="overwrite")
    songs.write.parquet("s3a://scpro2-udacity-data-engineering-output/songs.parquet", mode="overwrite")


def process_log(file, spark, outbucket):
    """process_log
    Process a log file by inserting the song play and user into s3
    """
    dayOfTheWeek = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
    jsonS = '[' + file.replace("}", "},")
    jsonS = jsonS[:-1] + ']'
    logs = json.loads(jsonS)
    print(logs)


def print_parquet_files(bucket):
    for obj in bucket.objects.all():
        print(obj.get()['Body'].read())
    
def main():
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    file = open('./creds.json', "r")
    creds = json.loads(file.read())
    file.close()

    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", creds['keyId'])
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", creds['keySec'])

    s3 = boto3.resource('s3', aws_access_key_id=creds['keyId'], aws_secret_access_key=creds['keySec'])
    ibucket = s3.Bucket('scpro2-udacity-data-engineering')

    process_song_data(spark, ibucket)
    
    print_parquet_files(s3.Bucket('scpro2-udacity-data-engineering-output'))
    
    #for obj in ibucket.objects.all():
        #if "log_data" in obj.key:
            #process_log(obj.get()['Body'].read().decode("utf-8"), spark, obucket)


if __name__ == "__main__":
    main()

