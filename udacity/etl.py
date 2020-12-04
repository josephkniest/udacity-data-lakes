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
from pyspark.sql import SQLContext
import datetime
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
    return spark.read.json(file).toPandas()

def process_song_data(spark, bucket):
    """process_song
    Process a song file by inserting artist and song into s3
    """
    
    artists = frame_row_from_json(json.dumps({
        'artist_id': 'S',
        'name': 'S',
        'location': 'S',
        'latitude': 1000000.100000,
        'longitude': 1000000.100000
    }), spark)
    
    songs = frame_row_from_json(json.dumps({
        'song_id': 'S',
        'title': 'S',
        'artist_id': 'S',
        'year': 1111,
        'duration': 1000000.100000
    }), spark)
    
    for obj in bucket.objects.all():
        if "song_data" in obj.key:
            songRec = json.loads(obj.get()['Body'].read().decode("utf-8"))
            artists = artists.append(frame_row_from_json(json.dumps({
                "artist_id": songRec["artist_id"],
                "name": songRec["artist_name"],
                "location": songRec["artist_location"],
                "latitude": songRec["artist_latitude"],
                "longitude": songRec["artist_longitude"]
            }), spark))
            songs = songs.append(frame_row_from_json(json.dumps({
                "song_id": songRec["song_id"],
                "title": songRec["title"],
                "artist_id": songRec["artist_id"],
                "year": songRec["year"],
                "duration": songRec["duration"]
            }), spark))

    spark.createDataFrame(artists).write.parquet("s3a://scpro2-udacity-data-engineering-output/artists.parquet", mode="overwrite")
    spark.createDataFrame(songs).write.parquet("s3a://scpro2-udacity-data-engineering-output/songs.parquet", mode="overwrite")

def process_logs(spark, bucket):
    """process_logs
    Process a log file by inserting the song play and user into s3
    """
    
    dayOfTheWeek = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
    
    time = frame_row_from_json(json.dumps({
        'start_time': 1000000100000,
        'hour': 0,
        'day': 0,
        'week': 0,
        'month': 10,
        'year': 0,
        'weekday': 'S'
    }), spark)
    
    users = frame_row_from_json(json.dumps({
        'user_id': 'S',
        'first_name': 'S',
        'last_name': 'S',
        'gender': 'S',
        'level': 'S'
    }), spark)
    
    plays = frame_row_from_json(json.dumps({
        'start_time': 1000000100000,
        'user_id': 'S',
        'song_id': 'S',
        'artist_id': 'S',
        'level': 'S',
        'session_id': 1000000100000,
        'location': 'S',
        'user_agent': 'S'
    }), spark)
    
    for obj in bucket.objects.all():
        if "log_data" in obj.key:
            file = obj.get()['Body'].read().decode("utf-8")
            jsonS = '[' + file.replace("}", "},")
            jsonS = jsonS[:-1] + ']'
            logs = json.loads(jsonS)
            for log in logs:
                dt = datetime.datetime.fromtimestamp(log["ts"] / 1000.0)
                time = time.append(frame_row_from_json(json.dumps({
                    'start_time': log['ts'],
                    'hour': dt.hour,
                    'day': dt.day,
                    'week': dt.isocalendar()[1],
                    'month': dt.month,
                    'year': dt.year,
                    'weekday': dayOfTheWeek[dt.weekday()]
                }), spark))
                
                users = users.append(frame_row_from_json(json.dumps({
                    'user_id': log['userId'],
                    'first_name': log['firstName'],
                    'last_name': log['lastName'],
                    'gender': log['gender'],
                    'level': log['level']
                }), spark))
                
                plays = frame_row_from_json(json.dumps({
                    'start_time': log['ts'],
                    'user_id': log['user_id'],
                    'song_id': 'S',
                    'artist_id': 'S',
                    'level': log['level'],
                    'session_id': log['sessionId'],
                    'location': log['location'],
                    'user_agent': log['userAgent']
                }), spark)
                
    spark.createDataFrame(time).write.parquet("s3a://scpro2-udacity-data-engineering-output/time.parquet", mode="overwrite")
    spark.createDataFrame(users).write.parquet("s3a://scpro2-udacity-data-engineering-output/users.parquet", mode="overwrite")
    spark.createDataFrame(plays).write.parquet("s3a://scpro2-udacity-data-engineering-output/songplays.parquet", mode="overwrite")
    
def print_parquet_files(ctx):
    ctx.read.parquet("s3a://scpro2-udacity-data-engineering-output/artists.parquet").show()
    ctx.read.parquet("s3a://scpro2-udacity-data-engineering-output/songs.parquet").show()
    ctx.read.parquet("s3a://scpro2-udacity-data-engineering-output/time.parquet").show()
    ctx.read.parquet("s3a://scpro2-udacity-data-engineering-output/users.parquet").show()
    ctx.read.parquet("s3a://scpro2-udacity-data-engineering-output/songplays.parquet").show()
    
def main():
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    file = open('./creds.json', "r")
    creds = json.loads(file.read())
    file.close()

    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", creds['keyId'])
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", creds['keySec'])

    s3 = boto3.resource('s3', aws_access_key_id=creds['keyId'], aws_secret_access_key=creds['keySec'])
    bucket = s3.Bucket('scpro2-udacity-data-engineering')
    
    process_song_data(spark, bucket)
    process_logs(spark, bucket)
    print_parquet_files(SQLContext(spark.sparkContext))

if __name__ == "__main__":
    main()

