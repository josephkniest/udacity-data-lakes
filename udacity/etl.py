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

file = open('./s3.json', "r")
s3_config = json.loads(file.read())
file.close()

def frame_row_from_json(json, spark):
    """frame_row_from_json
    This needs to exist as pyspark can't load in memory json directly
    it only supports reading files from disk. So here we write the json
    payload to a temp file on disc then read it back with spark.
    Parameters:
        json {string}: Json buffer to turn into a data frame
        spark {Spark}: The spark client handle
    Return: PandasDataFrame: The data frame for the row created by the in memory json
    """
    file = "./temp"
    f = open(file, "w")
    f.write(json)
    f.close()
    return spark.read.json(file).toPandas()

def process_songs(spark, bucket):
    """process_songs
    Process all song files from the source s3 bucket by inserting artist and song into s3
    Parameters:
        spark {Spark}: The spark client handle
        bucket {Bucket}: The S3 bucket containing the data
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

    spark.createDataFrame(artists).write.parquet(s3_config["artistsParquet"], mode="overwrite")
    spark.createDataFrame(songs).write.parquet(s3_config["songsParquet"], mode="overwrite")

def artist_id_from_name(name, spark):
    """artist_id_from_name
    Get the artist ID from the artist name
    Parameters:
        name {String}: Name of the artist whose ID we want
        spark {Spark}: The spark client handle
    """
    if name is None:
        return None
    
    list = SQLContext(spark.sparkContext).read.parquet(s3_config["artistsParquet"]
        ).select("artist_id"
        ).where("name = '{}'".format(name.replace("'", "''"))).take(1)
    return None if len(list) == 0 else list[0].artist_id
    

def song_id_from_title(title, spark):
    """song_id_from_title
    Get the song ID from the song title
    """
    if title is None:
        return None
    
    list = SQLContext(spark.sparkContext).read.parquet(s3_config["songsParquet"]
        ).select("song_id"
        ).where("title = '{}'".format(title.replace("'", "''"))).take(1)
    return None if len(list) == 0 else list[0].song_id
    
def process_logs(spark, bucket):
    """process_logs
    Process log files from source s3 by inserting the song play and user into s3
    Parameters:
        spark {Spark}: The spark client handle
        bucket {Bucket}: The S3 bucket containing the data
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
                
                plays = plays.append(frame_row_from_json(json.dumps({
                    'start_time': log['ts'],
                    'user_id': log['userId'],
                    'song_id': song_id_from_title(log['song'], spark),
                    'artist_id': artist_id_from_name(log['artist'], spark),
                    'level': log['level'],
                    'session_id': log['sessionId'],
                    'location': log['location'],
                    'user_agent': log['userAgent']
                }), spark))
                
    spark.createDataFrame(time).write.parquet(s3_config["timeParquet"], mode="overwrite")
    spark.createDataFrame(users).write.parquet(s3_config["usersParquet"], mode="overwrite")
    spark.createDataFrame(plays).write.parquet(s3_config["songplaysParquet"], mode="overwrite")
    
def print_parquet_files(ctx):
    """print_parquet_files
    Print the values of the parquet files in the output bucket
    Parameters:
        ctx {SparkContext}: The spark cluster context
    """
    ctx.read.parquet(s3_config["artistsParquet"]).show()
    ctx.read.parquet(s3_config["songsParquet"]).show()
    ctx.read.parquet(s3_config["timeParquet"]).show()
    ctx.read.parquet(s3_config["usersParquet"]).show()
    ctx.read.parquet(s3_config["songplaysParquet"]).show()
    
def main():
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    file = open('./creds.json', "r")
    creds = json.loads(file.read())
    file.close()

    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", creds['keyId'])
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", creds['keySec'])

    s3 = boto3.resource('s3', aws_access_key_id=creds['keyId'], aws_secret_access_key=creds['keySec'])
    bucket = s3.Bucket(s3_config["sourceBucket"])
    
    process_songs(spark, bucket)
    process_logs(spark, bucket)
    print_parquet_files(SQLContext(spark.sparkContext))

if __name__ == "__main__":
    main()

