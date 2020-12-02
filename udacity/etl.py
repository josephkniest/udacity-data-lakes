import json
import datetime
from pyspark.sql import SparkSession
def aws_module(resource):

    """aws_module

    Allocate an AWS resource client

    Parameters:
    resource (string): Resource type, e.g. 'S3'

    """


    file = open('./creds.json', "r")
    creds = json.loads(file.read())
    file.close()
    return boto3.resource(resource, aws_access_key_id=creds['keyId'], aws_secret_access_key=creds['keySec'])


def process_song(file):

    """process_song

    Insert songs and artists into parquet files in S3

    Parameters:
    file (string): Song file content

    """


    song = json.loads(file)
    cur = conn.cursor()
    print(song)
    sql = """
        insert into stage_songs (artist_id, name, location, latitude, longitude, song_id, title, year, duration)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.execute(sql, (
        None if song['artist_id'] is None else song['artist_id'].replace("'", "''"),
        None if song['artist_name'] is None else song['artist_name'].replace("'", "''"),
        None if song['artist_location'] is None else song['artist_location'].replace("'", "''"),
        None if song['artist_latitude'] is None else song['artist_latitude'],
        None if song['artist_longitude'] is None else song['artist_longitude'],
        None if song['song_id'] is None else song['song_id'].replace("'", "''"),
        None if song['title'] is None else song['title'].replace("'", "''"),
        None if song['year'] is None else song['year'],
        None if song['duration'] is None else song['duration']))


def process_log(file):


    """process_log

    Insert users and songplays into parquet files in S3

    Parameters:
    file (string): Log file content

    """

    dayOfTheWeek = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
    jsonS = '[' + file.replace("}", "},")
    jsonS = jsonS[:-1] + ']'
    logs = json.loads(jsonS)
    cur = conn.cursor()
    for log in logs:
        dt = datetime.datetime.fromtimestamp(log["ts"] / 1000.0)
        sql = """
            insert into public.time (start_time, hour, day, week, month, year, weekday)
            values ({}, {}, {}, {}, {}, {}, {})
        """.format(
            log['ts'],
            dt.hour,
            dt.day,
            dt.isocalendar()[1],
            dt.month,
            dt.year,
            dayOfTheWeek[dt.weekday()]
        )

        print(sql)

        sql = """
            insert into stage_logs (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent, first_name, last_name, gender)
            values ({}, '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}')
        """.format(
            log['ts'],
            log['userId'].replace("'", "''"),
            log['level'].replace("'", "''"),
            None if log['song'] is None else song_id_from_song_name(conn, log['song'].replace("'", "''")),
            None if log['artist'] is None else artist_id_from_artist_name(conn, log['artist'].replace("'", "''")),
            log['sessionId'],
            None if log['location'] is None else log['location'].replace("'", "''"),
            None if log['userAgent'] is None else log['userAgent'].replace("'", "''"),
            None if log['firstName'] is None else log['firstName'].replace("'", "''"),
            None if log['lastName'] is None else log['lastName'].replace("'", "''"),
            None if log['gender'] is None else log['gender'].replace("'", "''"))


def song_id_from_song_name(song_name):

    """song_id_from_song_name

    Look up the ID of the song from its name

    Parameters:
    song_name (string): Name of the song whose ID we need

    """

def artist_id_from_artist_name(artist_name):

    """artist_id_from_artist_name

    Look up the ID of the artist from its name

    Parameters:
    song_name (string): Name of the artist whose ID we need

    """

def main():
    spark = SparkSession.builder.master("local").appName("parquet_example").getOrCreate()
    s3 = aws_module('s3')
    bucket = s3.Bucket('scpro2-udacity-data-engineering')
    for obj in bucket.objects.all():
        if "song_data" in obj.key:
            process_song(obj.get()['Body'].read(), conn)

    for obj in bucket.objects.all():
        if "log_data" in obj.key:
            process_log(obj.get()['Body'].read().decode("utf-8"), conn)


    insert_artists_songs(conn)
    insert_users_songplays(conn)
    conn.commit()

    conn.close()

if __name__ == "__main__":
    main()
