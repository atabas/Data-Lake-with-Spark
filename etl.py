import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

print(config)
os.environ['AWS_ACCESS_KEY_ID']=config.get('creds', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('creds','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*"
    
    # read song data file
    df = spark.read.json(song_data)
    
    df.createOrReplaceTempView("song_table")
    # extract columns to create songs table
    songs_table = spark.sql('''
        SELECT song_table.song_id, 
        song_table.title, 
        song_table.artist_id,
        song_table.year,
        song_table.duration
        FROM song_table
        WHERE song_id IS NOT NULL
    ''')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year','artist_id').parquet(output_data+'songs')

    # extract columns to create artists table
    artists_table = spark.sql('''
        SELECT song_table.artist_id, 
        song_table.artist_name, 
        song_table.artist_location,
        song_table.artist_latitude,
        song_table.artist_longitude
        FROM song_table
        WHERE artist_id IS NOT NULL
    ''')

    # # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*/*"

    log_data = log_data.filter(log_data.page == 'NextSong')
    log_data.createOrReplaceTempView("log_table")

    # extract columns for users table    
    log_artists_table = spark.sql('''
        SELECT DISTINCT log_table.userId AS user_id, 
        log_table.firstName as first_name, 
        log_table.lastName as last_name,
        log_table.gender,
        log_table.level
        FROM log_table
        WHERE log_table.userId IS NOT NULL
    ''')

    # write users table to parquet files
    log_artists_table.write.mode('overwrite').parquet(output_data+'users')

    # extract columns to create time table
    # start_time, hour, day, week, month, year, weekday
    time_table = spark.sql('''
        SELECT log_table.start_time,
        hour(log_table.start_time) AS hour,
        day(log_table.start_time) AS day,
        weekofyear(log_table.start_time) AS week,
        month(log_table.start_time) AS month,
        year(log_table.start_time) AS year,
        dayofweek(log_table.start_time) AS weekday
        FROM log_table
    ''')

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data+'time')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
        SELECT monotonically_increasing_id() AS songplay_id,
        log_table.start_time,
        year(log_table.start_time) AS year,
        month(log_table.start_time) AS month,
        log_table.userId AS user_id,
        log_table.level AS level,
        song_table.song_id AS song_id,
        song_table.artist_id as artist_id,
        log_table.sessionId as session_id,
        log_table.location as location,
        log_table.userAgent as user_agent
        FROM song_table
        JOIN log_table ON song_table.title = log_table.song
    ''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data+'songplays')

            

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://anika-dend/spark-project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
