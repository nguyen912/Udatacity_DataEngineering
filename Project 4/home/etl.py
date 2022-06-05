import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, DateType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Create a Spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Use for: process song data
    Parameters: 
        spark: spark session
        input_data: s3 url where to get input data
        output_data: s3 url where output data is saved after processing
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
            col('song_id'),
            col('title'),
            col('artist_id'),
            col('year'),
            col('duration'))
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.dropDuplicates().write.partitionBy('year', 'artist_id').mode('overwrite').parquet(output_data + 'songs.parquet')

    # extract columns to create artists table
    artists_table = df.select(
        col('artist_id'),
        col('artist_name'),
        col('artist_location'),
        col('artist_latitude'),
        col('artist_longitude')
    )
    
    # write artists table to parquet files
    artists_table.dropDuplicates().write.mode( 'overwrite').parquet( output_data + 'artists.parquet')

def process_log_data(spark, input_data, output_data):
    """
    Use for: process log data
    Parameters: 
        spark: spark session
        input_data: s3 url where to get input data
        output_data: s3 url where output data is saved after processing
    """
    # get filepath to log data file
    log_data =input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df['page'] == 'NextSong')

    # extract columns for users table    
    users_table = df.select(
        col('userId'),
        col('firstName'),
        col('lastName'),
        col('gender'),
        col('level')
    )
    
    # write users table to parquet files
    users_table.dropDuplicates().write.mode( 'overwrite').parquet( output_data + 'users.parquet')

    # create timestamp column from original timestamp column
    get_timestamp = udf(
            lambda x: datetime.fromtimestamp( int(int(x) / 1000)), TimestampType())

    df = df.withColumn('timestamp', get_timestamp( df.ts)) 
    
    # create datetime column from original timestamp column
    get_datetime = udf(
            lambda x: datetime.fromtimestamp(int(int(x) / 1000)), DateType())
    df = df.withColumn( 'datetime', get_timestamp(df.ts))
    
    # extract columns to create time table
    time_table = df.selectExpr(
        'timestamp as start_time', 
        'hour(datetime) as hour',
        'dayofmonth(datetime) as day',
        'weekofyear(datetime) as week',
        'month(datetime) as month',
        'year(datetime) as year',
        'dayofweek(datetime) as weekday'
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.dropDuplicates().write.partitionBy('year', 'month').mode('overwrite').parquet(output_data + 'time.parquet')
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    # read song data to use for songplays_table
    song_df = spark.read.json(song_data)

    song_plays = df.join(
        song_df,
        [
            df.song   == song_df.title,
            df.artist == song_df.artist_name,
            df.length == song_df.duration],
        'left')
    
   # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_plays.selectExpr(
            'monotonically_increasing_id() as songplay_id',
            'timestamp as start_time',
            'userId as user_id',
            'level',
            'song_id',
            'artist_id',
            'sessionID as session_id',
            'location',
            'userAgent as user_agent',
            'month(datetime) as month',
            'year(datetime) as year')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.dropDuplicates().write.partitionBy('year', 'month').mode('overwrite').parquet(output_data + 'songplays.parquet')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://nhnguyen-data-lake-parquet/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
