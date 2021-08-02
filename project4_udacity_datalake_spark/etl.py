import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)
    
    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").mode("overwrite").parquet(output_data + "songs")


    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude',\
                              'artist_longitude')\
                      .withColumnRenamed('artist_name', 'name') \
                      .withColumnRenamed('artist_location', 'location') \
                      .withColumnRenamed('artist_latitude', 'latitude') \
                      .withColumnRenamed('artist_longitude', 'longitude')
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select('user_id', 'first_name', 'last_name', 'gender', 'level').dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "users")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(x/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x/1000))))
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select('user_id', 'first_name', 'last_name', 'gender', 'level')
    
    # write time table to parquet files partitioned by year and month
    time_table = df.select('timestamp',
                           df.datetime.alias('start_time'),
                           hour('datetime').alias('hour'),
                           dayofmonth('datetime').alias('day'),
                           weekofyear('datetime').alias('week'),
                           month('datetime').alias('month'),
                           year('datetime').alias('year'),
                           dayofweek('datetime').alias(weekday)).dropDuplicates()
    
    time_table.write.partitionBy("year","month").mode("overwrite").\
    parquet(output_data + "time")

    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, 'song_data/*/*/*/*.json'))

    # extract columns from joined song and log datasets to create songplays table 
    df.alias('log_df')
    joined_table = log_df.join(log_df, log_df.artist == song_df.artist_name)

    songplays_table = joined_df.select(
        log_df.datetime.alias('start_time'),
        log_df.userId.alias('user_id'),
        log_df.level.alias('level'),
        song_df.song_id.alias('song_id'),
        song_df.artist_id.alias('artist_id'),
        log_df.sessionId.alias('session_id'),
        log_df.location.alias('location'), 
        log_df.userAgent.alias('user_agent'),
        year('log_df.datetime').alias('year'),
        month('log_df.datetime').alias('month'))\
        .withColumn('songplay_id', monotonically_increasing_id())


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").mode("overwrite").\
    parquet(output_data + "songplays")


def main():
    spark = create_spark_session()
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-processed-abdullah/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
