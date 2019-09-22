import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = "s3a://udacity-dend/song_data/*/*/*/*.json"
    
    # read song data file
    df_song_data = spark.read.json(song_data)

    # extract columns to create songs table
    song_table_query = """
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM song_data
    ORDER BY song_id
    """
    
    df_song_data.createOrReplaceTempView("song_data")
    
    songs_table = spark.sql(song_table_query)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").parquet("output/songs_table.parquet")

    # extract columns to create artists table
    artist_table_query = """
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM artists_data
    ORDER BY artist_id
    """
    
    df_song_data.createOrReplaceTempView("artists_data")
    
    artists_table = spark.sql(artist_table_query)
    
    # write artists table to parquet files
    artists_table.write.parquet("output/artists_table.parquet")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = "s3a://udacity-dend/log_data/2018/11/*.json"

    # read log data file
    df_log_data = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_log_data_filtered = df_log_data.filter(df_log_data.page == 'NextSong')

    # extract columns for users table
    user_table_query = """
    SELECT DISTINCT userId as user_id, firstName as first_name, lastName as last_name, gender, level
    FROM users_data
    ORDER BY user_id
    """
    
    df_log_data_filtered.createOrReplaceTempView("users_data")
    
    users_table = spark.sql(user_table_query)
    
    # write users table to parquet files
    users_table.write.parquet("output/users_table.parquet")

    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    df_log_data_filtered = df_log_data_filtered.withColumn('timestamp',F.to_timestamp(F.from_unixtime(df_log_data_filtered.ts/1000, 'yyyy-MM-dd HH:mm:ss')))
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    df_log_data_filtered = df_log_data_filtered.withColumn("hour", F.hour("timestamp"))
    df_log_data_filtered = df_log_data_filtered.withColumn("day", F.dayofmonth("timestamp"))
    df_log_data_filtered = df_log_data_filtered.withColumn("week", F.weekofyear("timestamp"))
    df_log_data_filtered = df_log_data_filtered.withColumn("month", F.month("timestamp"))
    df_log_data_filtered = df_log_data_filtered.withColumn("year", F.year("timestamp"))
    df_log_data_filtered = df_log_data_filtered.withColumn("weekday", F.dayofweek("timestamp"))
    
    # extract columns to create time table
    time_table_query = """
    SELECT DISTINCT timestamp as start_time, hour, day, week, month, year, weekday
    FROM time_data
    ORDER BY start_time
    """
    
    df_log_data_filtered.createOrReplaceTempView("time_data")
    
    time_table = spark.sql(time_table_query)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet("output/time_table.parquet")

    # read in song data to use for songplays table
    #song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplay_table_query = """
    SELECT log_data.timestamp as start_time, log_data.userId as user_id,\
           log_data.level, songlist_data.song_id, songlist_data.artist_id, log_data.sessionId as session_id,\
           log_data.location, log_data.userAgent as user_agent
    FROM log_data
    JOIN songlist_data ON log_data.artist = songlist_data.artist_name
    ORDER BY start_time
    """
    
    df_log_data_filtered.createOrReplaceTempView("log_data")
    df_song_data.createOrReplaceTempView("songlist_data")
    
    songplay_table = spark.sql(songplay_table_query)

    # write songplays table to parquet files partitioned by year and month
    songplay_table.write.partitionBy("year","month").parquet("output/songplay.parquet")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
