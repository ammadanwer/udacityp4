import configparser
from datetime import datetime
import os
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


# config = configparser.ConfigParser()
# config.read('dl.cfg')
#
# os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = spark.read.json(input_data)
    # read song data file

    # extract columns to create songs table
    songs_df = song_data.select('song_id', 'title', 'artist_id', 'year', 'duration')
    #
    # write songs table to parquet files partitioned by year and artist
    songs_df.write.partitionBy('year', 'artist_id').parquet(f'{output_data}songs.pq')
    # extract columns to create artists table
    artists_table = song_data.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude',
                                     'artist_longitude')
    artists_table.show(n=10)
    # write artists table to parquet files
    artists_table.write.parquet(f'{output_data}artists.pq')


def process_log_data(spark, input_data, output_data):
    # get file path to log data file
    # log_data = spark.read.json(input_data)
    #
    # read log data file
    df = spark.read.json(input_data)
    #
    #     # filter by actions for song plays
    df = df.filter(df['page'] == 'NextSong')

    #
    #     # extract columns for users table
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').distinct()
    users_table.show(10)

    #
    #     # write users table to parquet files
    users_table.coalesce(1).write.parquet(f'{output_data}users.pq')
    #
    #     # create timestamp column from original timestamp column
    #     get_timestamp = udf()
    #     df =
    #
    #     # create datetime column from original timestamp column
    #     get_datetime = udf()
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0).isoformat())
    week_day = udf(lambda x: datetime.strptime(x.split('T')[0].strip(), '%Y-%m-%d').strftime('%w'))

    #
    #     # extract columns to create time table
    time_table = df.select('ts')
    time_table = time_table.withColumn('start_time', get_timestamp('ts'))
    time_table = time_table.withColumn('hour', hour('start_time'))
    time_table = time_table.withColumn('day', dayofmonth('start_time'))
    time_table = time_table.withColumn('week', weekofyear('start_time'))
    time_table = time_table.withColumn('month', month('start_time'))
    time_table = time_table.withColumn('year', year('start_time'))
    time_table = time_table.withColumn('weekday', week_day('start_time'))
    time_table.show(n=20)


#
#     # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(f'{output_data}time.pq')
#
#     # read in song data to use for songplays table
    log_df = df.withColumn('songplay_id', monotonically_increasing_id()).\
        withColumn('start_time', get_timestamp('ts')). \
        withColumn('month', month('start_time')). \
        select(col('userId').alias('user_id'), col('sessionId').alias('session_id'), col('userAgent').alias('user_agent'), 'location', 'song', 'level', 'songplay_id', 'start_time', 'month')
    log_df.show(n=20)
#     # extract columns from joined song and log datasets to create songplays table
    songs_df = spark.read.parquet('data/output_data/songs.pq')
    songplays_table = log_df.join(songs_df, log_df.song == songs_df.title).select('*')
    songplays_table.show(n=20)
#
#     # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').parquet(f'{output_data}songsplay.pq')


def main():
    spark = create_spark_session()
    # input_data = "s3://udacity-dend/"
    songs_input_data = "data/song-data/song_data/*/*/*"
    log_input_data = "data/log-data/*"
    output_data = "data/output_data/"

    process_song_data(spark, songs_input_data, output_data)
    process_log_data(spark, log_input_data, output_data)


if __name__ == "__main__":
    main()
