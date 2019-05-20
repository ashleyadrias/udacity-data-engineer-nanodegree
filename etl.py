import configparser
import datetime
import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import monotonically_increasing_id

'''
Fetch AWS IAM Credentials from dl.cfg
'''
#ADD A HEADER TO THE .cfg config file i.e [AWS]
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

'''Return spark session
Initiate a spark session on AWS hadoop
'''
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

'''Return none
Input: spark session, song_input_data json, and output_data S3 path
Purpose: Process song_input data using spark and output as parquet to S3
'''
def process_song_data(spark, song_input_data, output_data):
    # get filepath to song data file
    print('Read song data from json file')
    song_data = spark.read.json(song_input_data)
    
    # read song data file
    print('Print song data schema')
    song_df = song_data
    print(song_df.count())
    song_df.printSchema()

    # extract columns to create songs table
    print('Extract columns to create song table')
    artist_id = "artist_id"
    artist_latitude = "artist_latitude"
    artist_location = "artist_location"
    artist_longitude = "artist_longitude"
    artist_name = "artist_name"
    duration = "duration"
    num_songs = "num_songs"
    song_id = "song_id"
    title = "title"
    year = "year"
    
    print('Songs table: ')
    song_table = song_df.select(song_id, title, artist_id, year, duration)
    print(song_table.limit(5).toPandas())
    
    #create temp view
    song_df.createOrReplaceTempView("song_df_table")
    
    #create sql query to partition on year and artist_id
    song_table = spark.sql(
        """SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM song_df_table 
        GROUP BY year, artist_id
        """)
    
    # write songs table to parquet files partitioned by year and artist
    print('Writing to parquet')
    df_songs_table = songs_table.toPandas()
    year_list = list(set(df_songs_table['year'].tolist()))
    artist_id_list = list(set(df_songs_table['artist_id'].tolist()))
    
    for year in year_list:
        for artist_id in artist_id_list:
            #subset DF
            df_to_parquet = df_songs_table.loc[(df_songs_table['year']==int(year)) & (df_songs_table['artist_id']==str(artist_id))]
            df_to_parquet.write.parquet("{}/songs_table/{}/{}/songs_table.parquet".format(output_data,year,artist_id)

    # extract columns to create artists table
    print('Artist table: ')
    artists_table = song_df.select(artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
    print(artists_table.limit(5).toPandas())
    
    # write artists table to parquet files
    print('Writing artist table to parquet')
    artist_table.write.parquet("{}/artist_table/artist_table.parquet".format(output_data))

'''Return none
Input: spark session, log_input_data json, song_input_data json, and output_data S3 path
Purpose: Process log_input_data data using spark and output as parquet to S3
'''
def process_log_data(spark, log_input_data, song_input_data, output_data):
    # get filepath to log data file
    log_data = spark.read.json(log_input_data)

    # read log data file
    print('Print LOG data schema')
    log_df = log_data
    print(log_df.count())
    log_df.printSchema()
    print(log_df.limit(5).toPandas())

    # extract columns for users table  
    #print('Extract columns to create log table')
    artist= 'artist'
    auth= 'auth'
    firstName= 'firstName'
    gender= 'gender'
    itemInSession= 'itemInSession'
    lastName= 'lastName'
    length= 'length'
    level= 'level'
    location= 'location'
    method= 'method'
    page= 'page'
    registration= 'registration'
    sessionId= 'sessionId'
    song= 'song'
    status= 'status'
    ts= 'ts'
    userAgent= 'userAgent'
    userId= 'userId'
    timestamp='timestamp'
    start_time='start_time'
    hour = 'hour'
    day='day'
    week='week'
    month='month'
    year='year'
    weekday='weekday'
    
    print('Users table: ')
    users_table = log_df.select(firstName, lastName, gender, level, userId)
    print(users_table.limit(5).toPandas())

    #write users table to parquet files
    print('Writing user_table to parquet')
    users_table.write.parquet("{}/user_table/users_table.parquet".format(output_data))
    
    #create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000), TimestampType())
    log_df = log_df.withColumn("timestamp", get_timestamp(log_df.ts))
    log_df.printSchema()
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: F.to_date(x), TimestampType())
    log_df = log_df.withColumn("start_time", get_timestamp(log_df.ts))
    log_df.printSchema()
    log_df.head(1)
    
    # extract columns to create time table
    log_df = log_df.withColumn("hour", F.hour("timestamp"))
    log_df = log_df.withColumn("day", F.dayofweek("timestamp"))
    log_df = log_df.withColumn("week", F.weekofyear("timestamp"))
    log_df = log_df.withColumn("month", F.month("timestamp"))
    log_df = log_df.withColumn("year", F.year("timestamp"))
    log_df = log_df.withColumn("weekday", F.dayofweek("timestamp"))
    
    time_table = log_df.select(start_time, hour, day, week, month, year, weekday)
    
    #create temp view
    time_table.createOrReplaceTempView("time_table")
    
    #create sql query to partition on year and artist_id
    time_table = spark.sql(
        """SELECT DISTINCT start_time, hour, day, week, month, year, weekday
        FROM time_table 
        GROUP BY year, month
        """)
    
    # write time table to parquet files partitioned by year and month
    print('Writing time_table to parquet')
    df_time_table = time_table.toPandas()
    year_list = list(set(df_time_table['year'].tolist()))
    month_list = list(set(df_time_table['month'].tolist()))
    
    for year in year_list:
        for month in month_list:
            #subset DF
            df_to_parquet = df_time_table.loc[(df_songs_table['year']==int(year)) & (df_songs_table['month']==int(month))]
            df_to_parquet.write.parquet("{}/time_table/{}/{}/time_table.parquet".format(output_data,year,month)

    # read in song data to use for songplays table
    song_df = spark.read.json(song_input_data)
    
    #create temp tables
    log_df.createOrReplaceTempView("log_df_table")
    song_df.createOrReplaceTempView("song_df_table")

    songplays_table = spark.sql(
        """SELECT DISTINCT log_df_table.start_time, log_df_table.userId, log_df_table.level, log_df_table.sessionId, log_df_table.location,log_df_table.userAgent, song_df_table.song_id, song_df_table.artist_id
        FROM log_df_table 
        INNER JOIN song_df_table 
        ON song_df_table.artist_name = log_df_table.artist 
        INNER JOIN time_table
        ON time_table.start_time = log_df_table.start_time
        GROUP BY time_table.year, time_table.month
        """)
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    print('Writing songplays to parquet')
    df_songplays_table = songplays_table.toPandas()
    year_list = list(set(df_songplays_table['year'].tolist()))
    month_list = list(set(df_songplays_table['month'].tolist()))
    
    for year in year_list:
        for month in month_list:
            #subset DF
            df_to_parquet = df_songplays_table.loc[(df_songs_table['year']==int(year)) & (df_songs_table['month']==int(month))]
            df_to_parquet.write.parquet("{}/songplays/{}/{}/songsplays_table.parquet".format(output_data,year,month)


def main():
    #Create spark session
    spark = create_spark_session()
    
    #Specify data
    input_data = "s3a://udacity-dend/"
    song_input_data = "data/song-data/song_data/A/A/A/*.json"
    log_input_data = "data/log-data/*.json"
    output_data = "s3a://ashley-dend-udacity-p4"
    
    #Call process functions
    process_song_data(spark, song_input_data, output_data)    
    process_log_data(spark, log_input_data, song_input_data, output_data)


if __name__ == "__main__":
    main()
