import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """
    Description:
    
        This function loads the 'song data' from S3 and extracts the relevant columns to create the 'songs' and 'artists' table
        After that, the data is written back into S3 as separate parquet files
        
    Parameters:
    
        Spark - spark session
        input_data - location for the song data json files in S3 (this is defined at the very bottom of this script)
        output data - location for the parquet files (songs and artists) in S3 (this is defined at the very bottom of this script)
    """

    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/A/A/*.json")
    print('Loaded the song data frame')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = songs_table.dropDuplicates(['song_id']) #there could be multiple records per song_id. We are keeping just one copy

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite') #songs.parquet will be the sub-folder within the main S3 bucket
    print('Outputted the song data frame')

    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artists_table = artists_table.dropDuplicates(['artist_id']) #there could be multiple records per artist_id. We are keeping just one copy
    print('Loaded the artist data frame')

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite') #artists.parquet will be the sub-folder within the main S3 bucket
    print('Outputted the artist data frame')

def process_log_data(spark, input_data, output_data):
    """
    Description: 
    
        This function loads the 'log data' from S3 and extracts the relevant columns to create the 'time' and 'users tables'
        This function also loads the 'song data' so that we can join the two tables to create the 'songplays' table
        After that, the data is written back into S3 as separate parquet files
        
    Parameters:

        Spark - spark session
        input_data - location for the log data json files in S3 (this is defined at the very bottom of this script)
        output data - location for the parquet files (songplays, time, and users) in S3 (this is defined at the very bottom of this script)
    """
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    print('Loaded the Log Data file into df. Size is',df.count()) #the spark program was taking too long, so printing some debug commands
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong') #this is given in the project definition
    print('Filtered the songs')
    
    # extract columns for users table    
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table = users_table.dropDuplicates(['userId']) #there could be multiple records per userId. We are keeping just one copy
    print('Loaded the users data frame')

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite') #users.parquet will be the sub-folder within the main S3 bucket
    print('Outputted the users table into parquet format') 

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn('datetime', get_datetime(df.ts))
    print('Datetime extracted')
    
    # extract columns to create time table
    time_table = df.select(
        col('datetime').alias('start_time'),
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year') 
   )
    time_table = time_table.dropDuplicates(['start_time']) #there could be multiple records per start_time. We are keeping just one copy
    print('Created time table')

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time.parquet'), 'overwrite') #time.parquet will be the sub-folder within the main S3 bucket
    print('Outputted the time table into parquet format') #the spark program was taking too long, so printing some debug commands

    # read in song data to use for songplays table (same script as above but reading into a different data frame)
    song_data = os.path.join(input_data, "song_data/A/A/A/*.json")
    song_df = spark.read.json(song_data)
    print('Loaded the songs dataframe')
    
    # extract columns from joined song and log datasets to create songplays table 
    df = df.orderBy('ts')
    df = df.withColumn('songplay_id', F.monotonically_increasing_id())
    song_df.createOrReplaceTempView('songsView')
    df.createOrReplaceTempView('eventsView')
    print('Created views')

    songplays_table = spark.sql("""
        SELECT
            e.songplay_id,
            e.datetime as start_time,
            e.userId as user_id,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionId as session_id,
            e.location,
            e.userAgent as user_agent,
            year(e.datetime) as year,
            month(e.datetime) as month
        FROM eventsView e
        LEFT JOIN songsView s ON
            e.song = s.title AND
            e.artist = s.artist_name AND
            ABS(e.length - s.duration) < 2
    """)
    print('Joined the tables')
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite') #songplays.parquet will be the sub-folder within the main S3 bucket
    print('Outputted the songplays table into parquet format') 

def main():
    spark = create_spark_session()
    
    #Local mode - works perfectly!
    #input_data = "data/"
    #output_data = 'output_data/'

    #S3 mode
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://premrajamohan-spark-2/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
