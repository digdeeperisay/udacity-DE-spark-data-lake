## Summary

This project is used to ingest two types of data - 'song data' contains the information of various songs and 'log data' contains metadata information on song plays etc. We will load these JSON files from S3 and create the below fact/dimension tables. The below STAR schema is well suited for analyses around user behaviour.

- Fact Table

- songplays - records in log data associated with song plays i.e. records with page NextSong
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

- Dimension Tables

- users - users in the app
    - user_id, first_name, last_name, gender, level

- songs - songs in music database
    - song_id, title, artist_id, year, duration

- artists - artists in music database
    - artist_id, name, location, lattitude, longitude

- time - timestamps of records in songplays broken down into specific units
    - start_time, hour, day, week, month, year, weekday

We will use the 'etl.py' script to load the raw files from S3, extract the relevant columns for the above tables, and output them as parquet files back into a new bucket in S3. We will use Apache Spark and Python API to conduct this process. 

## View data

If you need to view the raw files inside the terminal, we can unzip the files using the command below.

$ unzip data/log_data.zip -d data
$ unzip data/song_data.zip -d data

After that, you can run 'head -10 filename.json' to view the first few records of filename.json

## Update config file

Inside the dl.cfg file, add a header [NAME] since that is required and update the 'etl.py' file accordingly to look into that [NAME] folder
Set environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the config file

## Update the etl.py file

All the logic to extract data from S3, transform it, and load it back into S3 goes into this file

## How to run

Create an S3 bucket in your AWS account and replace the 'output_data' variable in the main() function in the 'etl.py' script with s3a://<your bucket name>/

Type '$ python etl.py' to run the whole script