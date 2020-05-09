# Data Lake with Spark

**Overview**
- ETL pipeline for a Data Lake hosted on AWS S3
- Extracted data from S3, processed it using Spark
- Loaded back into S3 in a set of Fact and Dimension Tables using partitioning and parquet formatting to find insights about songs users listen to
- Deployed this Spark process on an AWS EMR cluster

**Star schema**

Fact Table

*songplays* - records in log data associated with song plays i.e. records with page NextSong (songplay_id, start_time, user_id,
level, song_id, artist_id, session_id, location, user_agent)

Dimension Tables

*users* - users in the app (user_id, first_name, last_name, gender, level)   
*songs* - songs in music database (song_id, title, artist_id, year, duration)  
*artists* - artists in music database (artist_id, name, location, lattitude, longitude)  
*time* - timestamps of records in songplays broken down into specific units (start_time, hour, day, week, month, year, weekday)

**Files**:
- etl.py reads data from S3, processes that data using Spark, and writes them back to S3
- dl.cfg contains AWS credentials

