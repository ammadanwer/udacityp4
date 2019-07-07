## Summary
This project is the fourth project for Udacity Data Engineering Nanodegree Program. The task includes facilitating the Sparkify moving the data warehouse to data lake.
The data consists of the songs to which the users are listening to. This would help Sparkify analyze the user activities.

The project is written in python and uses spark to read and process the data lake files in s3 bucket while the output fact and dimension tables are stored in parquet files in another s3 bucket.

## Source Data 
The source data is in the form of json files given in the following s3 paths:

    s3a://udacity-dend/log_data/*/*/*.json
    s3a://udacity-dend/song_data/*/*/*/*.json
    

Log json files contains songplay events of the users while song_data json files contain list of songs details.
## Database Schema

Following are the fact and dimension tables made in the form of parquet files for this project:
### Dimension Tables:

    users
        columns: user_id, first_name, last_name, gender, level
    songs
        columns: song_id, title, artist_id, year, duration
    artists
        columns: artist_id, name, location, lattitude, longitude
    time
        columns: start_time, hour, day, week, month, year, weekday

### Fact Table:

    songplays
        columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

## To run the project:

    Run command to install requirements.

        pip install -r requirements.txt

    Run python etl.py. This will start pipeline which will read the data from files in the bucket
     and output the tables in the form of parquet files in the bucket path defined in 'output_data' variable.
