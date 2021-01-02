import configparser

# CONFIG

# Read in information from dwh.cfg
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Assign values from dwh.cfg to variables for use in staging tables
LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')
IAM = config.get('IAM_ROLE','ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_staging"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# decided to add NOT NULL constraint on songplay_id, start_time, user_id, session_id

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS events_staging (artist varchar, auth varchar, firstName varchar, gender char (1), itemInSession int, lastName varchar, length float, level varchar, location varchar, method varchar, page varchar, registration float, sessionId int, song varchar, status int, ts float, userAgent varchar, userId int);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs_staging (num_songs int, artist_id varchar, artist_latitude float, artist_longitude float, artist_location varchar, artist_name varchar, song_id varchar, title varchar, duration float, year int);
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (songplay_id int IDENTITY(0,1) PRIMARY KEY NOT NULL, start_time timestamp NOT NULL, user_id int NOT NULL, level varchar, song_id varchar, artist_id varchar, session_id int NOT NULL, location varchar, user_agent varchar);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY NOT NULL, first_name varchar, last_name varchar, gender varchar, level varchar);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY, title varchar, artist_id varchar, year int, duration float);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, name varchar, location varchar, latitude float, longitude float);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (start_time timestamp NOT NULL, hour int, day int, week int, month int, year int, weekday int);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy events_staging from {}
    credentials aws_iam_role={}
    format as json {}
""").format(LOG_DATA, IAM, LOG_JSONPATH)

staging_songs_copy = ("""
    copy songs_staging from {}
    credentials aws_iam_role={}
    json 'auto'
""").format(SONG_DATA, IAM)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
        timestamp 'epoch' + e.ts / 1000 * interval '1 second' as start_time, 
        e.userId as user_id, 
        e.level as level, 
        s.song_id as song_id, 
        s.artist_id as artist_id, 
        e.sessionId as session_id, 
        e.location as location, 
        e.userAgent as user_agent 
    FROM events_staging as e 
    JOIN songs_staging as s 
    ON e.artist = s.artist_name AND e.song = s.title
    WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        userId as user_id,
        firstName as first_name,
        lastName as last_name,
        gender as gender,
        level as level
    FROM events_staging
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id as song_id,
        title as title,
        artist_id as artist_id,
        year as year,
        duration as duration
    FROM songs_staging 
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id as artist_id,
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    FROM songs_staging
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        start_time as start_time,
        EXTRACT(HOUR FROM start_time) as hour,
        EXTRACT(DAY FROM start_time) as day,
        EXTRACT(WEEK FROM start_time) as week,
        EXTRACT(MONTH FROM start_time) as month,
        EXTRACT(YEAR FROM start_time) as year,
        EXTRACT(WEEKDAY FROM start_time) as weekday
    FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
