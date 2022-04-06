import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE","ARN")
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSON_PATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar ,
        auth varchar, 
        firstName varchar, 
        gender char(1),
        itemInSession varchar, 
        lastName VARCHAR,
        length float,
        level varchar,
        location text, 
        method varchar, 
        page VARCHAR,
        registration varchar,
        sessionId integer,
        song varchar,
        status integer,
        ts BIGINT ,
        userAgent text, 
        userId integer
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        num_songs integer,
        artist_id varchar, 
        artist_latitude float,
        artist_longitude float,
        artist_location text,
        artist_name varchar,
        song_id varchar, 
        title varchar,
        duration float,
        year integer
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id integer NOT NULL IDENTITY(0,1),
        start_time TIMESTAMP NOT NULL,
        user_id INTEGER NOT NULL,
        level VARCHAR,
        song_id VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        session_id INTEGER,
        location TEXT,
        user_agent TEXT,
        PRIMARY KEY (songplay_id)
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER NOT NULL ,
        first_name VARCHAR,
        last_name VARCHAR,
        gender CHAR(1),
        level VARCHAR,
        PRIMARY KEY (user_id)
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR NOT NULL,
        title VARCHAR,
        artist_id VARCHAR,
        year INTEGER,
        duration FLOAT,
        PRIMARY KEY (song_id)
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR NOT NULL,
        name varchar,
        location TEXT,
        latitude FLOAT,
        longitude FLOAT,
        PRIMARY KEY (artist_id)
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP ,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday VARCHAR,
        PRIMARY KEY (start_time)
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    iam_role {}
    JSON {};
""").format(LOG_DATA,ARN,LOG_JSON_PATH)

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    iam_role {}
    JSON 'auto';
""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level,
    song_id, artist_id, session_id,location,user_agent)
    SELECT DISTINCT 
        timestamp with time zone 'epoch' + se.ts/1000 * interval '1 second',
        se.userId, se.level,ss.song_id,ss.artist_id,se.sessionId,se.location,
        se.userAgent
    FROM staging_events se
    JOIN staging_songs ss 
    ON se.song = ss.title AND se.artist=ss.artist_name
    AND se.length = ss.duration
    WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT se.userId, se.firstName,se.lastName,
        se.gender, se.level
    FROM staging_events se
    WHERE se.page = 'NextSong' AND se.userId IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration 
    FROM staging_songs ss
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT ss.artist_id, ss.artist_name, ss.artist_location,
        ss.artist_latitude, ss.artist_longitude
    FROM staging_songs ss
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT start_time,
        extract(hour from start_time),
        extract(day from start_time),
        extract(week from start_time),
        extract(month from start_time), 
        extract(year from start_time), 
        extract(weekday from start_time)
    FROM songplays

""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
