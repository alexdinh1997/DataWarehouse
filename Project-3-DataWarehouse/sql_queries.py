import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Assign new values to simplize read file dwh.cfg to create ETL
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSON_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
ARN = config.get("IAM_ROLE", "ARN")
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
CREATE TABLE staging_events(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR(1),
    itemInSession INT,
    lastName VARCHAR,
    length DECIMAL,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration VARCHAR,
    sessionId INT,
    song VARCHAR,
    status INT,
    ts timestamp,
    userAgent VARCHAR,
    userId INT
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    num_song INT,
    artist_id VARCHAR,
    artist_latitude DECIMAL,
    artist_longtitude DECIMAL,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration decimal,
    year INT)
""")

songplay_table_create = ("""
CREATE TABLE songplays(
    songplay_id   int IDENTITY (0,1),
    start_time timestamp REFERENCES time(start_time) sortkey,
    user_id INT REFERENCES users(user_id) distkey,
    level VARCHAR,
    song_id VARCHAR REFERENCES songs(song_id),
    artist_id VARCHAR REFERENCES artists(artist_id),
    session_id INT NOT NULL,
    location TEXT,
    user_agent TEXT
)
""")

user_table_create = ("""
CREATE TABLE users(
    user_id INT PRIMARY KEY distkey,
    first_name VARCHAR NOT NULL, 
    last_name VARCHAR NOT NULL, 
    gender VARCHAR(1), 
    level VARCHAR)
""")

song_table_create = ("""
CREATE TABLE songs(
    song_id VARCHAR PRIMARY KEY sortkey,
    title TEXT NOT NULL,
    artist_id VARCHAR NOT NULL,
    year INT,
    duration NUMERIC)
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id VARCHAR PRIMARY KEY sortkey,
    name VARCHAR NOT NULL,
    location TEXT,
    latitude DECIMAL,
    longitude DECIMAL)
""")

time_table_create = ("""
CREATE TABLE time(
    start_time timestamp PRIMARY KEY sortkey,
    hour INT NOT NULL,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday VARCHAR)
""")

# STAGING TABLES
# ETL impletementation 
staging_events_copy = ("""
    copy staging_events 
        FROM {} 
        iam_role {} 
        region 'us-west-2'
        FORMAT AS JSON {} 
        timeformat 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(LOG_DATA, ARN, LOG_JSON_PATH)

staging_songs_copy = ("""
    copy staging_songs 
        from {} 
        IAM_ROLE {}
        JSON 'auto'
        region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES
# in selection elements from Staging_event tables, we need to put page='NextSong' as a condition
songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)\
    select distinct se.ts, 
        se.userId, 
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId,
        se.location, 
        se.userAgent
    from staging_events se
    inner join staging_songs ss
    on se.song=ss.title and se.artist =ss.artist_name
    where se.page='NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT se.userId, 
                        se.firstName, 
                        se.lastName, 
                        se.gender, 
                        se.level
        FROM staging_events se
        WHERE se.userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
        SELECT DISTINCT ss.song_id, 
                        ss.title, 
                        ss.artist_id, 
                        ss.year, 
                        ss.duration
        FROM staging_songs ss
        WHERE ss.song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT ss.artist_id, 
                        ss.artist_name, 
                        ss.artist_location,
                        ss.artist_latitude,
                        ss.artist_longtitude
        FROM staging_songs ss
        WHERE ss.artist_id IS NOT NULL;
""")
# A little bit tricky here, we need to extract the time divisions via timestamp ts
time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT se.ts,
                        EXTRACT(hour from se.ts),
                        EXTRACT(day from se.ts),
                        EXTRACT(week from se.ts),
                        EXTRACT(month from se.ts),
                        EXTRACT(year from se.ts),
                        EXTRACT(weekday from se.ts)
        FROM staging_events se
        WHERE se.page='NextSong'
""")

# QUERY LISTS
# Due to conflicts time table and songplay, we create the fact table songplay for last

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
