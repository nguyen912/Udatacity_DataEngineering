import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events(
        artist varchar,
        auth varchar,
        firstName varchar,
        gender char(1),
        itemInSession int,
        lastName varchar,
        length decimal,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration varchar,
        sessionId int,
        song varchar,
        status int,
        ts timestamp,
        userAgent varchar,
        userId int
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        num_songs int,
        artist_id varchar,
        artist_latitude decimal,
        artist_longitude decimal,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration decimal,
        year int
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplay(
        songplay_id int identity(0,1) PRIMARY KEY,
        start_time timestamp NOT NULL,
        user_id int NOT NULL,
        level varchar,
        song_id varchar NOT NULL,
        artist_id varchar NOT NULL,
        session_id int NOT NULL,
        location varchar,
        user_agent varchar
    )
""")

user_table_create = ("""
    CREATE TABLE users(
        user_id int PRIMARY KEY,
        first_name varchar,
        last_name varchar,
        gender char(1),
        level varchar
    )
""")

song_table_create = ("""
    CREATE TABLE songs(
        song_id varchar PRIMARY KEY,
        title varchar,
        artist_id varchar NOT NULL,
        year int,
        duration decimal
    )
""")

artist_table_create = ("""
    CREATE TABLE artists(
        artist_id varchar PRIMARY KEY,
        name varchar,
        location varchar,
        latitude decimal,
        longitude decimal
    )
""")

time_table_create = ("""
    CREATE TABLE times(
        start_time timestamp  PRIMARY KEY,
        hour int, 
        day int, 
        week int, 
        month int, 
        year int, 
        weekday int
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    IAM_ROLE '{}'
    FORMAT AS JSON {}
""").format(
    config.get("S3", "LOG_DATA"),
    ARN,
    config.get("S3", "LOG_JSONPATH")
)

staging_songs_copy = ("""
    COPY staging_events FROM {}
    IAM_ROLE '{}'
    JSON 'auto'
""").format(
    config.get("S3", "SONG_DATA"), 
    ARN
)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay(
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT 
        timestamp 'epoch' + ts/1000 * INTERVAL '1 second' as start_time,
        se.userId,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId,
        se.location,
        se.userAgent
    FROM staging_events se
    WHERE 
    LEFT JOIN staging_songs ss 
        ON se.song = ss.title AND se.artist = ss.artist_name
    WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users(
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT distinct 
        se.userId, 
        se.firstName,
        se.lastName,
        se.gender,
        se.level
    FROM staging_events se
    WHERE se.page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs(
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT
        ss.song_id,
        ss.title,
        ss.artist_id,
        ss.year,
        ss.duration
    FROM staging_songs ss
""")

artist_table_insert = ("""
    INSERT INTO artists(
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT 
        ss.artist_id,
        ss.artist_name,
        ss.artist_location,
        ss.artist_latitude,
        ss.artist_longitude
    FROM staging_songs ss
""")

time_table_insert = ("""
    INSERT INTO times(
        start_time,
        hour, 
        day, 
        week, 
        month, 
        year, 
        weekday
    )
    SELECT distinct
        timestamp 'epoch' + ts/1000 * INTERVAL '1 second' as start_time,
        extract(hour from se.ts),
        extract(day from se.ts),
        extract(week from se.ts),
        extract(month from se.ts),
        extract(year from se.ts),
        extract(weekday from se.ts)
    FROM staging_events se
    WHERE se.page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
