"""
File contains all the sql queries in a string format.

Variables in this file are used in create_tables.py and etl.py.
"""

import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
schema_name = 'sparkify'

# DROP TABLES
staging_events_table_drop = f"DROP TABLE IF EXISTS {schema_name}.stagging_log_data;"
staging_songs_table_drop = f"DROP TABLE IF EXISTS {schema_name}.stagging_song_data;"
songplay_table_drop = f"DROP TABLE IF EXISTS {schema_name}.songplays;"
user_table_drop = f"DROP TABLE IF EXISTS {schema_name}.users;"
song_table_drop = f"DROP TABLE IF EXISTS {schema_name}.songs;"
artist_table_drop = f"DROP TABLE IF EXISTS {schema_name}.artists;"
time_table_drop = f"DROP TABLE IF EXISTS {schema_name}.time;"

# CREATA SCHEMA
create_schema = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"

# CREATE TABLES
staging_events_table_create = f"""
CREATE TABLE IF NOT EXISTS {schema_name}.stagging_log_data(
artist            VARCHAR,
auth              VARCHAR,
firstName         VARCHAR,
gender            VARCHAR,
itemInSession     INT,
lastName          VARCHAR,
length            FLOAT,
level             VARCHAR,
location          VARCHAR,
method            VARCHAR,
page              VARCHAR,
registration      BIGINT,
sessionId         INT,
song              VARCHAR,
status            INT,
ts                BIGINT,
userAgent         VARCHAR,
userId            INT
);
"""

staging_songs_table_create = f"""
CREATE TABLE IF NOT EXISTS {schema_name}.stagging_song_data(
num_songs         INT,
artist_id         VARCHAR,
artist_latitude   FLOAT,
artist_longitude  FLOAT,
artist_location   VARCHAR,
artist_name       VARCHAR,
song_id           VARCHAR,
title             VARCHAR,
duration          FLOAT,
year              INT
);
"""

songplay_table_create = f"""
CREATE TABLE IF NOT EXISTS {schema_name}.songplays(
songplay_id  BIGINT     IDENTITY(0,1)  PRIMARY KEY,
start_time   TIMESTAMP  NOT NULL       SORTKEY      REFERENCES {schema_name}.time(start_time),
user_id      INT        NOT NULL       DISTKEY      REFERENCES {schema_name}.users(user_id),
level        VARCHAR,
song_id      VARCHAR                                REFERENCES {schema_name}.songs(song_id),
artist_id    VARCHAR                                REFERENCES {schema_name}.artists(artist_id),
session_id   INT,
location     VARCHAR,
user_agent   VARCHAR);
"""

user_table_create = f"""
CREATE TABLE IF NOT EXISTS {schema_name}.users(
user_id           INT          PRIMARY KEY      SORTKEY,
first_name        VARCHAR,
last_name         VARCHAR,
gender            VARCHAR,
level             VARCHAR)
diststyle all;
"""

song_table_create = f"""
CREATE TABLE IF NOT EXISTS {schema_name}.songs(
song_id           VARCHAR      PRIMARY KEY      SORTKEY,
title             VARCHAR,
artist_id         VARCHAR,
year              INT,
duration          FLOAT)
diststyle all;
"""

artist_table_create = f"""
CREATE TABLE IF NOT EXISTS {schema_name}.artists(
artist_id         VARCHAR      PRIMARY KEY      SORTKEY,
name              VARCHAR,
location          VARCHAR,
latitude          FLOAT,
longitude         FLOAT)
diststyle all;
"""

time_table_create = f"""
CREATE TABLE IF NOT EXISTS {schema_name}.time(
start_time        TIMESTAMP    PRIMARY KEY      SORTKEY,
hour              INT          NOT NULL,
day               INT          NOT NULL,
week              INT          NOT NULL,
month             INT          NOT NULL,
year              INT          NOT NULL,
weekday           INT          NOT NULL)
diststyle all;
"""

# STAGING TABLES
staging_events_copy = f"""
copy {schema_name}.stagging_log_data from {config.get("S3","LOG_DATA")}
credentials 'aws_iam_role={config.get("IAM_ROLE","ARN")}'
compupdate on region 'us-west-2'
format as json {config.get("S3","LOG_JSONPATH")}
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
"""

staging_songs_copy = f"""
copy {schema_name}.stagging_song_data from {config.get("S3","SONG_DATA")}
credentials 'aws_iam_role={config.get("IAM_ROLE","ARN")}'
compupdate on region 'us-west-2'
format as json 'auto'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
"""

# FINAL TABLES
songplay_table_insert = f"""
INSERT INTO {schema_name}.songplays (start_time, user_id, level, song_id,
                       artist_id, session_id, location, user_agent)
(SELECT
    date_add('ms',ts,'1970-01-01') start_time,
    userId AS user_id,
    level,
    song_id,
    artist_id,
    sessionId AS session_id,
    location,
    userAgent AS user_agent
FROM {schema_name}.stagging_log_data AS events
LEFT JOIN {schema_name}.stagging_song_data AS songs
    ON events.song = songs.title
    AND events.artist = songs.artist_name
    AND events.length = songs.duration
WHERE page = 'NextSong');
"""

user_table_insert = f"""
INSERT INTO {schema_name}.users
(SELECT
        userId AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level
FROM {schema_name}.stagging_log_data
WHERE userId is not Null
GROUP BY 1, 2, 3, 4, 5);
"""

song_table_insert = f"""
INSERT INTO {schema_name}.songs
(SELECT
        song_id,
        title,
        artist_id,
        year,
        duration
FROM {schema_name}.stagging_song_data
GROUP BY 1, 2, 3, 4, 5);
"""

artist_table_insert = f"""
INSERT INTO {schema_name}.artists
(SELECT
        artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
FROM {schema_name}.stagging_song_data
GROUP BY 1, 2, 3, 4, 5);
"""

time_table_insert = f"""
INSERT INTO {schema_name}.time
(SELECT
    date_add('ms',ts,'1970-01-01') start_time,
    CAST(DATE_PART(hour, date_add('ms',ts,'1970-01-01')) AS INT) as hour,
    CAST(DATE_PART(day, date_add('ms',ts,'1970-01-01')) AS INT) as day,
    CAST(DATE_PART(week, date_add('ms',ts,'1970-01-01')) AS INT) as week,
    CAST(DATE_PART(month, date_add('ms',ts,'1970-01-01')) AS INT) as month,
    CAST(DATE_PART(year, date_add('ms',ts,'1970-01-01')) AS INT) as year,
    CAST(DATE_PART(weekday, date_add('ms',ts,'1970-01-01')) AS INT) as weekday
FROM {schema_name}.stagging_log_data
GROUP BY 1, 2, 3, 4, 5, 6, 7);
"""

# QUERY LISTS
create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create,
                        songplay_table_create]
drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]
copy_table_queries = [staging_events_copy,
                      staging_songs_copy]
insert_table_queries = [user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert,
                        songplay_table_insert]
