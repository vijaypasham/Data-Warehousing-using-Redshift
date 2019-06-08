import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events(
artist VARCHAR(255),
auth VARCHAR(255),
firstName VARCHAR(255),
gender VARCHAR(10),
itemInsession INT,
lastName VARCHAR(255),
length DECIMAL,
level VARCHAR(255),
location VARCHAR(255),
method VARCHAR(255),
page VARCHAR(255),
registration VARCHAR(255),
sessionId INT,
song VARCHAR(255),
status INT,
ts BIGINT,
userAgent VARCHAR(255),
userId INT
)
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs(
num_songs INT, 
artist_id VARCHAR(255), 
artist_latitude DECIMAL,
artist_longitude DECIMAL,
artist_location VARCHAR(255),
artist_name VARCHAR(255),
song_id VARCHAR(255),
title VARCHAR(255),
duration DECIMAL,
year INT
)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays( 
songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
start_time timestamp NOT NULL, 
user_id int, 
level varchar(255), 
song_id varchar(255)  NOT NULL, 
artist_id varchar(255)  NOT NULL, 
session_id int, 
location varchar(255), 
user_agent varchar(255)
)

""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users( 
user_id int PRIMARY KEY, 
first_name varchar(255) NOT NULL, 
last_name varchar(255), 
gender char(1), 
level varchar(255) NOT NULL
)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs( 
song_id varchar(255) PRIMARY KEY, 
title varchar(255) NOT NULL, 
artist_id varchar(255) NOT NULL, 
year int NOT NULL, 
duration numeric NOT NULL
)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists( 
artist_id varchar(255) PRIMARY KEY, 
name varchar(255) NOT NULL, 
location varchar(255), 
lattitude numeric, 
longitude varchar(255)
)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
( 
start_time timestamp PRIMARY KEY, 
hour int NOT NULL, 
day int NOT NULL,  
week int NOT NULL,  
month int NOT NULL, 
year int NOT NULL, 
weekday varchar(255) NOT NULL
)
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {}\
                       credentials 'aws_iam_role={}'\
                       compupdate off region 'us-west-2' \
                       FORMAT AS JSON {} timeformat 'epochmillisecs';\
                       """).format(config.get("S3","LOG_DATA"),
                       config.get("IAM_ROLE","ARN"),config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""COPY staging_songs FROM {}
iam_role  '{}' 
FORMAT AS JSON 'auto'
region 'us-west-2'""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplays(start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
(SELECT TIMESTAMP 'epoch'+ ts/1000 *INTERVAL '1second' AS START_TIME, userId, level,staging_songs.song_id, 
	staging_songs.artist_id, sessionId, location, userAagent FROM staging_events
        LEFT JOIN staging_songs ON staging_events.song = staging_songs.title)
""")

user_table_insert = ("""  INSERT INTO users(user_id, first_name, last_name, gender, level)
    (SELECT DISTINCT userId, firstName, lastName, gender, level FROM staging_events WHERE userId IS NOT NULL)
""")

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration)
    (SELECT DISTINCT song_id, title, artist_id, year, duration from staging_songs)
""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude)
    (SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude from staging_songs)
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT start_time, 
        EXTRACT(hr from start_time) AS hour,
        EXTRACT(d from start_time) AS day,
        EXTRACT(w from start_time) AS week,
        EXTRACT(mon from start_time) AS month,
        EXTRACT(yr from start_time) AS year, 
        EXTRACT(weekday from start_time) AS weekday 
    FROM (SELECT DISTINCT  TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time 
        FROM staging_events)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]