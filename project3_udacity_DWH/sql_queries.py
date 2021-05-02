import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
prefix_drop_statement = "drop table if exists "

staging_events_table_drop = prefix_drop_statement + "staging_events"
staging_songs_table_drop = prefix_drop_statement + "staging_songs"
songplays_table_drop = prefix_drop_statement + "songplays"
users_table_drop = prefix_drop_statement + "users"
songs_table_drop = prefix_drop_statement + "songs"
artists_table_drop = prefix_drop_statement + "artists"
time_table_drop = prefix_drop_statement + "time"

# CREATE TABLES
prefix_create_statement = "create table if not exists "

staging_events_table_create = prefix_create_statement + """staging_events (
artist          varchar,
auth            varchar,
first_name      varchar,
gender          varchar,
item_in_session int,
last_name       varchar,
length          numeric,
level           varchar,
location        varchar,
method          varchar,
page            varchar,
registration    numeric,
session_id      int,
song            varchar,
status          int,
ts              timestamp,
user_agent      varchar,
user_id         int
)"""

staging_songs_table_create = prefix_create_statement + """staging_songs ( 
num_songs        int,
artist_id        varchar,
artist_latitude  numeric,
artist_longitude numeric,
artist_location  varchar,
artist_name      varchar,
song_id          varchar,
title            varchar,
duration         numeric,
year             int
)"""

songplays_table_create = prefix_create_statement + """songplays (
songplay_id  int identity(0,1)    not null  primary key, 
start_time   timestamp            not null, 
user_id      int                  not null,
level        varchar              not null, 
song_id      varchar              not null, 
artist_id    varchar              not null, 
session_id   int, 
location     varchar,
user_agent   varchar
)"""

users_table_create = prefix_create_statement + """users (
user_id    int     not null primary key,
first_name varchar not null,
last_name  varchar not null,
gender     varchar,
level      varchar
)"""

songs_table_create = prefix_create_statement + """songs (
song_id   varchar not null primary key,
title     varchar not null,
artist_id varchar not null,
year      int,
duration  numeric
)"""

artists_table_create = prefix_create_statement + """artists (
artist_id varchar not null primary key,
name      varchar not null,
location  varchar,
latitude  numeric,
longitude numeric
)"""

time_table_create = prefix_create_statement + """time (
start_time timestamp  not null primary key,
hour       int        not null,
day        int        not null,
week       int        not null,
month      varchar    not null,
year       int        not null,
weekday    varchar    not null
)"""

# STAGING TABLES

staging_events_copy = """copy staging_events
from {}
credentials 'aws_iam_role={}'
json 'auto'
""".format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = """copy staging_songs
from {}
credentials 'aws_iam_role={}'
json 'auto'
""".format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplays_table_insert = """insert into songplays (
start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select st_events.ts, st_events.user_id, st_events.level, st_songs.song_id, st_songs.artist_id, st_events.session_id, st_events.location, st_events.user_agent
from staging_events as st_events
left join staging_songs as st_songs
on st_events.song = st_songs.title and st_events.artist = st_songs.artist_name
where st_events.page = 'NextSong'
"""

users_table_insert = """insert into users (
user_id, first_name, last_name, gender, level)
select distinct user_id, first_name, last_name, gender, level
from staging_events
where page = 'NextSong'
"""

songs_table_insert = """insert into songs (
song_id, title, artist_id, year, duration)
select distinct song_id, title, artist_id, year, duration
from staging_songs
"""

artists_table_insert = """insert into artists (
artist_id, name, location, latitude, longitude)
select distinct artist_id, artist_name, artist_location, artist_latitude,
artist_longitude
from staging_songs
"""

time_table_insert = """insert into time (
start_time, hour, day, week, month, year, weekday)
select distinct ts, extract (hour from ts), extract (day from ts), extract(week from ts), extract(month from ts),
extract(year from ts), extract(weekday from ts)
from staging_events
where page = 'NextSong'
"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplays_table_create, users_table_create, songs_table_create, artists_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplays_table_drop, users_table_drop, songs_table_drop, artists_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplays_table_insert, users_table_insert, songs_table_insert, artists_table_insert, time_table_insert]
