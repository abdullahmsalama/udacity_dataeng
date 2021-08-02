"""
This function contains all drop statements and create statements
needed to drop all tables and create tables with the required schema
as well as inserting data to each table,
for ease of access instead of writing the drop, create and insert statements
for each table each time we want to execute the command

as this function is used by other function just to read the created variables,
it has to input neither returns anything
"""

# DROP TABLES
#the drop_statement was created to not have to write it everytime
drop_statement = "drop table if exists "

songplay_table_drop = drop_statement + "songplays"
user_table_drop = drop_statement + "users"
song_table_drop = drop_statement + "songs"
artist_table_drop = drop_statement + "artists"
time_table_drop = drop_statement + "time"

# CREATE TABLES
## the create_statement was created to not have to write it everytime
create_statement = "create table if not exists "

### I'll do it in star-schema, a fact table (songplay) and 4 dimensions tables (user, song, artist and time tables)

songplay_table_create = create_statement +\
"""songplays (
songplay_id serial primary key,
start_time bigint not null,
user_id int not null,
level varchar,
song_id varchar,
artist_id varchar,
session_id int,
location varchar,
user_agent varchar,
unique(start_time, user_id))"""

user_table_create = create_statement +\
"""users (
user_id int not null primary key,
first_name varchar not null,
last_name varchar not null,
gender varchar,
level varchar)"""

song_table_create = create_statement +\
"""songs (
song_id varchar not null primary key,
title varchar not null,
artist_id varchar not null,
year int,
duration numeric)"""

artist_table_create = create_statement +\
"""artists (
artist_id varchar not null primary key,
name varchar not null,
location varchar,
latitude numeric,
longitude numeric)"""

time_table_create = create_statement +\
"""time (
start_time BIGINT not null primary key,
hour int not null,
day int not null,
week int not null,
month varchar not null,
year int not null,
weekday varchar not null)"""

# INSERT RECORDS

songplay_table_insert = """
insert into songplays(
start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
values (%s, %s, %s, %s, %s, %s, %s, %s)
on conflict (songplay_id) do nothing"""

user_table_insert = """
insert into users values(
%s,
%s,
%s,
%s,
%s) 
on conflict(user_id) do update set level=EXCLUDED.level"""

song_table_insert = """insert into songs values(
%s,
%s,
%s,
%s,
%s)
on conflict (song_id) do nothing"""


artist_table_insert = """insert into artists values
(%s,
 %s,
 %s,
 %s,
 %s)
 on conflict (artist_id) do nothing"""



time_table_insert = """insert into time values(
%s,
%s,
%s,
%s,
%s,
%s,
%s)
on conflict (start_time) do nothing"""


# FIND SONGS

song_select = """
select song_id,
songs.artist_id 
from 
songs 
join artists 
on 
(songs.artist_id = artists.artist_id) \
where 
songs.title = %s and 
artists.name = %s and 
songs.duration = %s"""

# QUERY LISTS

create_table_queries = [songplay_table_create,\
                        user_table_create,\
                        song_table_create,\
                        artist_table_create,\
                        time_table_create]
drop_table_queries = [songplay_table_drop,\
                      user_table_drop,\
                      song_table_drop,\
                      artist_table_drop,\
                      time_table_drop]