# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

songplay_table_create = ("create table  if not exists songplays\
                         (start_time text not null,\
                          user_id int not null, \
                          level text not null, \
                          song_id text not null, \
                          artist_id text not null,\
                          session_id text not null, \
                          location text not null, \
                          user_agent text not null,\
                          PRIMARY KEY (song_id, artist_id, start_time))")

user_table_create = ("create table  if not exists users\
                         (user_id int PRIMARY KEY, \
                          first_name text not null, \
                          last_name text not null, \
                          gender char(1) not null, \
                          level text not null)")


song_table_create = ("create table  if not exists songs\
                         (song_id text PRIMARY KEY, \
                          artist_id text not null, \
                          title text  not null, \
                          year int not null, \
                          duration decimal not null)")

artist_table_create = ("create table  if not exists artists\
                         (artist_id text PRIMARY KEY, \
                          artist_name text  not null, \
                          artist_location text, \
                          artist_longitude text not null, \
                          artist_latitude text not null)")

time_table_create = ("create table  if not exists time \
                         (start_time timestamp PRIMARY KEY, \
                          hour int not null, \
                          day int not null, \
                          week int not null, \
                          month int not null, \
                          year int not null, \
                          weekday int  not null)")

# INSERT RECORDS
#Fact
songplay_table_insert = ("insert into songplays (start_time, user_id, level, song_id, \
                         artist_id, session_id , location, user_agent)\
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s) \
                         on conflict (song_id, artist_id, start_time) do nothing")
#Dimensions
user_table_insert = ("insert into users (user_id, first_name, last_name, gender, level )\
                       VALUES (%s, %s, %s, %s, %s) ON CONFLICT(user_id) DO UPDATE SET level = excluded.level")

song_table_insert = ("insert into songs (song_id, artist_id, title, year, duration)\
                       VALUES (%s, %s, %s, %s, %s) ON CONFLICT(song_id) DO NOTHING")

artist_table_insert = ("insert into artists (artist_id, artist_name, artist_location, artist_longitude, artist_latitude)\
                        VALUES (%s, %s, %s, %s, %s) ON CONFLICT(artist_id) DO NOTHING")

time_table_insert = ("insert into time (start_time, hour, day, week, month, year, weekday)\
                        VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT(start_time) DO NOTHING")

# FIND SONGS

song_select = ("select songs.song_id, songs.artist_id from songs, artists where  songs.artist_id = artists.artist_id \
               and songs.title = %s and artists.artist_name = %s and songs.duration =%s;")


# COUNT CHECKS

songplay_table_cnt = ("select count(*) from songplays")

user_table_cnt = ("select count(*) from users")

song_table_cnt = ("select count(*) from songs")

artist_table_cnt = ("select count(*) from artists")

time_table_cnt = ("select count(*) from time")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]