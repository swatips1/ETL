#!/usr/bin/env python
# coding: utf-8

# # ETL Processes
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


#Connect to sparkify database and return a cursor.
def connecToDb():
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    return cur, conn

#Get all files available.
def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files

#Get data
def getDataFromSource(dataSource):
    # #### Let's perform ETL on a single file and load a single record into each table to start.
    # - Use the `get_files` function provided above to get a list of all song JSON files in select source
    dataSource = get_files(dataSource)
    # - Select the first data point in this list
    filepath = dataSource[1]
    # - Read thefile and view the data
    df = pd.read_json(filepath, lines=True)
    return df

#Get record count for selected table
def confirmInsertion(table_name, query_name, cur, conn):
    # Confirm successful insertion of Record
    print("Checking data in ", table_name , " table...")
    cur.execute(song_table_cnt)
    if(cur.rowcount == 0):
        print("Check failed. No rows were inserted in ", table_name , " table. Check your code!")
        cnt =0
    else:
        print("Check sucessful.Total number of records in ", table_name , " table: ", cur.rowcount)
        cnt =1
    return cnt

#Check status of the ETL
def checkStatus(cnt):
    # Inform the user of the status of the ELT job
    if (cnt==5):
        print('ETL Operation complete.')
    else:
        print('Processing for one or more tables was not successful. Please fix errors and try again.')


#Process songs table.
def processSongs(df, cur, conn):
    # #### Extract Data for Songs Table
    # - Select columns for song ID, title, artist ID, year, and duration
    # - Use `df.values` to select just the values from the dataframe
    # - Index to select the first (only) record in the dataframe
    # - Convert the array to a list and set it to `song_data`
    song_data = df[['song_id', 'artist_id', 'title', 'year', 'duration']].values.tolist()

    # #### Insert Record into Song Table
    # - Implement the `song_table_insert` query in `sql_queries.py` and run the cell below to insert a record for this song into the `songs` table.
    print('Inserting song...')
    cur.execute(song_table_insert, song_data[0])
    conn.commit()

    # #### Confirm successful insertion of record(s) 
    cnt = confirmInsertion('songs', song_table_insert, cur, conn)
    return cnt

#Process Artists table.
def processArtists(df, cur, conn): 
    #### Extract Data for Artists Table
    # - Select columns for artist ID, name, location, latitude, and longitude
    # - Use `df.values` to select just the values from the dataframe
    # - Index to select the first (only) record in the dataframe
    # - Convert the array to a list and set it to `artist_data`
    artist_data =  df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()

    # #### Insert Record into Artist Table
    # Implement the `artist_table_insert` query in `sql_queries.py` and run the cell below to insert a record for this song's artist into the `artists` table. 
    print('Inserting artist...')
    cur.execute(artist_table_insert, artist_data[0])
    conn.commit()
    
    # #### Confirm successful insertion of record(s) 
    cnt = confirmInsertion('artists', artist_table_cnt, cur, conn)
    return cnt


#Process Time table.
def processTime(df, cur, conn): 
    # #### Extract Data for Time Table
    # - Filter records by `NextSong` action
    df = df.loc[df['page'] == 'NextSong']

    # #### Convert the `ts` timestamp column to datetime
    #   - Hint: the current timestamp is in milliseconds
    df = df.copy() 
    df['ts1'] = df['ts']
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')

    # #### Extract the timestamp, hour, day, week of year, month, year, and weekday from the `ts` column and set `time_data` to a list containing these values in order
    #   - Hint: use pandas' [`dt` attribute](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.dt.html) to access easily datetimelike properties.
    time_data = [df.ts, df.ts.dt.hour, df.ts.dt.day, df.ts.dt.week,
                df.ts.dt.month, df.ts.dt.year, df.ts.dt.weekday]

    # - Specify labels for these columns and set to `column_labels`
    column_labels = (['timestamp, hour, day, week, month, year, weekday'])

    # - Create a dataframe, `time_df,` containing the time data for this file by combining `column_labels` and `time_data` into a dictionary and converting this into a dataframe
    # Extract the timestamp, hour, day, week of year, month, year, and weekday from the ts column and set time_data to a list containing these values in order

    d = {'timestamp':df.ts,'hour':df.ts.dt.hour, 'day':df.ts.dt.day, 'week':df.ts.dt.week, 
        'month':df.ts.dt.month, 'year':df.ts.dt.year, 'weekday':df.ts.dt.weekday}
    time_df = pd.DataFrame(d)

    # #### Insert Records into Time Table
    #- Implement the `time_table_insert` query in `sql_queries.py` and run the cell below to insert records for the timestamps in this log file into the `time` table. 
    # - Remember to run `create_tables.py` before running the cell below to ensure you've created/resetted the `time` table in the sparkify database.
    print('Inserting time...')
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        conn.commit()
    
    # #### Confirm successful insertion of record(s) 
    cnt = confirmInsertion('time', time_table_cnt, cur, conn)
    return cnt

#Process Users table.
def processUsers(df, cur, conn):
    # #### Extract Data for Users Table
    # - Select columns for user ID, first name, last name, gender and level and set to `user_df` 
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']] 
    # #### Insert Records into Users Table
    # - Implement the `user_table_insert` query in `sql_queries.py` and run the cell below to insert records for the users in this log file into the `users` table. 
    # - Remember to run `create_tables.py` before running the cell below to ensure you've created/resetted the `users` table in the sparkify database.
    print('Inserting user...')
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        conn.commit()

    # #### Confirm successful insertion of record(s) 
    cnt = confirmInsertion('users', user_table_cnt, cur, conn)
    return cnt

#Process SongPlays table.
def processSongPlays(df, cur, conn): 
    # #### Extract Data and Songplays Table
    # This one is a little more complicated since information from the songs table, artists table, and original log file are all needed for the `songplays` table. 
    # Since the log file does not specify an ID for either the song or the artist, you'll need to get the song ID and artist ID by querying the songs and artists tables to find matches based on song title, artist name, and song duration time.
    # - Implement the `song_select` query in `sql_queries.py` to find the song ID and artist ID based on the title, artist name, and duration of a song.
    # - Select the timestamp, user ID, level, song ID, artist ID, session ID, location, and user agent and set to `songplay_data`
    # 
    # #### Insert Records into Songplays Table
    # - Implement the `songplay_table_insert` query and run the cell below to insert records for the songplay actions in this log file into the `songplays` table. 
    # - Remember to run `create_tables.py` before running the cell below to ensure you've created/resetted the `songplays` table in the sparkify database.
    print('Inserting songplays...')
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        #Select the timestamp, user ID, level, song ID, artist ID, session ID, location, and user agent and set to `songplay_data`
        songplay_data = (df[['ts', 'userId', 'level', 'song', 'artist', 'sessionId', 'location', 'userAgent']]).values[0].tolist()

        cur.execute(songplay_table_insert, songplay_data)
        conn.commit()

    # #### Confirm successful insertion of record(s) 
    cnt = confirmInsertion('songplays', songplay_table_cnt, cur, conn)
    return cnt

def main():
    """
    Processes JSON files to load data into fact and dimention tables.
    """
    print('Welcome to Sparkify DB ELT.')
    print('This program will create and populate a set of tables required for Sparkify\'s operations')
    
    # Simple counter that validates success of whole operation at the end of the script 
    cnt = 0
    
    # ###Connect to the database
    cur, conn = connecToDb()
    # # Process `song_data`
    # In this first part, you'll perform ETL on the first dataset, `song_data`, to create the `songs` and `artists` dimensional tables.
    df = getDataFromSource('data/song_data')
    
    # ### Now process tables that depend on songs data
    # - Process songs table.
    cnt1 = processSongs(df, cur, conn)
    cnt = cnt + cnt1
    #Process artists table.
    cnt1 = processArtists(df, cur, conn)
    cnt = cnt + cnt1

    # - Process `log_data`
    # In this part, you'll perform ETL on the second dataset, `log_data`, to create the `time` and `users` dimensional tables, as well as the `songplays` fact table.
    df = getDataFromSource('data/log_data')
    # - Filter records by `NextSong` action
    df = df.loc[df['page'] == 'NextSong']

    # ### Now process tables that depend on log data
    # - Process songs table.
    cnt1 = processTime(df, cur, conn)
    cnt = cnt + cnt1
    # - Process users table.
    cnt1 = processUsers(df, cur, conn)
    cnt = cnt + cnt1
    # - Process songplays table.
    cnt1 = processSongPlays(df, cur, conn)
    cnt = cnt + cnt1
    
    # ### Processing complete. Close Connection to Sparkify Database
    conn.close()

    # ###Check status of the ETL
    checkStatus(cnt)
    
    
if __name__ == "__main__":
    main()
    










    



