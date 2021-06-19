import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Processes a single JSON song file.
     - Reads the JSON song file
     - Inserts relevant song data from log file as a record into the `songs`
       table
     - Inserts relevant artist data from log file as a record into the
     `artists` table
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].\
                values[0].tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location',
                      'artist_latitude', 'artist_longitude']].\
                       values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Processes a single log file:
    - Reads the JSON log file
    - Transforms log timestamp data
    - Inserts relevant time data from log file as records into the `time` table
    - Inserts relevant user data from log file as records into the `users`
      table
    - Inserts relevant songplay data from log file and `songs`+`artists`
      tables into `songplays` table
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    t['hour'], t['day'], t['week_of_year'], t['month'], t['year'],\
      t['weekday'] = t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month,\
      t.dt.year, t.dt.weekday

    # insert time data records
    time_data = (list(zip(t, t['hour'], t['day'], t['week_of_year'],
                          t['month'], t['year'], t['weekday'])))
    column_labels = ('start_time', 'hour', 'day', 'week', 'month',
                     'year', 'weekday')
    time_df = pd.DataFrame(time_data, columns=column_labels)

    time_df['start_time']= pd.to_datetime(time_df['start_time'])

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')

    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId,
                         row.level, songid, artistid, row.sessionId,
                         row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Processes all the JSON files from passed `filepath` directory using passed
    `func` function
    """
    # get all files matching extension from directory
    all_files = []
    # generates the filename
    for root, dirs, files in os.walk(filepath):
        # retrieves the file and joins the absolute path
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            # appends the file to the abspath
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - Creates connection+cursor to database
    - Calls helper functions for processing JSON files at
      passed directory `filepath`s
    - Closes database connection
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    # cur.execute("select * from songplays WHERE song_id is not null and artist_id is not null")
    # results = cur.fetchall()
    # print("Result of `select * from songplays WHERE song_id is not null and artist_id is not null`:")
    # print(results)
    conn.close()

if __name__ == "__main__":
    main()
