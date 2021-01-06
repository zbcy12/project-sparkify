import os
import io
import glob
import psycopg2
import pandas as pd
from tqdm import tqdm
from sql_queries import *
from psycopg2 import extras


def process_song_file(cur, filepath):
    '''
    Insert artist and song table for each song JSON file.

            Parameters:
                    cur:  database cursor
                filepath:  absoluate path of song JSON file

    '''

    df = pd.read_json(filepath, lines=True)
    song_data = list(
        df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    artist_data = list(df[['artist_id', 'artist_name',
                           'artist_latitude', 'artist_longitude', 'artist_location']].values[0])

    try:
        cur.execute(artist_table_insert, artist_data)
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e:
        print(e)


def process_log_file(cur, filepath):
    '''
    Insert users, time and songplays table for each log JSON file.

            Parameters:
                    cur:  database connection cursor
                filepath:  absoluate path of log JSON file

    '''
    
    df = pd.read_json(filepath, lines=True)
    df = df[df['page'] == 'NextSong']

    t = pd.to_datetime(df.ts, unit='ms')
    time_data = (df.ts, t.dt.hour, t.dt.day, t.dt.weekofyear,
                 t.dt.month, t.dt.year, t.dt.weekday)
    time_cols = ('start_time', 'hour', 'day',
                 'week', 'month', 'year', 'weekday')
    d = dict(zip(time_cols, time_data))

    time_df = pd.DataFrame(data=d)
    time_f = io.StringIO(time_df.to_csv(header=False, index=False, sep="\t"))
    cur.copy_from(time_f, "time", columns=time_df.columns)

    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    for _, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    for _, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        songplay_data = (row.ts, row.userId, row.level, songid, artistid,
                         row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Get all file path and process data based on func.

            Parameters:
                         cur: connection cursor
                        conn: database connection 
                    filepath: path to data file directory
                        func: function to process data 

    '''

    all_files = []
    for root, _, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    for i, datafile in tqdm(enumerate(all_files, 1)):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed'.format(i, num_files), flush=True)


def main():
    conn = psycopg2.connect(user="student",
                            password="student",
                            dbname="sparkifydb",
                            host="127.0.0.1")
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
