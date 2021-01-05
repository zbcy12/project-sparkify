import os
import io
import glob
import psycopg2
import pandas as pd
from tqdm import tqdm
from sql_queries import *
from psycopg2 import extras


def process_song_file(cur, filepath):

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert data
    song_data = list(
        df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    artist_data = list(df[['artist_id', 'artist_name',
                           'artist_latitude', 'artist_longitude', 'artist_location']])

    try:
        cur.execute(artist_table_insert, artist_data)
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e:
        print(e)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)
    df = df[df['page'] == 'NextSong']

    # convert ts to datetime
    t = pd.to_datetime(df.ts, unit='ms')

    # insert time data
    time_data = (df.ts, t.dt.hour, t.dt.day, t.dt.weekofyear,
                 t.dt.month, t.dt.year, t.dt.weekday)
    time_cols = ('start_time', 'hour', 'day',
                 'week', 'month', 'year', 'weekday')
    d = dict(zip(time_cols, time_data))
    time_df = pd.DataFrame(data=d)
    time_f = io.StringIO(time_df.to_csv(header=False, index=False, sep="\t"))
    cur.copy_from(time_f, "time", columns=time_df.columns)

    # load and insert user data
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    for _, row in user_df.iterrows():
        extras.execute_values(cur, user_table_insert, user_df.values)

    # insert songplay records
    for _, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay data
        songplay_data = (row.ts, row.userId, row.level, songid, artistid,
                         row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files from directory
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        all_files = [os.path.abspath(f) for f in files]

    # number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # process each file
    for i, datafile in tqdm(enumerate(all_files, 1)):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed'.format(i, num_files), flush=True)

def main():
    conn = psycopg2.connect(user="postgres",
                            password="741100",
                            dbname="sparkifydb",
                            host="127.0.0.1")
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()

if __name__ == "__main__":
    main()
