# Drop tables
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE;"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE;"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE;"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE;"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE;"

# create tables
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    gender VARCHAR CHECK (gender in ('F','M', NULL)),
    level VARCHAR CHECK (level in ('free', 'paid'))
    );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
    artist_id VARCHAR REFERENCES artists(artist_id),
    title VARCHAR NOT NULL,
    duration NUMERIC,
    year INT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY,
    artist_name VARCHAR NOT NULL,
    artist_location VARCHAR,
    latitude NUMERIC,
    longitude NUMERIC
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    time_id SERIAL PRIMARY KEY,
    start_time BIGINT,
    hour INT, 
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id SERIAL PRIMARY KEY,
    start_time BIGINT,
    user_id INT REFERENCES users(user_id),
    level VARCHAR CHECK (level in ('free', 'paid')),
    song_id VARCHAR REFERENCES songs(song_id),
    artist_id VARCHAR REFERENCES artists(artist_id),
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR,
    time_id INT REFERENCES time(time_id)
);
""")

# insert data
user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id)
DO UPDATE SET 
level=EXCLUDED.level
WHERE users.level != EXCLUDED.level;
""")



song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id)
DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, artist_name, latitude, longitude, artist_location)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id)
DO NOTHING;
""")

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")


# get songId and artistId for songplay table
song_select = ("""
SELECT song_id, songs.artist_id 
FROM songs JOIN artists ON songs.artist_id = artists.artist_id
WHERE title = (%s) AND artist_name = (%s) AND duration = (%s);
""")

# QUERY LISTS
create_table_queries = [artist_table_create, song_table_create, time_table_create,
                        user_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
