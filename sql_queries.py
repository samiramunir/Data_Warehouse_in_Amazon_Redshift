import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get('IAM_ROLE', 'ARN')
log_data = config.get('S3', 'LOG_DATA')
log_json_path = config.get('S3', 'LOG_JSONPATH')
song_data = config.get('S3', 'SONG_DATA')
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
       (artist_id       VARCHAR(MAX) DISTKEY SORTKEY,
        auth            VARCHAR(MAX),
        first_name      VARCHAR(MAX),
        gender          VARCHAR(MAX),
        item_in_session    INT,
        last_name       VARCHAR(MAX),
        length          numeric,
        level           VARCHAR(MAX),
        location        VARCHAR(MAX),
        method          VARCHAR(MAX),
        page            VARCHAR(MAX),
        registration    VARCHAR(MAX),
        session_id      INT,
        song            VARCHAR(MAX),
        status          INT,
        ts              TIMESTAMP,
        user_agent      TEXT,
        user_id         VARCHAR(MAX))
    DISTSTYLE KEY;

""")

#     SELECT (SELECT DISTINCT(concat(a.user_id, cast(a.ts as VARCHAR(MAX))) as songplay_id,
#             a.ts,
#             a.user_id,
#             a.level,
#             b.song_id,
#             a.artist_id,
#             a.session_id,
#             a.location,
#             a.user_agent)
#     FROM staging_events a
#     JOIN staging_songs b ON a.artist_id = b.artist_id
#     WHERE page == 'NextSong';)
# """)

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id           VARCHAR(MAX) DISTKEY SORTKEY,
        artist_latitude     NUMERIC,
        artist_location     VARCHAR(MAX),
        artist_longitude    NUMERIC,
        artist_name         VARCHAR(MAX),
        duration            NUMERIC,
        num_songs           INT,
        song_id             VARCHAR(MAX),
        title               VARCHAR(MAX),
        year                INT)
     DISTSTYLE KEY;
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id     INTEGER PRIMARY KEY SORTKEY,
        start_time      TIMESTAMP NOT NULL,
        user_id         INT NOT NULL DISTKEY,
        level           TEXT,
        song_id         VARCHAR(MAX),
        artist_id       VARCHAR(MAX),
        session_id      INT,
        location        TEXT,
        user_agent      TEXT)
    DISTSTYLE KEY;
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id         INTEGER PRIMARY KEY SORTKEY DISTKEY,
        first_name      TEXT,
        last_name       TEXT,
        gender          TEXT,
        level           TEXT)
    DISTSTYLE KEY;;
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id         INTEGER PRIMARY KEY SORTKEY,
        title           TEXT,
        artist_id       VARCHAR(MAX) NOT NULL DISTKEY,
        year            INT,
        duration        NUMERIC)
    DISTSTYLE KEY;
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id       INTEGER PRIMARY KEY SORTKEY,
        name            TEXT,
        location        VARCHAR(MAX),
        lattitude       NUMERIC,
        longitude       NUMERIC)
    DISTSTYLE ALL;
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time      TIMESTAMP PRIMARY KEY SORTKEY DISTKEY,
        hour            INT,
        day             INT,
        week            INT,
        month           INT,
        year            INT,
        weekday         INT)
    DISTSTYLE KEY;
""")

# STAGING TABLES
table = 'staging_events'
staging_events_copy = """
                    copy {} from '{}'
                    credentials 'aws_iam_role={}'
                    region 'us-west-2' JSON '{}'
                    timeformat 'epochmillisecs';
        """.format(table, log_data, ARN, log_json_path)
table = 'staging_songs'
staging_songs_copy = """
                    copy {} from '{}'
                    credentials 'aws_iam_role={}'
                    region 'us-west-2' JSON '{}'
                    timeformat 'epochmillisecs';
        """.format(table, log_data, ARN, log_json_path)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (songplay_id, start_time,
                                                    user_id, level, song_id,
                                                    artist_id, session_id,
                                                    location, user_agent)
                                            values (%s, %s, %s, %s, %s,
                                                    %s, %s, %s, %s)
                                                    ON CONFLICT (songplay_id)
                                                    DO NOTHING;
""")


user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name,
                                            gender, level)
                                            values (%s, %s, %s, %s, %s)
                                            ON CONFLICT (user_id)
                                            DO UPDATE
                                            SET level=EXCLUDED.level;
""")

song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id,
                                            year, duration)
                                            values (%s, %s, %s, %s, %s)
                                            ON CONFLICT (song_id)
                                            DO NOTHING;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location,
                                                latitude, longitude)
                                                values (%s, %s, %s, %s, %s)
                                                ON CONFLICT (artist_id)
                                                DO NOTHING;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day,
                                            week, month, year, weekday)
                                            values (%s, %s, %s, %s, %s, %s, %s)
                                            ON CONFLICT (start_time)
                                            DO NOTHING;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
