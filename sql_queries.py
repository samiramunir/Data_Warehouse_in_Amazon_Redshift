import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

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
       (artist_id       varchar distkey sortkey,
        auth            varchar,
        first_name      varchar,
        gender          varchar,
        item_in_session    int,
        last_name       varchar,
        length          numeric,
        level           varchar,
        location        varchar,
        method          varchar,
        page            varchar,
        registration    varchar,
        session_id      int,
        song            varchar,
        status          int,
        ts              bigint,
        user_agent      text,
        user_id         varchar)
    DISTSTYLE KEY;

""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id           VARCHAR DISTKEY SORTKEY,
        artist_latitude     NUMERIC,
        artist_location     VARCHAR,
        artist_longitude    NUMERIC,
        artist_name         TEXT,
        duration            NUMERIC,
        num_songs           INT,
        song_id             VARCHAR,
        title               TEXT,
        year                INT)
     DISTSTYLE KEY;
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id     INTEGER PRIMARY KEY SORTKEY,
        start_time      TIMESTAMP,
        user_id         INT DISTKEY,
        level           TEXT,
        song_id         VARCHAR,
        artist_id       VARCHAR,
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
        artist_id       VARCHAR DISTKEY,
        year            INT,
        duration        NUMERIC)
    DISTSTYLE KEY;
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id       INTEGER PRIMARY KEY SORTKEY,
        name            TEXT,
        location        VARCHAR,
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

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
