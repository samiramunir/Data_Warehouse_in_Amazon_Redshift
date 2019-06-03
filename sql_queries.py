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
       (artist_id       VARCHAR,
        auth            VARCHAR,
        first_name      VARCHAR,
        gender          VARCHAR,
        item_in_session    INT,
        last_name       VARCHAR,
        length          numeric,
        level           VARCHAR,
        location        VARCHAR,
        method          VARCHAR,
        page            VARCHAR,
        registration    VARCHAR,
        session_id      INT,
        song            VARCHAR,
        status          INT,
        ts              TIMESTAMP,
        user_agent      TEXT,
        user_id         INT);

""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id           VARCHAR(MAX),
        artist_latitude     NUMERIC,
        artist_location     VARCHAR(MAX),
        artist_longitude    NUMERIC,
        artist_name         VARCHAR(MAX),
        duration            NUMERIC,
        num_songs           INT,
        song_id             VARCHAR(MAX),
        title               VARCHAR(MAX),
        year                INT);
""")




songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id     VARCHAR PRIMARY KEY SORTKEY NOT NULL,
        start_time      TIMESTAMP NOT NULL,
        user_id         INT DISTKEY,
        level           VARCHAR NOT NULL,
        song_id         VARCHAR(MAX) NOT NULL,
        artist_id       VARCHAR(MAX) NOT NULL,
        session_id      INT NOT NULL,
        location        VARCHAR,
        user_agent      TEXT)
    DISTSTYLE KEY;
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id         INT PRIMARY KEY SORTKEY DISTKEY,
        first_name      VARCHAR,
        last_name       VARCHAR,
        gender          VARCHAR,
        level           VARCHAR NOT NULL)
    DISTSTYLE KEY;
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id         VARCHAR(MAX) PRIMARY KEY SORTKEY,
        title           VARCHAR(MAX),
        artist_id       VARCHAR(MAX) NOT NULL DISTKEY,
        year            INT,
        duration        NUMERIC)
    DISTSTYLE KEY;
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id       VARCHAR(MAX) NOT NULL PRIMARY KEY SORTKEY,
        name            VARCHAR(MAX) NOT NULL,
        location        VARCHAR(MAX),
        lattitude       NUMERIC,
        longitude       NUMERIC)
    DISTSTYLE ALL;
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time      TIMESTAMP NOT NULL PRIMARY KEY SORTKEY DISTKEY,
        hour            INT,
        day             INT,
        week            INT,
        month           INT,
        year            INT,
        weekday         INT)
    DISTSTYLE KEY;
""")

# STAGING TABLES

staging_events_copy = """
                    copy staging_events
                    from '{}'
                    credentials 'aws_iam_role={}'
                    region 'us-west-2'
                    JSON '{}'
                    timeformat 'epochmillisecs';
        """.format(log_data, ARN, log_json_path)

staging_songs_copy = """
                    copy staging_songs
                    from '{}'
                    credentials 'aws_iam_role={}'
                    region 'us-west-2'
                    JSON 'auto';
        """.format(song_data, ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays

                                SELECT * FROM

    (SELECT     DISTINCT(concat(a.song, cast(a.ts AS VARCHAR))) AS songplay_id,
                a.ts 			                                AS start_time,
                a.user_id                                       AS user_id,
                a.level                                         AS levl,
                b.song_id                                       AS song_id,
                b.artist_id                                     AS artist_id,
                a.session_id                                    AS session_id,
                a.location		                                AS location,
                a.user_agent                                    AS user_agent

    FROM staging_events a
    JOIN staging_songs  b
    ON a.artist_id = b.artist_name
    WHERE a.page = 'NextSong');
""")


user_table_insert = ("""INSERT INTO users
                        SELECT  s3.user_id, s3.first_name, s3.last_name, s3.gender, s3.level
                        FROM (SELECT s1.user_id, s1.first_name, s1.last_name, s1.gender, s1.level, s1.ts
                                FROM staging_events s1
                                JOIN (SELECT user_id, MAX(ts) AS ts
                                      FROM staging_events
                                      GROUP BY user_id) AS s2
                                ON   s1.user_id = s2.user_id
                                AND  s1.ts = s2.ts) AS s3 ;
""")

song_table_insert = (""" INSERT INTO songs
                            SELECT * FROM
                                        (SELECT DISTINCT(song_id),
                                                         title,
                                                         artist_id,
                                                         year,
                                                         duration
                                         FROM staging_songs);
""")

artist_table_insert = (""" INSERT INTO artists
                            SELECT * FROM
                                        (SELECT DISTINCT(artist_id)     AS artist_id,
                                                artist_name             AS name,
                                                artist_location         AS location,
                                                artist_latitude         AS latitude,
                                                artist_longitude        AS longitude
                                         FROM staging_songs);
""")

time_table_insert = ("""INSERT INTO time
                            SELECT * FROM
                                        (SELECT DISTINCT(ts)                AS start_time,
                                                EXTRACT(hour from ts)       AS hour,
                                                EXTRACT(day from ts)        AS day,
                                                EXTRACT(week from ts)       AS week,
                                                EXTRACT(month from ts)      AS month,
                                                EXTRACT(month from ts)      AS year,
                                                EXTRACT(weekday from ts)    AS weekday
                                         FROM staging_events);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
