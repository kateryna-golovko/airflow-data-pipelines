class SqlQueries:
    # CREATE TABLES
    # Create emoty staging and final tables with column names and types defined
    staging_events_table_create = ("""
        DROP TABLE IF EXISTS staging_events;

        CREATE TABLE staging_events (
            artist varchar(255),
            auth varchar(255),
            firstname varchar(255),
            gender varchar(50),
            iteminsession bigint,
            lastname varchar(255),
            length numeric,
            level varchar(50),
            location varchar(500),
            method varchar(50),
            page varchar(100),
            registration numeric,
            sessionid bigint,
            song varchar(255),
            status int,
            ts bigint,
            useragent varchar(500),
            userid bigint
        );
    """)

    staging_songs_table_create = ("""
        DROP TABLE IF EXISTS staging_songs;

        CREATE TABLE staging_songs (
            num_songs int,
            artist_id varchar(255),
            artist_latitude varchar(255),
            artist_longitude varchar(255),
            artist_location varchar(500),
            artist_name varchar(255),
            song_id varchar(255),
            title varchar(255),
            duration float,
            year int 
        );
    """)

    songplay_table_create = ("""
        DROP TABLE IF EXISTS songplay;

        CREATE TABLE songplay (
            songplay_id varchar(32) PRIMARY KEY, 
            start_time timestamp NOT NULL, 
            userid bigint NOT NULL, 
            level varchar(100), 
            song_id varchar(255) NOT NULL, 
            artist_id varchar(255) NOT NULL, 
            sessionid bigint NOT NULL, 
            location varchar(500), 
            useragent varchar(500)    
        );
    """)

    user_table_create = ("""
        DROP TABLE IF EXISTS user_info;

        CREATE TABLE user_info (
            userid bigint PRIMARY KEY,
            firstname varchar(255),
            lastname varchar(255),
            gender varchar(50),
            level varchar(50)
        );
    """)

    song_table_create = ("""
        DROP TABLE IF EXISTS song;

        CREATE TABLE song (
            song_id varchar(255) PRIMARY KEY,
            title varchar(255),
            artist_id varchar(255),
            year int,
            duration float
        );
    """)

    artist_table_create = ("""
        DROP TABLE IF EXISTS artist;

        CREATE TABLE artist (
            artist_id varchar(255) PRIMARY KEY,
            artist_name varchar(255),
            artist_location varchar(500),
            artist_latitude float,
            artist_longitude float
        );
    """)

    time_table_create = ("""
        DROP TABLE IF EXISTS time;

        CREATE TABLE time (
            start_time timestamp PRIMARY KEY,
            hour int,
            day int,
            week int,
            month int,
            year int,
            weekday int
        );
    """)

    # STAGING TABLES
    # Load the data from the S3 bucket into the staging tables, specify IAM role for S3 Read access for Redshift
    # Specify that the file is JSON format
    # The invalid characters will be replaced with '?' during loading to avoid failed load
    # Modify the SQL COPY statements to use dynamic parameters
    staging_events_copy = ("""
        COPY staging_events FROM 's3://kgolovko-data-pipelines/log-data/'
        CREDENTIALS 'aws_iam_role={aws_iam_role}'
        FORMAT AS JSON 'auto'
        REGION '{region}'
        ACCEPTINVCHARS AS '?'; 
    """)

    staging_songs_copy = ("""
        COPY staging_songs FROM 's3://kgolovko-data-pipelines/song-data/'
        CREDENTIALS 'aws_iam_role={aws_iam_role}'
        FORMAT AS JSON '{json_path}'
        REGION '{region}'
        ACCEPTINVCHARS AS '?' 
    """)



    #INSERT DATA
    # Load data from staging tables into the final tables within Redshift
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)