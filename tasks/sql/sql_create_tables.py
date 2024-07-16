CREATE_RECENTLY_PLAYED_RAW_TABLE = """
CREATE TABLE IF NOT EXISTS recently_played_raw (
    played_at TIMESTAMPTZ PRIMARY KEY,
    track_id TEXT,
    track_name TEXT,
    artist_id TEXT,
    artist_name TEXT,
    album_name TEXT,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    transformed BOOLEAN DEFAULT FALSE
);
"""
CREATE_ARTISTS_RAW_TABLE = """
CREATE TABLE IF NOT EXISTS artists_raw (
    artist_id TEXT PRIMARY KEY,
    artist_name TEXT,
    genres TEXT,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
"""
CREATE_AUDIO_FEATURES_RAW_TABLE = """
CREATE TABLE IF NOT EXISTS audio_features_raw (
    id TEXT PRIMARY KEY,
    danceability FLOAT,
    energy FLOAT,
    key INTEGER,
    loudness FLOAT,
    mode INTEGER,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    duration_ms INTEGER,
    time_signature INTEGER,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
"""
CREATE_SPOTIFY_DATA_TRANSFORMED_TABLE = """
CREATE TABLE IF NOT EXISTS spotify_data_transformed (
    played_at TIMESTAMPTZ PRIMARY KEY
,   track_id TEXT
,   track_name TEXT
,   artist_id TEXT
,   artist_name TEXT
,   album_name TEXT
,   genres TEXT
,   danceability FLOAT
,   energy FLOAT
,   key INTEGER
,   loudness FLOAT
,   mode INTEGER
,   speechiness FLOAT
,   acousticness FLOAT
,   instrumentalness FLOAT
,   liveness FLOAT
,   valence FLOAT
,   tempo FLOAT
,   duration_ms INTEGER
,   time_signature INTEGER
,   updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
"""
