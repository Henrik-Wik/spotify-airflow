CREATE_RAW_TABLE = """
CREATE TABLE IF NOT EXISTS spotify_songs_raw (
    id SERIAL PRIMARY KEY,
    raw_json JSONB,
    processed BOOLEAN DEFAULT FALSE,
    timestamp TIMESTAMPTZ
);
"""

CREATE_TRANSFORMED_TABLE = """
CREATE TABLE IF NOT EXISTS spotify_songs_transformed (
    played_at_utc TIMESTAMP,
    played_date_utc DATE,
    song_name TEXT,
    artist_name TEXT,
    song_duration_ms INTEGER,
    song_link TEXT,
    album_art_link TEXT,
    album_name TEXT,
    album_id TEXT,
    artist_id TEXT,
    track_id TEXT,
    last_updated_datetime_utc TIMESTAMP
);
"""
