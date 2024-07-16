# Insert raw data into tables.
INSERT_RECENTLY_PLAYED_RAW = """
INSERT INTO recently_played_raw (
    played_at
,   track_id
,   track_name
,   artist_id
,   artist_name
,   album_name
)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (played_at) DO NOTHING;

"""
INSERT_ARTISTS_RAW = """
INSERT INTO artists_raw (
    artist_id
,   artist_name
,   genres
)
VALUES (%s, %s, %s)
ON CONFLICT (artist_id) DO NOTHING;
"""

INSERT_AUDIO_FEATURES_RAW = """
INSERT INTO audio_features_raw (
    id
,   danceability
,   energy
,   key
,   loudness
,   mode
,   speechiness
,   acousticness
,   instrumentalness
,   liveness
,   valence
,   tempo
,   duration_ms
,   time_signature
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (id) DO NOTHING;
"""
