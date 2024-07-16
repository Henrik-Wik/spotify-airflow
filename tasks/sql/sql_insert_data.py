# Insert raw data into tables.
INSERT_RECENTLY_PLAYED_RAW = """
INSERT INTO recently_played_raw (
    played_at
,   track_id
,   track_name
,   artist_id
,   artist_name
,   album_name
,   transformed
,   updated_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (played_at) DO UPDATE SET
    played_at = EXCLUDED.played_at
,   track_id = EXCLUDED.track_id
,   track_name = EXCLUDED.track_name
,   artist_id = EXCLUDED.artist_id
,   artist_name = EXCLUDED.artist_name
,   album_name = EXCLUDED.album_name
,   transformed = EXCLUDED.transformed
,   updated_at = EXCLUDED.updated_at

"""
INSERT_ARTISTS_RAW = """
INSERT INTO artists_raw (
    artist_id
,   artist_name
,   genres
,   updated_at
)
VALUES (%s, %s, %s, %s)
ON CONFLICT (artist_id) DO UPDATE SET
    artist_id = EXCLUDED.artist_id
,   artist_name = EXCLUDED.artist_name
,   genres = EXCLUDED.genres
,   updated_at = EXCLUDED.updated_at
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
,   updated_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (id) DO UPDATE SET
    id = EXCLUDED.id
,   danceability = EXCLUDED.danceability
,   energy = EXCLUDED.energy
,   key = EXCLUDED.key
,   loudness = EXCLUDED.loudness
,   mode = EXCLUDED.mode
,   speechiness = EXCLUDED.speechiness
,   acousticness = EXCLUDED.acousticness
,   instrumentalness = EXCLUDED.instrumentalness
,   liveness = EXCLUDED.liveness
,   valence = EXCLUDED.valence
,   tempo = EXCLUDED.tempo
,   duration_ms = EXCLUDED.duration_ms
,   time_signature = EXCLUDED.time_signature
,   updated_at = EXCLUDED.updated_at
"""
