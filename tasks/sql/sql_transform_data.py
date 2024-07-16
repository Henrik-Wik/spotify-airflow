TRANSFORM_SPOTIFY_DATA = """
INSERT INTO spotify_data_transformed (
SELECT
    rp.played_at
,   rp.track_id
,   rp.track_name
,   rp.artist_id
,   rp.artist_name
,   rp.album_name
,   a.genres
,   af.danceability
,   af.energy
,   af.key
,   af.loudness
,   af.mode
,   af.speechiness
,   af.acousticness
,   af.instrumentalness
,   af.liveness
,   af.valence
,   af.tempo
,   af.duration_ms
,   af.time_signature
FROM recently_played_raw rp
LEFT JOIN artists_raw a ON rp.artist_id = a.artist_id
LEFT JOIN audio_features_raw af ON rp.track_id = af.id
WHERE rp.transformed = FALSE
)
ON CONFLICT (played_at) DO UPDATE SET
    played_at = Excluded.played_at
,   track_id = Excluded.track_id
,   track_name = Excluded.track_name
,   artist_id = Excluded.artist_id
,   artist_name = Excluded.artist_name
,   album_name = Excluded.album_name
,   genres = Excluded.genres
,   danceability = Excluded.danceability
,   energy = Excluded.energy
,   key = Excluded.key
,   loudness = Excluded.loudness
,   mode = Excluded.mode
,   speechiness = Excluded.speechiness
,   acousticness = Excluded.acousticness
,   instrumentalness = Excluded.instrumentalness
,   liveness = Excluded.liveness
,   valence = Excluded.valence
,   tempo = Excluded.tempo
,   duration_ms = Excluded.duration_ms
,   time_signature = Excluded.time_signature
;

UPDATE recently_played_raw
SET transformed = TRUE
WHERE transformed = FALSE;
"""
