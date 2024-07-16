TRANSFORM_SPOTIFY_DATA = """
INSERT INTO spotify_data_transformed
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
LEFT JOIN artists_raw a
ON rp.artist_id = a.artist_id
LEFT JOIN audio_features_raw af
ON rp.track_id = af.id
"""
