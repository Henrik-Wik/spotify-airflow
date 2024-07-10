# Insert transformed data from raw JSON to the transformed table
TRANSFORM_AND_UPDATE_DATA = """
INSERT INTO spotify_songs_transformed (
    played_at,
    played_date,
    song_name,
    artist_name,
    song_duration_ms,
    song_link,
    album_art_link,
    album_name,
    album_id,
    artist_id,
    track_id,
    updated_at
)
SELECT
    (data ->> 'played_at')::TIMESTAMP as played_at,
    LEFT(data ->> 'played_at', 10)::DATE as played_date,
    data -> 'track' ->> 'name' as song_name,
    data -> 'track' -> 'album' -> 'artists' -> 0 ->> 'name' as artist_name,
    (data -> 'track' ->> 'duration_ms')::INTEGER as song_duration_ms,
    data -> 'track' -> 'external_urls' ->> 'spotify' as song_link,
    data -> 'track' -> 'album' -> 'images' -> 1 ->> 'url' as album_art_link,
    data -> 'track' -> 'album' ->> 'name' as album_name,
    data -> 'track' -> 'album' ->> 'id' as album_id,
    data -> 'track' -> 'artists' -> 0 ->> 'id' as artist_id,
    data -> 'track' ->> 'id' as track_id,
    NOW()::TIMESTAMP as updated_at
FROM (
    SELECT jsonb_array_elements(raw_json->'items') as data
    FROM spotify_songs_raw
    WHERE processed = FALSE
) sub;

UPDATE spotify_songs_raw SET processed = TRUE WHERE processed = FALSE;
"""
