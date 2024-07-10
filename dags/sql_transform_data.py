# Insert transformed data from raw JSON to the transformed table
TRANSFORM_AND_UPDATE_DATA = """
INSERT INTO spotify_songs_transformed (
    played_at_utc,
    played_date_utc,
    song_name,
    artist_name,
    song_duration_ms,
    song_link,
    album_art_link,
    album_name,
    album_id,
    artist_id,
    track_id,
    last_updated_datetime_utc
)
SELECT
    (data ->> 'played_at')::TIMESTAMP as played_at_utc,
    LEFT(data ->> 'played_at', 10)::DATE as played_date_utc,
    data -> 'track' ->> 'name' as song_name,
    data -> 'track' -> 'album' -> 'artists' -> 0 ->> 'name' as artist_name,
    (data -> 'track' ->> 'duration_ms')::INTEGER as song_duration_ms,
    data -> 'track' -> 'external_urls' ->> 'spotify' as song_link,
    data -> 'track' -> 'album' -> 'images' -> 1 ->> 'url' as album_art_link,
    data -> 'track' -> 'album' ->> 'name' as album_name,
    data -> 'track' -> 'album' ->> 'id' as album_id,
    data -> 'track' -> 'artists' -> 0 ->> 'id' as artist_id,
    data -> 'track' ->> 'id' as track_id,
    NOW()::TIMESTAMP as last_updated_datetime_utc
FROM (
    SELECT jsonb_array_elements(raw_json->'items') as data
    FROM spotify_songs_raw
    WHERE processed = FALSE
) sub;

UPDATE spotify_songs_raw SET processed = True WHERE processed = FALSE;
"""
