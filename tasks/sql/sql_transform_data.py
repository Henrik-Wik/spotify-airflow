# Insert transformed data from raw JSON to the transformed table
TRANSFORM_AND_UPDATE_DATA = """
INSERT INTO spotify_songs_transformed (
    played_at_timestamp,
    played_at_date,
    song_name,
    artist_name,
    song_duration_ms,
    song_link,
    album_art_link,
    album_name,
    album_id,
    artist_id,
    track_id,
    updated_at_timestamp
)
SELECT
    (data ->> 'played_at')::TIMESTAMPTZ as played_at_timestamp,
    LEFT(data ->> 'played_at', 10)::DATE as played_at_date,
    data -> 'track' ->> 'name' as song_name,
    data -> 'track' -> 'album' -> 'artists' -> 0 ->> 'name' as artist_name,
    (data -> 'track' ->> 'duration_ms')::INTEGER as song_duration_ms,
    data -> 'track' -> 'external_urls' ->> 'spotify' as song_link,
    data -> 'track' -> 'album' -> 'images' -> 1 ->> 'url' as album_art_link,
    data -> 'track' -> 'album' ->> 'name' as album_name,
    data -> 'track' -> 'album' ->> 'id' as album_id,
    data -> 'track' -> 'artists' -> 0 ->> 'id' as artist_id,
    data -> 'track' ->> 'id' as track_id,
    NOW()::TIMESTAMPTZ as updated_at_timestamp
FROM (
    SELECT jsonb_array_elements(raw_json->'items') as data
    FROM spotify_songs_raw
    WHERE processed = FALSE
) sub
ON CONFLICT (played_at_timestamp) DO UPDATE SET
    updated_at_timestamp = EXCLUDED.updated_at_timestamp;

UPDATE spotify_songs_raw SET processed = TRUE WHERE processed = FALSE;
"""
# the "on conflict" clause lets us ignore rows that are not unique but still update the "updated at" timestamp.
