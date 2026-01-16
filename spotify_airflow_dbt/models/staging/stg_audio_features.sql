-- Staging model for audio features
-- Cleans and standardizes raw audio features data

select
    id as track_id,
    danceability,
    energy,
    track_name,
    artist_id,
    artist_name,
    album_name,
    key,
    loudness,
    mode,
    speechiness,
    acousticness,
    instrumentalness,
    liveness,
    valence,
    tempo,
    duration_ms,
    time_signature,
    updated_at

from {{ source('spotify_raw', 'audio_features_raw') }}

where 
    -- Exclude records with missing critical fields
    id is not null
    and track_id is not null
    -- Validate audio feature ranges
    and danceability between 0 and 1
    and energy between 0 and 1
    and speechiness between 0 and 1
    and acousticness between 0 and 1
    and instrumentalness between 0 and 1
    and liveness between 0 and 1
    and valence between 0 and 1