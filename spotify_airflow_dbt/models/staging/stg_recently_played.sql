-- Staging model for recently played tracks
-- Cleans and standardizes raw recently played data

select
    played_at,
    track_id,
    track_name,
    artist_id,
    artist_name,
    album_name,
    updated_at,
    transformed

from {{ source('spotify_raw', 'recently_played_raw') }}

where 
    -- Only include records that haven't been transformed yet
    transformed = false
    -- Exclude records with missing critical fields
    and track_id is not null
    and artist_id is not null
    and played_at is not null