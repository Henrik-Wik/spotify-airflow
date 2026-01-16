-- Staging model for artist data
-- Cleans and standardizes raw artist information

select
    artist_id,
    artist_name,
    genres,
    updated_at

from {{ source('spotify_raw', 'artists_raw') }}

where 
    -- Exclude records with missing critical fields
    artist_id is not null
    and artist_name is not null