-- Intermediate model joining recently played tracks with artist and audio features
-- Creates a comprehensive dataset for analysis

with recently_played as (
    select * from {{ ref('stg_recently_played') }}
),

artists as (
    select * from {{ ref('stg_artists') }}
),

audio_features as (
    select * from {{ ref('stg_audio_features') }}
),

joined_data as (
    select
        rp.played_at,
        rp.track_id,
        rp.track_name,
        rp.artist_id,
        rp.artist_name,
        rp.album_name,
        coalesce(a.genres, '[]') as genres,
        af.danceability,
        af.energy,
        af.key,
        af.loudness,
        af.mode,
        af.speechiness,
        af.acousticness,
        af.instrumentalness,
        af.liveness,
        af.valence,
        af.tempo,
        af.duration_ms,
        af.time_signature,
        rp.updated_at
        
    from recently_played rp
    left join artists a on rp.artist_id = a.artist_id
    left join audio_features af on rp.track_id = af.track_id
)

select * from joined_data