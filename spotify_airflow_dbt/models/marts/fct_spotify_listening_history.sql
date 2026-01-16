-- Final analytics model for Spotify listening data
-- Provides comprehensive view of listening patterns with audio features

with spotify_data as (
    select * from {{ ref('int_spotify_plays_with_features') }}
),

enhanced_data as (
    select
        -- Core track information
        played_at,
        track_id,
        track_name,
        artist_id,
        artist_name,
        album_name,
        
        -- Time-based analysis columns
        date(played_at) as play_date,
        extract(hour from played_at) as play_hour,
        extract(dow from played_at) as day_of_week,
        extract(month from played_at) as play_month,
        extract(year from played_at) as play_year,
        
        -- Artist genres (parsed from JSON)
        genres,
        
        -- Audio features
        danceability,
        energy,
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
        
        -- Derived metrics
        case 
            when tempo < 60 then 'very_slow'
            when tempo < 90 then 'slow'
            when tempo < 120 then 'moderate'
            when tempo < 140 then 'fast'
            else 'very_fast'
        end as tempo_category,
        
        case
            when duration_ms < 120000 then 'short'
            when duration_ms < 240000 then 'medium'
            when duration_ms < 360000 then 'long'
            else 'very_long'
        end as duration_category,
        
        -- Energy/Valence quadrants for mood analysis
        case
            when energy > 0.5 and valence > 0.5 then 'happy_energetic'
            when energy > 0.5 and valence <= 0.5 then 'angry_energetic'
            when energy <= 0.5 and valence > 0.5 then 'relaxed_happy'
            else 'sad_relaxed'
        end as mood_category,
        
        updated_at
        
    from spotify_data
)

select * from enhanced_data