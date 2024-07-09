import auth
import pandas as pd
import datetime
import spotipy
from pg_connect import PgConnect

from spotipy.oauth2 import SpotifyOAuth


def fetch_spotify_data():

    sp = spotipy.Spotify(
        auth_manager=SpotifyOAuth(
            client_id=auth.CLIENT_ID,
            client_secret=auth.CLIENT_SECRET,
            redirect_uri=auth.REDIRECT_URI,
            scope="user-read-private user-library-read user-read-recently-played",
        )
    )
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=2)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    recently_played = sp.current_user_recently_played(after=yesterday_unix_timestamp)

    song_list = []
    for row in recently_played["items"]:
        song_id = row["track"]["id"]
        song_name = row["track"]["name"]
        song_duration = row["track"]["duration_ms"]
        song_url = row["track"]["external_urls"]["spotify"]
        song_popularity = row["track"]["popularity"]
        song_time_played = row["played_at"]
        album_id = row["track"]["album"]["id"]
        artist_id = row["track"]["album"]["artists"][0]["id"]
        song_element = {
            "song_id": song_id,
            "song_name": song_name,
            "duration_ms": song_duration,
            "url": song_url,
            "popularity": song_popularity,
            "date_time_played": song_time_played,
            "album_id": album_id,
            "artist_id": artist_id,
        }
        song_list.append(song_element)

    song_df = pd.DataFrame.from_dict(song_list)
    song_df["unique_identifier"] = (
        song_df["song_id"] + "-" + song_df["date_time_played"].astype(str)
    )
    song_df = song_df[
        [
            "unique_identifier",
            "song_id",
            "song_name",
            "duration_ms",
            "url",
            "popularity",
            "date_time_played",
            "album_id",
            "artist_id",
        ]
    ]

    with PgConnect() as engine:
        if engine:
            try:
                song_df.to_sql("songs_raw", engine, index=False, if_exists="append")

            except Exception as e:
                print(f"Error executing query: {e}")


if __name__ == "__main__":
    fetch_spotify_data()
