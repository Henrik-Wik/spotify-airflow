import pandas as pd
import spotipy
import json

from pg_connect import PgConnect
from spotipy.oauth2 import SpotifyOAuth
from datetime import timedelta, datetime, timezone
from auth import CLIENT_ID, CLIENT_SECRET, REDIRECT_URI


class FetchSpotifyData:

    def __init__(self):
        self.client_id = CLIENT_ID
        self.client_secret = CLIENT_SECRET
        self.redirect_uri = REDIRECT_URI

    def get_latest_listened_timestamp(self):
        max_played_at_utc = None
        with PgConnect() as conn:
            if conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(
                        "SELECT MAX(played_at_utc) FROM public.spotify_songs_raw"
                    )
                    max_played_at_utc = cursor.fetchall()[0][0]

                except Exception as e:
                    print(f"Error executing query: {e}")

        if max_played_at_utc is None:
            today = datetime.now()
            previous_date = today - timedelta(days=90)
            latest_timestamp = int(previous_date.timestamp()) * 1000

        else:
            max_played_at_utc = datetime.strptime(
                max_played_at_utc, "%Y-%m-%dT%H:%M:%S.%fZ"
            )
            latest_timestamp = int(max_played_at_utc.timestamp()) * 1000

        return latest_timestamp

    def get_data(self):

        sp = spotipy.Spotify(
            auth_manager=SpotifyOAuth(
                client_id=self.client_id,
                client_secret=self.client_secret,
                redirect_uri=self.redirect_uri,
                scope="user-read-private user-library-read user-read-recently-played",
            )
        )

        today = datetime.now()
        yesterday = today - timedelta(days=2)
        yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

        latest_timestamp = self.get_latest_listened_timestamp()

        song_data = sp.current_user_recently_played(after=latest_timestamp)

        song_data_raw = json.dumps(song_data)

        with PgConnect() as conn:
            if conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(
                        "INSERT INTO spotify_songs_raw (song_data_raw) VALUES (%s)",
                        (song_data_raw,),
                    )
                    conn.commit()
                except Exception as e:
                    print(f"Error executing query: {e}")

    def transform_data(self):

        played_at_utc = []
        played_date_utc = []
        song_names = []
        artist_names = []
        song_durations_ms = []
        song_links = []
        album_art_links = []
        album_names = []
        album_ids = []
        artist_ids = []
        track_ids = []

        # Extract only the necessary data from the json object
        for song in self.song_data["items"]:
            played_at_utc.append(song["played_at"])
            played_date_utc.append(song["played_at"][0:10])
            song_names.append(song["track"]["name"])
            artist_names.append(song["track"]["album"]["artists"][0]["name"])
            song_durations_ms.append(song["track"]["duration_ms"])
            song_links.append(song["track"]["external_urls"]["spotify"])
            album_art_links.append(song["track"]["album"]["images"][1]["url"])
            album_names.append(song["track"]["album"]["name"])
            album_ids.append(song["track"]["album"]["id"])
            artist_ids.append(song["track"]["artists"][0]["id"])
            track_ids.append(song["track"]["id"])

        # Prepare a dictionary in order to turn it into a pandas dataframe
        song_dict = {
            "played_at_utc": played_at_utc,
            "played_date_utc": played_date_utc,
            "song_name": song_names,
            "artist_name": artist_names,
            "song_duration_ms": song_durations_ms,
            "song_link": song_links,
            "album_art_link": album_art_links,
            "album_name": album_names,
            "album_id": album_ids,
            "artist_id": artist_ids,
            "track_id": track_ids,
        }

        song_df = pd.DataFrame(
            song_dict,
            columns=[
                "played_at_utc",
                "played_date_utc",
                "song_name",
                "artist_name",
                "song_duration_ms",
                "song_link",
                "album_art_link",
                "album_name",
                "album_id",
                "artist_id",
                "track_id",
            ],
        )

        last_updated_datetime_utc = datetime.now(timezone.utc)
        song_df["last_updated_datetime_utc"] = last_updated_datetime_utc
        song_df = song_df.sort_values("played_at_utc", ascending=True)

        # Remove latest song since last run since this will be a duplicate then write to csv
        song_df = song_df.iloc[2:, :]

        with PgConnect() as conn:
            if conn:
                try:
                    song_df.to_sql(
                        "spotify_songs_raw", conn, index=False, if_exists="append"
                    )

                except Exception as e:
                    print(f"Error executing query: {e}")


if __name__ == "__main__":
    fetch_spotify_data = FetchSpotifyData()
    fetch_spotify_data.get_data()
