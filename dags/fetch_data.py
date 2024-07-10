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
