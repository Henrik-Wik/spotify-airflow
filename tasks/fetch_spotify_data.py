import json
import spotipy

from spotipy.oauth2 import SpotifyOAuth
from config.pg_connect import PgConnect
from datetime import datetime, timezone
from typing import Optional, Tuple, List
from config.auth import CLIENT_ID, CLIENT_SECRET, REDIRECT_URI


class FetchSpotifyData:
    def __init__(self):
        self.client_id: str = CLIENT_ID
        self.client_secret: str = CLIENT_SECRET
        self.redirect_uri: str = REDIRECT_URI

    def get_access_token(self) -> SpotifyOAuth:
        auth_manager = SpotifyOAuth(
            client_id=self.client_id,
            client_secret=self.client_secret,
            redirect_uri=self.redirect_uri,
            scope="user-read-private user-library-read user-read-recently-played",
            cache_path="/opt/airflow/config/.cache",
        )

        return auth_manager

    @staticmethod
    def get_latest_played_timestamp() -> Optional[datetime]:
        """gets latest played timestamp from database to limit the fetched data."""
        timestamp: Optional[datetime] = None
        with PgConnect() as conn:
            if conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(
                        f"SELECT MAX(played_at_timestamp) FROM public.spotify_songs_raw"
                    )
                    timestamp = cursor.fetchone()[0]
                except Exception as e:
                    print(f"Error executing query: {e}")
        return timestamp

    def get_data(self) -> Tuple[str, datetime, List[str]]:
        auth_manager = self.get_access_token()
        sp = spotipy.Spotify(auth_manager=auth_manager)

        # only gets data listened to after the latest played song in the db.
        latest_played_at_timestamp = self.get_latest_played_timestamp()
        latest_unix_timestamp = int(latest_played_at_timestamp.timestamp() * 1000)
        song_data = sp.current_user_recently_played(after=latest_unix_timestamp)
        song_data_raw = json.dumps(song_data)

        # get current and played at time to add to data
        fetched_timestamp_insert = datetime.now(timezone.utc)
        played_at = [item["played_at"] for item in song_data["items"]]

        return song_data_raw, fetched_timestamp_insert, played_at

    def load_data(self) -> None:

        song_data_raw, fetched_timestamp_insert, played_at = self.get_data()

        # if we have data insert it into raw table.
        if played_at:
            latest_played_at_insert = max(played_at)
            with PgConnect() as conn:
                if conn:
                    cursor = conn.cursor()
                    try:
                        cursor.execute(
                            "INSERT INTO spotify_songs_raw (raw_json, fetched_timestamp, played_at_timestamp) VALUES (%s, %s, %s)",
                            (
                                song_data_raw,
                                fetched_timestamp_insert,
                                latest_played_at_insert,
                            ),
                        )
                        conn.commit()
                    except Exception as e:
                        print(f"Error executing query: {e}")
        else:
            print("No new music to fetch. Skipping.")