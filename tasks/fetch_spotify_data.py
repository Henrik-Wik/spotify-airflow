import spotipy
import pandas as pd

from spotipy.oauth2 import SpotifyOAuth
from config.pg_connect import PgConnect
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List, Dict
from config.auth import CLIENT_ID, CLIENT_SECRET, REDIRECT_URI
from tasks.sql.sql_insert_data import (
    INSERT_ARTISTS_RAW,
    INSERT_AUDIO_FEATURES_RAW,
    INSERT_RECENTLY_PLAYED_RAW,
)


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
                        f"SELECT MAX(played_at) FROM public.recently_played_raw"
                    )
                    timestamp = cursor.fetchone()[0]
                except Exception as e:
                    print(f"Error executing query: {e}")
        if not timestamp:
            timestamp = datetime.now(timezone.utc) - timedelta(days=1)
        return timestamp

    def fetch_data(self) -> Tuple[List[Dict], datetime, Dict[str, Dict]]:
        auth_manager = self.get_access_token()
        sp = spotipy.Spotify(auth_manager=auth_manager)

        # only gets data listened to after the latest played song in the db.
        latest_played_at_timestamp = self.get_latest_played_timestamp()
        latest_unix_timestamp = int(latest_played_at_timestamp.timestamp() * 1000)
        song_data = sp.current_user_recently_played(
            limit=50, before=latest_unix_timestamp
        )
        if not song_data:
            print("No new music to fetch. Skipping transformation.")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        else:
            recent_songs = []
            artists = {}
            track_ids = []
            for item in song_data["items"]:
                recent_song_info = {
                    "played_at": item["played_at"],
                    "track_id": item["track"]["id"],
                    "track_name": item["track"]["name"],
                    "artist_id": item["track"]["artists"][0]["id"],
                    "artist_name": item["track"]["artists"][0]["name"],
                    "album_name": item["track"]["album"]["name"],
                }
                recent_songs.append(recent_song_info)
                track_ids.append(recent_song_info["track_id"])
                if recent_song_info["artist_id"] not in artists:
                    artist_info = sp.artist(recent_song_info["artist_id"])
                    artists[recent_song_info["artist_id"]] = {
                        "artist_id": artist_info["id"],
                        "artist_name": artist_info["name"],
                        "genres": artist_info["genres"],
                    }
            recent_songs_df = pd.DataFrame(recent_songs)
            artists_df = pd.DataFrame.from_dict(artists, orient="index")

            audio_features = sp.audio_features(track_ids)
            audio_features_df = pd.DataFrame(audio_features)

            audio_features_df = audio_features_df[
                [
                    "id",
                    "danceability",
                    "energy",
                    "key",
                    "loudness",
                    "mode",
                    "speechiness",
                    "acousticness",
                    "instrumentalness",
                    "liveness",
                    "valence",
                    "tempo",
                    "duration_ms",
                    "time_signature",
                ]
            ]
            return recent_songs_df, artists_df, audio_features_df

    def load_data(self):
        recent_songs_df, artists_df, audio_features_df = self.fetch_data()

        if recent_songs_df.empty:
            print("Nothing to load into db. Skipping.")
        else:
            dfs = [
                (recent_songs_df, "recently_played_raw"),
                (artists_df, "artists_raw"),
                (audio_features_df, "audio_features_raw"),
            ]

            queries = {
                "recently_played_raw": INSERT_RECENTLY_PLAYED_RAW,
                "artists_raw": INSERT_ARTISTS_RAW,
                "audio_features_raw": INSERT_AUDIO_FEATURES_RAW,
            }

            with PgConnect() as conn:
                if conn:
                    cursor = conn.cursor()
                    for df, table_name in dfs:
                        data_tuples = list(df.itertuples(index=False, name=None))

                        try:
                            cursor.executemany(queries[table_name], data_tuples)
                            conn.commit()

                        except Exception as e:
                            print(f"Error executing query for table: {table_name} {e}")
                            conn.rollback()
                    cursor.close()
