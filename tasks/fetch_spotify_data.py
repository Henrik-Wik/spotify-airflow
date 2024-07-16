import spotipy
import pandas as pd
import logging
from airflow.exceptions import AirflowSkipException, AirflowFailException
from spotipy.oauth2 import SpotifyOAuth
from config.pg_connect import PgConnect
from datetime import datetime, timedelta, timezone
from typing import Tuple, List, Dict
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
        self.logger = logging.getLogger("FetchSpotifyData")
        self.logger.setLevel(logging.INFO)
        self.current_time = datetime.now(timezone.utc)
        self.sp = self.init_spotify_auth()

    def init_spotify_auth(self) -> spotipy.Spotify:
        auth_manager = SpotifyOAuth(
            client_id=self.client_id,
            client_secret=self.client_secret,
            redirect_uri=self.redirect_uri,
            scope="user-read-recently-played",
            cache_path="/opt/airflow/config/.cache",
        )

        return spotipy.Spotify(auth_manager=auth_manager)

    def get_latest_played_timestamp(self) -> datetime:
        """Get latest played timestamp from database to limit the fetched data."""
        timestamp: datetime = None
        with PgConnect() as conn:
            if conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(
                        f"SELECT MAX(played_at) FROM public.recently_played_raw"
                    )
                    timestamp = cursor.fetchone()[0]
                except Exception as e:
                    raise AirflowFailException(f"Error executing query: {e}")
        if not timestamp:
            timestamp = self.current_time - timedelta(days=30)
        return timestamp

    def fetch_song_data(self) -> Tuple[List[Dict], List[str], set]:
        """Fetch and process recently played songs data."""
        latest_played_at_timestamp = self.get_latest_played_timestamp()
        latest_unix_timestamp = int(latest_played_at_timestamp.timestamp() * 1000)
        song_data = self.sp.current_user_recently_played(
            limit=50, after=latest_unix_timestamp
        )
        recent_songs = []
        artist_ids = set()
        track_ids = []

        for item in song_data["items"]:
            recent_song_info = {
                "played_at": item["played_at"],
                "track_id": item["track"]["id"],
                "track_name": item["track"]["name"],
                "artist_id": item["track"]["artists"][0]["id"],
                "artist_name": item["track"]["artists"][0]["name"],
                "album_name": item["track"]["album"]["name"],
                "transformed": False,
                "updated_at": self.current_time,
            }
            recent_songs.append(recent_song_info)
            track_ids.append(recent_song_info["track_id"])
            artist_ids.add(recent_song_info["artist_id"])

        return recent_songs, track_ids, artist_ids

    def fetch_artist_data(self, artist_ids: set) -> pd.DataFrame:
        """Fetch and process artists data."""
        artists = {}
        artist_ids_list = list(artist_ids)
        for i in range(0, len(artist_ids_list), 50):
            artists_data = self.sp.artists(artist_ids_list[i : i + 50])
            for artist in artists_data["artists"]:
                artists[artist["id"]] = {
                    "artist_id": artist["id"],
                    "artist_name": artist["name"],
                    "genres": artist["genres"],
                    "updated_at": self.current_time,
                }

        return pd.DataFrame.from_dict(artists, orient="index")

    def fetch_audio_features(self, track_ids: List[str]) -> pd.DataFrame:
        """Fetch and process audio features data."""
        audio_features_list = []
        for i in range(0, len(track_ids), 100):
            audio_features = self.sp.audio_features(track_ids[i : i + 50])
            audio_features_list.extend(audio_features)
        audio_features_df = pd.DataFrame(audio_features)
        audio_features_df["updated_at"] = self.current_time

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
                "updated_at",
            ]
        ]
        return audio_features_df

    def load_data(self):
        """Load data into postgresql database."""
        recent_songs, track_ids, artist_ids = self.fetch_song_data()
        if recent_songs:
            self.logger.info(f"Fetched {len(recent_songs)} songs.")

            recent_songs_df = pd.DataFrame(recent_songs)
            artists_df = self.fetch_artist_data(artist_ids)
            audio_features_df = self.fetch_audio_features(track_ids)

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
                            conn.rollback()
                            raise AirflowFailException(
                                f"Error executing query for table: {table_name} {e}"
                            )
                    cursor.close()
        else:  # skip task if no new music can be fetched
            raise AirflowSkipException("No new music to fetch. Skipping.")
