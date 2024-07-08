import duckdb
import auth
import pandas as pd
import datetime
import spotipy
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
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    recently_played = sp.current_user_recently_played(after=yesterday_unix_timestamp)

    album_list = []
    for row in recently_played["items"]:
        album_id = row["track"]["album"]["id"]
        album_name = row["track"]["album"]["name"]
        album_release_date = row["track"]["album"]["release_date"]
        album_total_tracks = row["track"]["album"]["total_tracks"]
        album_url = row["track"]["album"]["external_urls"]["spotify"]
        album_element = {
            "album_id": album_id,
            "name": album_name,
            "release_date": album_release_date,
            "total_tracks": album_total_tracks,
            "url": album_url,
        }
        album_list.append(album_element)

    artist_dict = {}
    id_list = []
    name_list = []
    url_list = []
    for item in recently_played["items"]:
        for key, value in item.items():
            if key == "track":
                for data_point in value["artists"]:
                    id_list.append(data_point["id"])
                    name_list.append(data_point["name"])
                    url_list.append(data_point["external_urls"]["spotify"])

    artist_dict = {"artist_id": id_list, "name": name_list, "url": url_list}

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

    album_df = pd.DataFrame.from_dict(album_list)
    album_df = album_df.drop_duplicates(subset=["album_id"])

    artist_df = pd.DataFrame.from_dict(artist_dict)
    artist_df = artist_df.drop_duplicates(subset=["artist_id"])

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

    with duckdb.connect("spotify_data.db") as con:
        con.execute(
            "CREATE OR REPLACE TABLE spotify_album_data_raw AS SELECT * FROM album_df"
        ).df()
        con.execute(
            "CREATE OR REPLACE TABLE spotify_artist_data_raw AS SELECT * FROM artist_df"
        ).df()
        con.execute(
            "CREATE OR REPLACE TABLE spotify_song_data_raw AS SELECT * FROM song_df"
        ).df()

        # Query and print the data to check
        albums = con.execute("SELECT * FROM spotify_album_data_raw").df()
        artists = con.execute("SELECT * FROM spotify_artist_data_raw").df()
        songs = con.execute("SELECT * FROM spotify_song_data_raw").df()

        print("Added in database successfully")

    print("Albums:")
    print(albums.head())  # Print first few rows of albums table

    print("\nArtists:")
    print(artists.head())  # Print first few rows of artists table

    print("\nSongs:")
    print(songs.head())  # Print first few rows of songs table


fetch_spotify_data()
