import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import json
from google.cloud import storage
import os


os.environ["GOOGLE_APPLICATION_CREDIENTIALS"] = '/root/.gcp/gcs-spotify-analysis-key.json'
f = open('spotify-token.json', 'r')
JSON_DATA = json.load(f)
REFLESH_TOKEN = JSON_DATA["reflesh_token"]


class SpotifyToken(object):
    def __init__(self, proxies=None, state=None):
        self.client_id = JSON_DATA["client_id"]
        self.client_secret = JSON_DATA["client_secret"]
        self.redirect_uri = JSON_DATA["client_secret"]
        self.project_name = 'atomic-lens-188216'
        self.bucket_name = 'spotify-analysis'


def get_new_access_token(oauth):
    refresh_token = REFLESH_TOKEN
    new_token = oauth.refresh_access_token(refresh_token)
    access_token = new_token['access_token']
    return access_token


def get_top200csv(project_name, bucket_name):
    source_blob_name = "top200_nonduplicate.csv"
    destination_file_name = "top200_nonduplicate.csv"
    storage_client = storage.Client(project_name)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    return destination_file_name


def load_csv(csv_file_pass):
    df = pd.read_csv(csv_file_pass)
    df_artists = df.loc[:, ['Artist']]
    artists = df_artists.values.tolist()
    return artists


def get_audio_features(sp, search_word):
    #try:
    # search by artist name
    name = search_word    
    result = sp.search(q='artist:' + name, type='artist')

    # get the artist_id
    artist_id = result['artists']['items'][0]['id']

    # get the latest album of the artist
    albums = sp.artist_albums(artist_id)

     # get the ids of their albumns
    albums_ids = []
    albums_count = 0
    for items in albums['items']:
        albums_ids.append(items['id'])
        albums_count += 1

    # get json of information about all songs
    songs_list = []
    for ids in albums_ids:
        album_tracks = sp.album_tracks(ids)
        songs_list.append(album_tracks)

    # get ids and names of all songs
    songs_ids = []
    songs_names = []
    for i in range(albums_count):
        for j in range(len(songs_list[i]['items'])):
            songs_ids.append(songs_list[i]['items'][j]['id'])
            songs_names.append(songs_list[i]['items'][j]['name'])

    # get Audio Features of their songs
    songs_ids = list(split_list(songs_ids, 20))
    audio_features_json = []
    for songs20 in songs_ids:
        audio_features_20 = sp.audio_features(tracks=songs20)
        for audio_feature in audio_features_20:
            audio_features_json.append(audio_feature)

    # make dataframe
    df_features = pd.DataFrame(data=audio_features_json)
    df = pd.concat([df_features, pd.Series(songs_names).rename('name')], axis=1)
    df = df.assign(Artist=search_word)
    print("get audio features of the songs of {}".format(search_word))
    return df

    #except:
    #    print("Could not find the artist.")
    #    return None


def split_list(songs_ids, n):
    for idx in range(0, len(songs_ids), n):
        yield songs_ids[idx:idx + n]


def upload_csv(filepath):
    client = storage.Client("atomic-lens-188216")
    bucket = client.get_bucket('spotify-analysis')
    blob = bucket.blob('top200_audiofeatures.csv')
    blob.upload_from_filename(filename=filepath)


def get_new_access_token(oauth):
    refresh_token = REFLESH_TOKEN
    new_token = oauth.refresh_access_token(refresh_token)
    access_token = new_token['access_token']
    return access_token


if __name__ == '__main__':
    user = SpotifyToken()
    oauth = SpotifyOAuth(client_id=user.client_id, client_secret=user.client_secret,
    redirect_uri=None)
    access_token = get_new_access_token(oauth)
    sp = spotipy.Spotify(auth=access_token)
    sp.trace = False
    csv_file_path = get_top200csv(user.project_name, user.bucket_name)
    artists = load_csv(csv_file_path)
    df_features_top200 = pd.DataFrame()
    for artist in artists:
        print(artist[0])
        df_audiofeature_artist = get_audio_features(sp, search_word=artist[0])
        df_features_top200 = pd.concat([df_features_top200, df_audiofeature_artist])
    top200_audiofeatures_csv = 'df-audio-features/top200_audiofeatures.csv'
    df_features_top200.to_csv(top200_audiofeatures_csv)
    upload_csv(top200_audiofeatures_csv)

