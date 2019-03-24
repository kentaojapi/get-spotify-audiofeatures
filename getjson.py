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


def get_audio_features(sp, search_word):
    '''
    Get Audio Features.
    '''
    try:
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
        audio_features_json = []
        for song_id in songs_ids:
            audio_feature = sp.audio_features(tracks=song_id)
            audio_features_json.append(audio_feature[0])

        # make pandas.DataFrame from json
        df_features = pd.DataFrame(data=audio_features_json)
        df = pd.concat([df_features, pd.Series(songs_names).rename('name')], axis=1)
        df.to_csv('./df-audio-features/audio_features_{}.csv'.format(search_word), index=False)

        # post to GCS.
        client = storage.Client("atomic-lens-188216")
        bucket = client.get_bucket('spotify-analysis')
        blob = bucket.blob('audio-features_{}.csv'.format(search_word))
        blob.upload_from_filename(filename='df-audio-features/audio_features_{}.csv'.format(search_word))

    except:
        print("Could not find the artist.")


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
    search_word = input('artist name: ')
    get_audio_features(sp, search_word)
