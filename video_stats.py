import requests
import json
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path = "./.env")

Api_key = os.getenv("Api_key")
Channel_handle = "MrBeast"

def get_playlist_id():

    try:
       

        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={Channel_handle}&key={Api_key}"

        response = requests.get(url)

        response.raise_for_status()

        data = response.json()

        channel_items = data['items'][0]

        channel_playlistId = channel_items['contentDetails']['relatedPlaylists']['uploads']

        #print(json.dumps(data,indent=4))
        #print(channel_playlistId)
        return channel_playlistId

    except requests.exceptions.RequestException as e:
        raise e
    
if __name__ == '__main__':
    get_playlist_id()