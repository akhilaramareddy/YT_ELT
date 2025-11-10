import requests
import json
import os
from dotenv import load_dotenv
from datetime import date
#import sys
#sys.stdout.reconfigure(encoding='utf-8')
#load_dotenv(dotenv_path="./.env")
from airflow.decorators import task
from airflow.models import Variable
    

API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
maxResults = 50


@task
def get_playlist_id():
    try:
        url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        #print(json.dumps(data, indent=4))
        channel_items = data["items"][0]
        channel_playlistId= channel_items["contentDetails"]["relatedPlaylists"]["uploads"]
        print(channel_playlistId)
        return channel_playlistId
    except requests.exceptions.RequestException as e:
        raise e
    
    

@task
def get_vedio_ids(playlistId):
    vedio_ids = []
    pageToken = None
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResults}&playlistId={playlistId}&key={API_KEY}" 
    try:
        while True:
            URL = base_url
            if pageToken:
                URL += f"&pageToken={pageToken}"
            # >>> CHANGED: always call the API every loop (not only when pageToken exists)
            response = requests.get(URL)  # <<< CHANGED
            response.raise_for_status()   # <<< CHANGED (helps surface HTTP errors)
            data = response.json()        # <<< CHANGED (moved out so it always runs)

            for item in data.get('items', []):  # <<< CHANGED (unindented so it always runs)
                vedio_id = item["contentDetails"]["videoId"]
                vedio_ids.append(vedio_id)

            pageToken = data.get("nextPageToken")
            if not pageToken:
                break
        return vedio_ids

    except requests.exceptions.RequestException as e:
        raise e

@task
def extract_vedio_data(vedio_ids):
    extracted_data= []
    def batch_list(vedio_id_lst, batch_size=50):
        for vedio_id in range(0, len(vedio_id_lst), batch_size):
            yield vedio_id_lst[vedio_id: vedio_id + batch_size] 
    
    'https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id=0e3GPea1Tyg&key=[YOUR_API_KEY]' 
    try:
        for batch in batch_list(vedio_ids, maxResults):
            vedio_ids_str = ",".join(batch)
            url = f'https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={vedio_ids_str}&key={API_KEY}'
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            for item in data.get('items', []):
                vedio_id= item["id"]
                snippet = item["snippet"]
                content_details = item["contentDetails"]
                statistics = item["statistics"]
                vedio_data = {
                    "vedio_id": vedio_id,
                    "title": snippet.get("title"),
                    "publishedAt": snippet.get("publishedAt"),
                    "duration": content_details.get("duration"),
                    "viewCount": statistics.get("viewCount", None),
                    "likeCount": statistics.get("likeCount", None),
                    "commentCount": statistics.get("commentCount", None),
                }
                extracted_data.append(vedio_data)
        return extracted_data
            
    except requests.exceptions.RequestException as e:
        raise e 
    
@task    
def save_to_json(extracted_data):
    os.makedirs("data", exist_ok=True)
    file_path = f"data/YT_data_{date.today()}.json"
    with open(file_path, "w", encoding="utf-8") as json_outfile:
        json.dump(extracted_data, json_outfile, indent=4, ensure_ascii=False)
    
#if __name__ == "__main__":
    #playlistId = get_playlist_id()
     #>>> CHANGED: actually fetch and show the video ids
    #video_ids = get_vedio_ids(playlistId)    
    #vedio_data = extract_vedio_data(video_ids)# <<< CHANGED
    #save_to_json(vedio_data)

