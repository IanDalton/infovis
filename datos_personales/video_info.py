import aiohttp
import asyncio
import os
from dotenv import load_dotenv
import pandas as pd
import csv
from datetime import datetime 



async def fetch(session, url,index):
    async with session.get(url) as response:
        response = await response.json()
        print(index,"Done!") if index is not None else None
        return response
    
    

async def _get_video_data(database:pd.DataFrame,debug=False):
    load_dotenv()
    API_KEY = os.getenv('API_KEY')

    video_ids = database['ID_Video'].tolist()
    video_ids = list(set(video_ids))
    videos = []
    for i in range(0,len(video_ids),50):
        videos.append(video_ids[i:i+50])
    
    for i,video in enumerate(videos):
        videos[i] = ','.join(video)
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for i,video in enumerate(videos):
            url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,contentDetails,contentDetails,statistics&id={video}&key={API_KEY}"
            i = i if debug else None
            tasks.append(fetch(session, url,i))
        responses = await asyncio.gather(*tasks)
        return responses

def get_video_data(database:pd.DataFrame,tag_amount=20,debug=False):
    
    responses = asyncio.run(_get_video_data(database,debug))
    data = []
    for response in responses:
        data.extend(response['items'])
    clean_data = clean_video_data(data,tag_amount)

    #Join left on ID_Video == video_id
    data = database.merge(clean_data,how="outer",left_on="ID_Video",right_on="video_id")
    #Drop video_id
    data = data.drop(columns=["video_id"])
    return data

def clean_video_data(data:list,tag_amount):
    clean_data = []
    for video in data:
        video_data = dict()
        video_data["video_id"] = video.get("id")
        stats = video.get("statistics")
        try:
            duration = video.get("contentDetails").get("duration")
            duration = duration.split("T")[1]
            #Split duration in hours, minutes and seconds

            hours,duration = duration.split("H") if "H" in duration else ["0",duration]

            minutes,duration = duration.split("M") if "M" in duration else ["0",duration]

            seconds, duration = duration.split("S") if "S" in duration else ["0",duration]
            del duration
            video_data["duration"] = str(datetime(year=1984,month=1,day=1,hour=int(hours),minute=int(minutes),second=int(seconds)))
        except:
            video_data["duration"] = None    

        video = video.get("snippet")
        
        video_data["description"] = video.get("description")
        video_data["publishedAt"] = video.get("publishedAt")
        video_data["thumbnail"]= video.get("thumbnails").get("high").get("url")
        video_data["tags"] = video.get("tags")
        video_data["categoryId"] = video.get("categoryId")
        video_data["language"] = video.get("defaultLanguage")

        video_data["viewCount"] = stats.get("viewCount")
        video_data["likeCount"] = stats.get("likeCount")
        video_data["commentCount"] = stats.get("commentCount")

        clean_data.append(video_data)
    clean_data = pd.DataFrame(clean_data)

    # select the top n most popular tags and remove the rest
    tags = clean_data["tags"].explode().value_counts().head(tag_amount).index

    def tag_cleaner(tag_list):
        if tag_list is None:
            return []
        return [tag for tag in tag_list if tag in tags]

    clean_data['tags'] = clean_data['tags'].apply(tag_cleaner)


    clean_data['tags'] = clean_data['tags'].apply(lambda x: ','.join(x))

    # Use str.get_dummies to create dummy variables for each tag
    tag_dummies = clean_data['tags'].str.get_dummies(sep=',')

    # Concatenate the original DataFrame with the dummy DataFrame
    clean_data = pd.concat([clean_data, tag_dummies], axis=1)
    clean_data = clean_data.drop(columns=["tags"])

    return clean_data

if __name__ == "__main__":
    with open('datos_personales\watch-history.csv', 'r',encoding="utf-8") as f:
        reader = csv.reader(f)
        data = list(reader)
    data = pd.DataFrame(data[1:],columns=data[0])
    
    data = get_video_data(data)
    
