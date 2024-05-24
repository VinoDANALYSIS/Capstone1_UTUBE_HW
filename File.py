from googleapiclient.discovery import build
api_servicename='youtube'
api_version='v3'
API_Key='key'
CHANNELID='UC4vgd34lbz4rMekxNfmxOIg'
import json
import re
from datetime import datetime

#API
youtube=build('youtube','v3',developerKey=API_Key)
def channel_data(channel_id):
    request=youtube.channels().list(part='snippet,contentDetails,statistics',id=channel_id)
    response=request.execute()
    for i in response['items']:
        Info=dict(channel_name=response['items'][0]['snippet']['title'],
        Description=response['items'][0]['snippet']['description'],
        Channel_ID=response['items'][0]['id'],
        Subscribers=response['items'][0]['statistics']['subscriberCount'],
        Views=response['items'][0]['statistics']['viewCount'],
        Videos_Count=response['items'][0]['statistics']['videoCount'],
        Playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
    return Info

#VIDEO IDS
def video_ids_data(channel_id):
    request=youtube.channels().list(part='contentDetails',id=channel_id).execute()
    playlist_id=request['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None
    video_ids=[]
    #loop
    while True:
        response1=youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

def format_published_date(published_date, date_format='%Y-%m-%d'):
    dt = datetime.strptime(published_date, '%Y-%m-%dT%H:%M:%SZ')
    return dt.strftime(date_format)

def parse_duration(duration):
    match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration)
    if not match:
        return "00:00"
    
    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0
    
    total_minutes = hours * 60 + minutes
    return f"{total_minutes:02}:{seconds:02}"

#VIDEO DETAILS
def video_data(video_ids):
    video_details=[]
    for video_info in video_ids:
        request=youtube.videos().list(part='snippet,contentDetails,statistics',id=video_info)
        response=request.execute()

        for item in response['items']:
            Info=dict(Channel_Name=item['snippet']['channelTitle'],
                      Channel_Id=item['snippet']['channelId'],
                          Video_id=item['id'],
                          Title=item['snippet']['title'],
                          Tags=json.dumps(item['snippet'].get('tags')),
                          Thumbnails=item['snippet']['thumbnails']['default']['url'],
                          Description=item['snippet'].get('description'),
                          Published_Date=format_published_date(item['snippet']['publishedAt']),
                          Duration=parse_duration(item['contentDetails']['duration']),
                          ViewCount=item['statistics'].get('viewcount'),
                          Likes=item['statistics'].get('likecount'),
                          Comments=item['statistics'].get('commentCount'),
                          Favorite_count=item['statistics']['favoriteCount'],
                          Definition=item['contentDetails']['definition'],
                          Caption=item['contentDetails']['caption'])
            video_details.append(Info)
    return video_details

def format_published_date_comment(published_date, date_format='%Y-%m-%d'):
    dt = datetime.strptime(published_date, '%Y-%m-%dT%H:%M:%SZ')
    return dt.strftime(date_format)


#comment details
def comment_data(video_ids):
    Comment_Info=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(part='snippet',videoId=video_id,maxResults=50)
            response=request.execute()

            for item in response['items']:
                Info=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                         Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                         Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                         Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                         Comment_Published=format_published_date_comment(item['snippet']['topLevelComment']['snippet']['publishedAt']))

                Comment_Info.append(Info)
    except:
        pass
    return Comment_Info



#Playlist details
def playlist_data(channel_id):
    next_page_token=None
    Playlist_Details=[]

    while True:
        request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,pageToken=next_page_token)
        response=request.execute()

        for item in response['items']:
                Info=dict(Playlist_Id=item['id'],
                         Title=item['snippet']['title'],
                         Channel_Id=item['snippet']['channelId'],
                         Channel_Name=item['snippet']['channelTitle'],
                         Published_Date=format_published_date(item['snippet']['publishedAt']),
                         Video_Count=item['contentDetails']['itemCount'])

                Playlist_Details.append(Info)
        next_page_token=response.get('nextPageToken')

        if next_page_token is None:
            break
            
    return Playlist_Details

#1st channel#THE BOOK SHOW
CHANNEL1_DETAILS=channel_data('UC4vgd34lbz4rMekxNfmxOIg')
VIDEO1_IDS_DETAILS=video_ids_data('UC4vgd34lbz4rMekxNfmxOIg')
VIDEO1_DETAILS=video_data(VIDEO1_IDS_DETAILS)
COMMENT1_DETAILS=comment_data(VIDEO1_IDS_DETAILS)
PLAYLIST1_DETAILS=playlist_data('UC4vgd34lbz4rMekxNfmxOIg')

C1=tuple(CHANNEL1_DETAILS.values())
#sql

import mysql.connector as db
import pandas as pd
#connection
my_db=db.connect(host="localhost",
                user="user1",
                password="*****",
                database="youtube")
#cursor
curr=my_db.cursor()
#channel table
create_query='''CREATE TABLE youtube.Channel_det(channel_name varchar(80),
                Description text,
                Channel_ID varchar(100) primary key,
                Subscribers int,
                Views int,
                Videos_Count int,
                Playlist_id varchar(100))'''
    
curr.execute(create_query)
#values
insert_query=insert_query='''insert into youtube.Channel_det(channel_name,Description,Channel_ID,Subscribers,Views,Videos_Count,Playlist_id )
                    values(%s,%s,%s,%s,%s,%s,%s)'''
values=(tuple(CHANNEL1_DETAILS.values()))
curr.execute(insert_query,values)
my_db.commit()

#PLAYLIST1_DETAILS
pl1=[]
for i in PLAYLIST1_DETAILS:
    j=tuple(i.values())
    pl1.append(j)
#Playlist
import mysql.connector as db
import pandas as pd
#connection
my_db=db.connect(host="localhost",
                user="user1",
                password="****",
                database="youtube")
#cursor
curr=my_db.cursor()
#playlist_table
create_query='''CREATE TABLE PLAYLIST_det(Playlist_Id varchar(100) primary key,
                    Title varchar(100),
                    Channel_Id varchar(100),
                    Channel_Name varchar(100),
                    Published_Date timestamp,
                    Video_Count int)'''

curr.execute(create_query)
my_db.commit()

insert_query='''insert into PLAYLIST_det(Playlist_Id,Title,Channel_Id,Channel_Name,Published_Date,Video_Count )
                    values(%s,%s,%s,%s,%s,%s)'''

values=(pl1)


curr.executemany(insert_query,values)
my_db.commit()

#VIDEO1_DETAILS
DATA_1=[]
for i in VIDEO1_DETAILS:
    j=tuple(i.values())
    DATA_1.append(j)
#sql-video table
import mysql.connector as db
import pandas as pd
#connection
my_db=db.connect(host="localhost",
                user="user1",
                password="****",
                database="youtube")
#cursor
curr=my_db.cursor()

create_query='''CREATE TABLE videos_de(Channel_Name varchar(100),
                Channel_Id varchar(100),Video_id varchar(50) primary key,
                Title varchar(150),Tags TEXT,Thumbnails varchar(200),
                Description text,Published_Date Date,Duration time,
                ViewCount int,Likes int,Comments bigint,Favorite_count bigint,                    
                Definition varchar(10),Caption varchar(10))'''


curr.execute(create_query)

insert_query='''insert into videos_de(Channel_Name,Channel_Id,Video_id,Title,Tags,Thumbnails,Description,
                    Published_Date,Duration,ViewCount,Likes,Comments,Favorite_count,Definition,Caption)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
values=(DATA_1)

curr.executemany(insert_query,values)
my_db.commit()
#COMMENT1_DETAILS
comment_1=[]
for i in COMMENT1_DETAILS:
    j=tuple(i.values())
    comment_1.append(j)
#sql comment table
create_query='''CREATE TABLE Comments_det(Comment_Id varchar(100) primary key,
                Video_Id varchar(80),
                Comment_Text text,
                Comment_Author varchar(100),
                Comment_Published DATE)'''

curr.execute(create_query)
my_db.commit()

insert_query='''insert into Comments_det(Comment_Id,Video_Id,Comment_Text,Comment_Author,Comment_Published )
                values(%s,%s,%s,%s,%s)'''

values=(comment_1)


curr.executemany(insert_query,values)
my_db.commit()

#2ND channel-Oh! My Thagaval
CHANNEL2_DETAILS=channel_data('UCOeIvpX4x09EsWS-6FwWonQ')
VIDEO2_IDS_DETAILS=video_ids_data('UCOeIvpX4x09EsWS-6FwWonQ')
VIDEO2_DETAILS=video_data(VIDEO2_IDS_DETAILS)
COMMENT2_DETAILS=comment_data(VIDEO2_IDS_DETAILS)
PLAYLIST2_DETAILS=playlist_data('UCOeIvpX4x09EsWS-6FwWonQ')

#CHANNEL 
C2=tuple(CHANNEL2_DETAILS.values())
#values
insert_query=insert_query='''insert into youtube.Channel_det(channel_name,Description,Channel_ID,Subscribers,Views,Videos_Count,Playlist_id )
                    values(%s,%s,%s,%s,%s,%s,%s)'''
values=(C2)
curr.execute(insert_query,values)
my_db.commit()

#PLAYLIST
pl2=[]
for i in PLAYLIST2_DETAILS:
    j=tuple(i.values())
    pl2.append(j)
    
insert_query='''insert into PLAYLIST_det(Playlist_Id,Title,Channel_Id,Channel_Name,Published_Date,Video_Count )
                    values(%s,%s,%s,%s,%s,%s)'''

values=(pl2)


curr.executemany(insert_query,values)
my_db.commit()

#VIDEO

D2=[]
for i in VIDEO2_DETAILS:
    j=tuple(i.values())
    D2.append(j)
    
insert_query='''insert into videos_de(Channel_Name,Channel_Id,Video_id,Title,Tags,Thumbnails,Description,
                    Published_Date,Duration,ViewCount,Likes,Comments,Favorite_count,Definition,Caption)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
values=(D2)

curr.executemany(insert_query,values)
my_db.commit()

#COMMENT
comment_2=[]
for i in COMMENT2_DETAILS:
    j=tuple(i.values())
    comment_2.append(j)
    
insert_query='''insert into Comments_det(Comment_Id,Video_Id,Comment_Text,Comment_Author,Comment_Published )
                values(%s,%s,%s,%s,%s)'''

values=(comment_2)


curr.executemany(insert_query,values)
my_db.commit()

#3rd Channel-Marko
CHANNEL3_DETAILS=channel_data('UCCQ6SXMc7MoJ88jjpn6j-8Q')
VIDEO3_IDS_DETAILS=video_ids_data('UCCQ6SXMc7MoJ88jjpn6j-8Q')
VIDEO3_DETAILS=video_data(VIDEO3_IDS_DETAILS)
COMMENT3_DETAILS=comment_data(VIDEO3_IDS_DETAILS)
PLAYLIST3_DETAILS=playlist_data('UCCQ6SXMc7MoJ88jjpn6j-8Q')

#CHANNEL 
C3=tuple(CHANNEL3_DETAILS.values())
#values
insert_query=insert_query='''insert into youtube.Channel_det(channel_name,Description,Channel_ID,Subscribers,Views,Videos_Count,Playlist_id )
                    values(%s,%s,%s,%s,%s,%s,%s)'''
values=(C3)
curr.execute(insert_query,values)
my_db.commit()

#PLAYLIST
pl3=[]
for i in PLAYLIST3_DETAILS:
    j=tuple(i.values())
    pl3.append(j)
    
insert_query='''insert into PLAYLIST_det(Playlist_Id,Title,Channel_Id,Channel_Name,Published_Date,Video_Count )
                    values(%s,%s,%s,%s,%s,%s)'''

values=(pl3)


curr.executemany(insert_query,values)
my_db.commit()

#VIDEO

D3=[]
for i in VIDEO3_DETAILS:
    j=tuple(i.values())
    D3.append(j)
    
insert_query='''insert into videos_de(Channel_Name,Channel_Id,Video_id,Title,Tags,Thumbnails,Description,
                    Published_Date,Duration,ViewCount,Likes,Comments,Favorite_count,Definition,Caption)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
values=(D3)

curr.executemany(insert_query,values)
my_db.commit()

#COMMENT
comment_3=[]
for i in COMMENT3_DETAILS:
    j=tuple(i.values())
    comment_3.append(j)
    
insert_query='''insert into Comments_det(Comment_Id,Video_Id,Comment_Text,Comment_Author,Comment_Published )
                values(%s,%s,%s,%s,%s)'''

values=(comment_3)


curr.executemany(insert_query,values)
my_db.commit()

#4th Channel-Kenny Gunderman
CHANNEL4_DETAILS=channel_data('UCkCJ0zLrSg7VudR97g-FNVQ')
VIDEO4_IDS_DETAILS=video_ids_data('UCkCJ0zLrSg7VudR97g-FNVQ')
VIDEO4_DETAILS=video_data(VIDEO4_IDS_DETAILS)
COMMENT4_DETAILS=comment_data(VIDEO4_IDS_DETAILS)
PLAYLIST4_DETAILS=playlist_data('UCkCJ0zLrSg7VudR97g-FNVQ')

#CHANNEL 
C4=tuple(CHANNEL4_DETAILS.values())
#values
insert_query=insert_query='''insert into youtube.Channel_det(channel_name,Description,Channel_ID,Subscribers,Views,Videos_Count,Playlist_id )
                    values(%s,%s,%s,%s,%s,%s,%s)'''
values=(C4)
curr.execute(insert_query,values)
my_db.commit()

#PLAYLIST
pl4=[]
for i in PLAYLIST4_DETAILS:
    j=tuple(i.values())
    pl4.append(j)
    
insert_query='''insert into PLAYLIST_det(Playlist_Id,Title,Channel_Id,Channel_Name,Published_Date,Video_Count )
                    values(%s,%s,%s,%s,%s,%s)'''

values=(pl4)


curr.executemany(insert_query,values)
my_db.commit()

#VIDEO

D4=[]
for i in VIDEO4_DETAILS:
    j=tuple(i.values())
    D4.append(j)
    
insert_query='''insert into videos_de(Channel_Name,Channel_Id,Video_id,Title,Tags,Thumbnails,Description,
                    Published_Date,Duration,ViewCount,Likes,Comments,Favorite_count,Definition,Caption)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
values=(D4)

curr.executemany(insert_query,values)
my_db.commit()

#COMMENT
comment_4=[]
for i in COMMENT4_DETAILS:
    j=tuple(i.values())
    comment_4.append(j)
    
insert_query='''insert into Comments_det(Comment_Id,Video_Id,Comment_Text,Comment_Author,Comment_Published )
                values(%s,%s,%s,%s,%s)'''

values=(comment_4)


curr.executemany(insert_query,values)
my_db.commit()

#5th Channel-Nischa
CHANNEL5_DETAILS=channel_data('UCQpPo9BNwezg54N9hMFQp6Q')
VIDEO5_IDS_DETAILS=video_ids_data('UCQpPo9BNwezg54N9hMFQp6Q')
VIDEO5_DETAILS=video_data(VIDEO5_IDS_DETAILS)
COMMENT5_DETAILS=comment_data(VIDEO5_IDS_DETAILS)
PLAYLIST5_DETAILS=playlist_data('UCQpPo9BNwezg54N9hMFQp6Q')

#CHANNEL 
C5=tuple(CHANNEL5_DETAILS.values())
#values
insert_query=insert_query='''insert into youtube.Channel_det(channel_name,Description,Channel_ID,Subscribers,Views,Videos_Count,Playlist_id )
                    values(%s,%s,%s,%s,%s,%s,%s)'''
values=(C5)
curr.execute(insert_query,values)
my_db.commit()

#PLAYLIST
pl5=[]
for i in PLAYLIST5_DETAILS:
    j=tuple(i.values())
    pl5.append(j)
    
insert_query='''insert into PLAYLIST_det(Playlist_Id,Title,Channel_Id,Channel_Name,Published_Date,Video_Count )
                    values(%s,%s,%s,%s,%s,%s)'''

values=(pl5)


curr.executemany(insert_query,values)
my_db.commit()
#VIDEO

D5=[]
for i in VIDEO5_DETAILS:
    j=tuple(i.values())
    D5.append(j)
    
insert_query='''insert into videos_de(Channel_Name,Channel_Id,Video_id,Title,Tags,Thumbnails,Description,
                    Published_Date,Duration,ViewCount,Likes,Comments,Favorite_count,Definition,Caption)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
values=(D5)

curr.executemany(insert_query,values)
my_db.commit()

#COMMENT
comment_5=[]
for i in COMMENT5_DETAILS:
    j=tuple(i.values())
    comment_5.append(j)
    
insert_query='''insert into Comments_det(Comment_Id,Video_Id,Comment_Text,Comment_Author,Comment_Published )
                values(%s,%s,%s,%s,%s)'''

values=(comment_5)
curr.executemany(insert_query,values)
my_db.commit()

#6th Channel-Shrutika Arjun
CHANNEL6_DETAILS=channel_data('UC70iJbY5k-asxC4wO37y2ag')
VIDEO6_IDS_DETAILS=video_ids_data('UC70iJbY5k-asxC4wO37y2ag')
VIDEO6_DETAILS=video_data(VIDEO6_IDS_DETAILS)
COMMENT6_DETAILS=comment_data(VIDEO6_IDS_DETAILS)
PLAYLIST6_DETAILS=playlist_data('UC70iJbY5k-asxC4wO37y2ag')

#CHANNEL 
C6=tuple(CHANNEL6_DETAILS.values())
#values
insert_query=insert_query='''insert into youtube.Channel_det(channel_name,Description,Channel_ID,Subscribers,Views,Videos_Count,Playlist_id )
                    values(%s,%s,%s,%s,%s,%s,%s)'''
values=(C6)
curr.execute(insert_query,values)
my_db.commit()

#PLAYLIST
pl6=[]
for i in PLAYLIST6_DETAILS:
    j=tuple(i.values())
    pl6.append(j)
    
insert_query='''insert into PLAYLIST_det(Playlist_Id,Title,Channel_Id,Channel_Name,Published_Date,Video_Count )
                    values(%s,%s,%s,%s,%s,%s)'''

values=(pl6)


curr.executemany(insert_query,values)
my_db.commit()

#VIDEO

D6=[]
for i in VIDEO6_DETAILS:
    j=tuple(i.values())
    D6.append(j)
    
insert_query='''insert into videos_de(Channel_Name,Channel_Id,Video_id,Title,Tags,Thumbnails,Description,
                    Published_Date,Duration,ViewCount,Likes,Comments,Favorite_count,Definition,Caption)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
values=(D6)

curr.executemany(insert_query,values)
my_db.commit()

#COMMENT
comment_6=[]
for i in COMMENT6_DETAILS:
    j=tuple(i.values())
    comment_6.append(j)
    
insert_query='''insert into Comments_det(Comment_Id,Video_Id,Comment_Text,Comment_Author,Comment_Published )
                values(%s,%s,%s,%s,%s)'''

values=(comment_6)


curr.executemany(insert_query,values)
my_db.commit()

#7th Channel-BLACK BOX
CHANNEL7_DETAILS=channel_data('UCGgG1_YB_h4gylBbtJmDeZQ')
VIDEO7_IDS_DETAILS=video_ids_data('UCGgG1_YB_h4gylBbtJmDeZQ')
VIDEO7_DETAILS=video_data(VIDEO7_IDS_DETAILS)
COMMENT7_DETAILS=comment_data(VIDEO7_IDS_DETAILS)
PLAYLIST7_DETAILS=playlist_data('UCGgG1_YB_h4gylBbtJmDeZQ')

#CHANNEL
C7=tuple(CHANNEL7_DETAILS.values())
#values
insert_query=insert_query='''insert into youtube.Channel_det(channel_name,Description,Channel_ID,Subscribers,Views,Videos_Count,Playlist_id )
                    values(%s,%s,%s,%s,%s,%s,%s)'''
values=(C7)
curr.execute(insert_query,values)
my_db.commit()

#PLAYLIST
pl7=[]
for i in PLAYLIST7_DETAILS:
    j=tuple(i.values())
    pl7.append(j)
    
insert_query='''insert into PLAYLIST_det(Playlist_Id,Title,Channel_Id,Channel_Name,Published_Date,Video_Count )
                    values(%s,%s,%s,%s,%s,%s)'''

values=(pl7)


curr.executemany(insert_query,values)
my_db.commit()

#VIDEO

D7=[]
for i in VIDEO7_DETAILS:
    j=tuple(i.values())
    D7.append(j)
    
insert_query='''insert into videos_de(Channel_Name,Channel_Id,Video_id,Title,Tags,Thumbnails,Description,
                    Published_Date,Duration,ViewCount,Likes,Comments,Favorite_count,Definition,Caption)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
values=(D7)

curr.executemany(insert_query,values)
my_db.commit()

#COMMENT
comment_7=[]
for i in COMMENT7_DETAILS:
    j=tuple(i.values())
    comment_7.append(j)
    
insert_query='''insert into Comments_det(Comment_Id,Video_Id,Comment_Text,Comment_Author,Comment_Published )
                values(%s,%s,%s,%s,%s)'''

values=(comment_7)


curr.executemany(insert_query,values)
my_db.commit()

#8th Channel-Samyuktha Shan
CHANNEL8_DETAILS=channel_data('UCRiz2-lYcLhGOp0p8nhq0KA')
VIDEO8_IDS_DETAILS=video_ids_data('UCRiz2-lYcLhGOp0p8nhq0KA')
VIDEO8_DETAILS=video_data(VIDEO8_IDS_DETAILS)
COMMENT8_DETAILS=comment_data(VIDEO8_IDS_DETAILS)
PLAYLIST8_DETAILS=playlist_data('UCRiz2-lYcLhGOp0p8nhq0KA')

#CHANNEL
C8=tuple(CHANNEL8_DETAILS.values())
#values
insert_query=insert_query='''insert into youtube.Channel_det(channel_name,Description,Channel_ID,Subscribers,Views,Videos_Count,Playlist_id )
                    values(%s,%s,%s,%s,%s,%s,%s)'''
values=(C8)
curr.execute(insert_query,values)
my_db.commit()

#PLAYLIST
pl8=[]
for i in PLAYLIST8_DETAILS:
    j=tuple(i.values())
    pl8.append(j)
    
insert_query='''insert into PLAYLIST_det(Playlist_Id,Title,Channel_Id,Channel_Name,Published_Date,Video_Count )
                    values(%s,%s,%s,%s,%s,%s)'''

values=(pl8)


curr.executemany(insert_query,values)
my_db.commit()

#VIDEO

D8=[]
for i in VIDEO8_DETAILS:
    j=tuple(i.values())
    D8.append(j)
    
insert_query='''insert into videos_de(Channel_Name,Channel_Id,Video_id,Title,Tags,Thumbnails,Description,
                    Published_Date,Duration,ViewCount,Likes,Comments,Favorite_count,Definition,Caption)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
values=(D8)

curr.executemany(insert_query,values)
my_db.commit()

#COMMENT
comment_8=[]
for i in COMMENT8_DETAILS:
    j=tuple(i.values())
    comment_8.append(j)
    
insert_query='''insert into Comments_det(Comment_Id,Video_Id,Comment_Text,Comment_Author,Comment_Published )
                values(%s,%s,%s,%s,%s)'''

values=(comment_8)


curr.executemany(insert_query,values)
my_db.commit()

#9th Channel-Priyanka Deshpande
CHANNEL9_DETAILS=channel_data('UCNrlHfYLo7s2eoeKEJiJi3Q')
VIDEO9_IDS_DETAILS=video_ids_data('UCNrlHfYLo7s2eoeKEJiJi3Q')
VIDEO9_DETAILS=video_data(VIDEO9_IDS_DETAILS)
COMMENT9_DETAILS=comment_data(VIDEO9_IDS_DETAILS)
PLAYLIST9_DETAILS=playlist_data('UCNrlHfYLo7s2eoeKEJiJi3Q')

#CHANNEL
C9=tuple(CHANNEL9_DETAILS.values())
#values
insert_query=insert_query='''insert into youtube.Channel_det(channel_name,Description,Channel_ID,Subscribers,Views,Videos_Count,Playlist_id )
                    values(%s,%s,%s,%s,%s,%s,%s)'''
values=(C9)
curr.execute(insert_query,values)
my_db.commit()

#PLAYLIST
pl9=[]
for i in PLAYLIST9_DETAILS:
    j=tuple(i.values())
    pl9.append(j)
    
insert_query='''insert into PLAYLIST_det(Playlist_Id,Title,Channel_Id,Channel_Name,Published_Date,Video_Count )
                    values(%s,%s,%s,%s,%s,%s)'''

values=(pl9)


curr.executemany(insert_query,values)
my_db.commit()

#VIDEO

D9=[]
for i in VIDEO9_DETAILS:
    j=tuple(i.values())
    D9.append(j)
    
insert_query='''insert into videos_de(Channel_Name,Channel_Id,Video_id,Title,Tags,Thumbnails,Description,
                    Published_Date,Duration,ViewCount,Likes,Comments,Favorite_count,Definition,Caption)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
values=(D9)

curr.executemany(insert_query,values)
my_db.commit()

#COMMENT
comment_9=[]
for i in COMMENT9_DETAILS:
    j=tuple(i.values())
    comment_9.append(j)
    
insert_query='''insert into Comments_det(Comment_Id,Video_Id,Comment_Text,Comment_Author,Comment_Published )
                values(%s,%s,%s,%s,%s)'''

values=(comment_9)


curr.executemany(insert_query,values)
my_db.commit()

#10th Channel-Vj Siddhu Vlogs
CHANNEL10_DETAILS=channel_data('UCJcCB-QYPIBcbKcBQOTwhiA')
VIDEO10_IDS_DETAILS=video_ids_data('UCJcCB-QYPIBcbKcBQOTwhiA')
VIDEO10_DETAILS=video_data(VIDEO10_IDS_DETAILS)
COMMENT10_DETAILS=comment_data(VIDEO10_IDS_DETAILS)
PLAYLIST10_DETAILS=playlist_data('UCJcCB-QYPIBcbKcBQOTwhiA')

#CHANNEL
C10=tuple(CHANNEL10_DETAILS.values())
#values
insert_query=insert_query='''insert into youtube.Channel_det(channel_name,Description,Channel_ID,Subscribers,Views,Videos_Count,Playlist_id )
                    values(%s,%s,%s,%s,%s,%s,%s)'''
values=(C10)
curr.execute(insert_query,values)
my_db.commit()

#PLAYLIST
pl10=[]
for i in PLAYLIST10_DETAILS:
    j=tuple(i.values())
    pl10.append(j)
    
insert_query='''insert into PLAYLIST_det(Playlist_Id,Title,Channel_Id,Channel_Name,Published_Date,Video_Count )
                    values(%s,%s,%s,%s,%s,%s)'''

values=(pl10)


curr.executemany(insert_query,values)
my_db.commit()

#VIDEO

D10=[]
for i in VIDEO10_DETAILS:
    j=tuple(i.values())
    D10.append(j)
    
insert_query='''insert into videos_de(Channel_Name,Channel_Id,Video_id,Title,Tags,Thumbnails,Description,
                    Published_Date,Duration,ViewCount,Likes,Comments,Favorite_count,Definition,Caption)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
values=(D10)

curr.executemany(insert_query,values)
my_db.commit()

#COMMENT
comment_10=[]
for i in COMMENT10_DETAILS:
    j=tuple(i.values())
    comment_10.append(j)
    
insert_query='''insert into Comments_det(Comment_Id,Video_Id,Comment_Text,Comment_Author,Comment_Published )
                values(%s,%s,%s,%s,%s)'''

values=(comment_10)


curr.executemany(insert_query,values)
my_db.commit()

#Streamlit
import streamlit as st
import pandas as pd
import json

# MySQL connection
def create_connection():
    return db.connect(
        host="localhost",
        user="user1",
        password="****",
        database="youtube"
    )

# Fetch channel data
def fetch_channel_data():
    conn = create_connection()
    query = "SELECT * FROM youtube.Channel_det"
    df1 = pd.read_sql(query, conn)
    conn.close()
    return df1

# Fetch playlist data
def fetch_playlist_data():
    conn = create_connection()
    query = "SELECT * FROM PLAYLIST_det"
    df2= pd.read_sql(query, conn)
    conn.close()
    return df2

# Fetch video data
def fetch_video_data():
    conn = create_connection()
    query = "SELECT * FROM videos_de"
    df3 = pd.read_sql(query, conn)
    conn.close()
    return df3

# Fetch comment data
def fetch_comment_data():
    conn = create_connection()
    query = "SELECT * FROM Comments_det"
    df4 = pd.read_sql(query, conn)
    conn.close()
    return df4
# Streamlit app
st.title("YouTube Data Harvesting and Warehousing")

st.sidebar.title("Navigation")
page = st.sidebar.radio("Select the Table to view", ["Channel Data", "Playlist Data", "Video Data", "Comment Data"])

if page == "Channel Data":
    st.header("Channel Data")
    df_channel = fetch_channel_data()
    st.dataframe(df_channel)
elif page == "Playlist Data":
    st.header("Playlist Data")
    df_playlist = fetch_playlist_data()
    st.dataframe(df_playlist)
elif page == "Video Data":
    st.header("Video Data")
    df_video = fetch_video_data()
    st.dataframe(df_video)
elif page == "Comment Data":
    st.header("Comment Data")
    df_comment = fetch_comment_data()
    st.dataframe(df_comment)

#sql connection
my_db=db.connect(host="localhost",
                user="user1",
                password="****",
                database="youtube")
#cursor
curr=my_db.cursor()

Question=st.selectbox("Select your Question",("1. All the Videos and Channel Name",
                                             "2. Channels with most number of videos",
                                             "3. Top 10 Most Viewed Videos",
                                             "4. Comments in each Video",
                                             "5. Videos with highest number of Likes",
                                             "6. Total Number of Likes for each videos",
                                             "7. Viewscount of each channel",
                                             "8. Videos published in the year of 2022",
                                             "9. Average Duration of all Videos in each channel",
                                             "10. Videos with highest number of comments"))

if Question=="1. All the Videos and Channel Name":
    Query_1='''select title as videos,channel_name as channelname from videos_de'''
    curr.execute(Query_1)
    my_db.commit()
    T1=curr.fetchall()
    df=pd.DataFrame(T1,columns=['Video Title','Channel Name'])
    st.write(df)

elif Question=="2. Channels with most number of videos":
    Query_2='''select channel_name as channelname,total_videos as no_videos from Channel_det order by total_videos desc '''
    curr.execute(Query_2)
    my_db.commit()
    T2=curr.fetchall()
    df2=pd.DataFrame(T2,columns=['Channel Name','No of Videos'])
    st.write(df2)

elif Question=="3. Top 10 Most Viewed Videos":
    Query_3='''select views as Views,channel_name as channelname,title as videotitle 
            from videos_de where views is not null order by views desc limit 10 '''
    curr.execute(Query_3)
    my_db.commit()
    T3=curr.fetchall()
    df3=pd.DataFrame(T3,columns=['Views','Channel Name','Video Title'])
    st.write(df3)

elif Question=="4. Comments in each Video":
    Query_4='''select comments as number_comments,title as videotitle from videos_de where comments is not null  '''
    curr.execute(Query_4)
    my_db.commit()
    T4=curr.fetchall()
    df4=pd.DataFrame(T4,columns=['No of Comments','Video Title'])
    st.write(df4)

elif Question=="5. Videos with highest number of Likes":
    Query_5='''select title as videotitle,channel_name as channelname,likes as likecount 
            from videos_de where likes is not null order by likes desc  '''
    curr.execute(Query_5)
    my_db.commit()
    T5=curr.fetchall()
    df5=pd.DataFrame(T5,columns=['Video Title','Channel Name','Likes Count'])
    st.write(df5)

elif Question=="6. Total Number of Likes for each videos":
    Query_6='''select likes as likecount,title as videotitle from videos_de  '''
    curr.execute(Query_6)
    my_db.commit()
    T6=curr.fetchall()
    df6=pd.DataFrame(T6,columns=['Likes Count','Video Title'])
    st.write(df6)

elif Question=="7. Viewscount of each channel":
    Query_7='''select channel_name as Channel Name,views as TotalViews from Channel_det '''
    curr.execute(Query_7)
    my_db.commit()
    T7=curr.fetchall()
    df7=pd.DataFrame(T7,columns=['ChannelName','Views Count'])
    st.write(df7)

elif Question=="8. Videos published in the year of 2022":
    Query_8='''select title as Videotitle,published_date as videoPublishedDate,channel_name as ChannelName 
            from videos_de where extract(year from published_date)=2022 '''
    curr.execute(Query_8)
    my_db.commit()
    T8=curr.fetchall()
    df8=pd.DataFrame(T8,columns=['VideoTitle','PublishedDate','Channel Name'])
    st.write(df8)

elif Question=="9. Average Duration of all Videos in each channel":
    Query_9='''select channel_name as ChannelName,avg(Duration) as Duration
            from videos_de group by channel_name '''
    curr.execute(Query_9)
    my_db.commit()
    T9=curr.fetchall()
    df9=pd.DataFrame(T9,columns=['Channel Name','Average Duration'])

    t9=[]
    for index,row in df9.iterrows():
        ChannelTitle=row['Channel Name']
        Avg_Duartion=row['Average Duration']
        Avg_Duartion_str=str(Avg_Duartion)
        t9.append(dict(Title=ChannelTitle,AverageDuration=Avg_Duartion_str))
    data_frame=pd.DataFrame(t9)
    st.write(data_frame)

elif Question=="10. Videos with highest number of comments":
    Query_10='''select title as videotitle,channel_name as Channel Name,comments as Comments from videos_de 
    where comments is not null order by comments desc'''
    curr.execute(Query_10)
    my_db.commit()
    T10=curr.fetchall()
    df10=pd.DataFrame(T10,columns=['Video Title','Channel Name','Comments'])
