#Youtube Data Harvesting and Warehousing Project Coding

#Packages
import json
import re
from datetime import datetime
import mysql.connector as db
import pandas as pd
import streamlit as st
import pandas as pd
import json
import plotly.express as px
from PIL import Image
import os
#API
from googleapiclient.discovery import build
api_servicename='youtube'
api_version='v3'
API_Key='AIzaSyDWpdxWljH6il6AB_R1xgXocYbvSmBkNmI'

#API-Data Extraction and Collection
youtube=build('youtube','v3',developerKey='AIzaSyDWpdxWljH6il6AB_R1xgXocYbvSmBkNmI')
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

#Storing the Data using SQL
def channel_content(channel_id):
    CHANNEL_DETAILS=channel_data(channel_id)
    VIDEO_IDS_DETAILS=video_ids_data(channel_id)
    VIDEO_DETAILS=video_data(VIDEO_IDS_DETAILS)
    COMMENT_DETAILS=comment_data(VIDEO_IDS_DETAILS)
    PLAYLIST_DETAILS=playlist_data(channel_id)
    
    import mysql.connector as db
    import pandas as pd
    #connection
    my_db=db.connect(host="localhost",
                    user="vino",
                    password="shastik@2901",
                    database="project1")
    #cursor
    curr=my_db.cursor()
    #channel table
    create_query='''CREATE TABLE if not exists Channel_det(channel_name varchar(80),
                    Description text,
                    Channel_ID varchar(100) primary key,
                    Subscribers int,
                    Views int,
                    Videos_Count int,
                    Playlist_id varchar(100))'''

    curr.execute(create_query)
    my_db.commit()
    
    insert_query = '''INSERT INTO Channel_det(channel_name, Description, Channel_ID, Subscribers, Views, Videos_Count, Playlist_id)
                      VALUES (%s, %s, %s, %s, %s, %s, %s)'''
    values = tuple(CHANNEL_DETAILS.values())
    curr.execute(insert_query, values)
    my_db.commit()

    #playlist_table
    create_query='''CREATE TABLE if not exists PLAYLIST_det(Playlist_Id varchar(100) primary key,
                        Title varchar(100),
                        Channel_Id varchar(100),
                        Channel_Name varchar(100),
                        Published_Date timestamp,
                        Video_Count int,
                        FOREIGN KEY (channel_Id) REFERENCES Channel_det(Channel_ID))'''

    curr.execute(create_query)
    my_db.commit()
    
    insert_query = '''INSERT INTO PLAYLIST_det(Playlist_Id, Title, Channel_Id, Channel_Name, Published_Date, Video_Count)
                      VALUES (%s, %s, %s, %s, %s, %s)'''
    values = [tuple(pl.values()) for pl in PLAYLIST_DETAILS]
    curr.executemany(insert_query, values)
    my_db.commit()


    #Video Table
    create_query='''CREATE TABLE if not exists videos_de(Channel_Name varchar(100),
                    Channel_Id varchar(100),Video_id varchar(50) primary key,
                    Title varchar(150),Tags TEXT,Thumbnails varchar(200),
                    Description text,Published_Date Date,Duration time,
                    ViewCount int,Likes int,Comments bigint,Favorite_count bigint,                    
                    Definition varchar(10),Caption varchar(10),
                    FOREIGN KEY (Channel_Id) REFERENCES Channel_det(Channel_ID))'''

    curr.execute(create_query)
    my_db.commit()
    
    insert_query = '''INSERT INTO videos_de(Channel_Name, Channel_Id, Video_id, Title, Tags, Thumbnails, Description,
                                            Published_Date, Duration, ViewCount, Likes, Comments, Favorite_count, Definition, Caption)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    values = [tuple(video.values()) for video in VIDEO_DETAILS]
    curr.executemany(insert_query, values)
    my_db.commit()
    
    #Comment Table
    create_query='''CREATE TABLE if not exists Comments_det(Comment_Id varchar(100) primary key,
                    Video_Id varchar(80),
                    Comment_Text text,
                    Comment_Author varchar(100),
                    Comment_Published DATE,
                    FOREIGN KEY (Video_Id) REFERENCES videos_de(Video_id))'''

    curr.execute(create_query)
    my_db.commit()
    
    insert_query = '''INSERT INTO Comments_det(Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_Published)
                      VALUES (%s, %s, %s, %s, %s)'''
    values = [tuple(comment.values()) for comment in COMMENT_DETAILS]
    curr.executemany(insert_query, values)
    my_db.commit()


def table():
    channel_content('UCCQ6SXMc7MoJ88jjpn6j-8Q')
    channel_content('UCOeIvpX4x09EsWS-6FwWonQ')
    channel_content('UCkCJ0zLrSg7VudR97g-FNVQ')
    channel_content('UCQpPo9BNwezg54N9hMFQp6Q')
    channel_content('UCJcCB-QYPIBcbKcBQOTwhiA')
    channel_content('UCN5qBo0kXcPOlbLPcRylqdA')
    channel_content('UCRiz2-lYcLhGOp0p8nhq0KA')
    channel_content('UC70iJbY5k-asxC4wO37y2ag')
    channel_content('UCGgG1_YB_h4gylBbtJmDeZQ')
    channel_content('UCvaMpohMIVypvEWBEOHo1wA')

import mysql.connector as db
import pandas as pd
#connection
my_db=db.connect(host="localhost",
                user="vino",
                password="shastik@2901",
                database="project1")
#cursor
curr=my_db.cursor()

#Streamlit
import streamlit as st
import pandas as pd
import json
import plotly.express as px
from PIL import Image
import os

def fetch_channel_data():
    query='''select * from channel_det'''
    curr.execute(query)
    c1=curr.fetchall()
    my_db.commit()
    df=pd.DataFrame(c1,columns=('channel_name', 'Description', 'Channel_ID', 'Subscribers', 
                                'Views', 'Videos_Count', 'Playlist_id'))
    Result=st.write("Channel Details",df)
    return(Result)

def fetch_playlist_data():
    query='''select * from playlist_det'''
    curr.execute(query)
    c1=curr.fetchall()
    my_db.commit()
    df=pd.DataFrame(c1,columns=('Playlist_Id', 'Title', 'Channel_Id', 'Channel_Name', 'Published_Date', 'Video_Count'))
    Result=st.write("Playlist Details",df)
    return(Result)

def fetch_video_data():
    query='''select * from videos_de'''
    curr.execute(query)
    c1=curr.fetchall()
    my_db.commit()
    df=pd.DataFrame(c1,columns=('Channel_Name', 'Channel_Id', 'Video_id', 'Title', 'Tags', 'Thumbnails', 'Description',
                                'Published_Date', 'Duration', 'ViewCount', 'Likes', 'Comments', 'Favorite_count', 'Definition', 'Caption'))
    Result=st.write("Video Details",df)
    return(Result)


def fetch_comment_data():
    query='''select * from comments_det'''
    curr.execute(query)
    c1=curr.fetchall()
    my_db.commit()
    df=pd.DataFrame(c1,columns=('Comment_Id', 'Video_Id', 'Comment_Text', 'Comment_Author', 'Comment_Published'))
    Result=st.write("Comment Details",df)
    return(Result)
      
# Streamlit app
st.title(":red[YouTube Data Harvesting and Warehousing]")

st.sidebar.markdown("")
st.sidebar.image(Image.open(r"C:\Users\hp\Desktop\UTUBE PROJECT\OIP (1).jpeg"),width=215)
st.sidebar.markdown("")
st.sidebar.title(":red[Skills]")
st.sidebar.write(":blue[Python Scripting]")
st.sidebar.write(":blue[Data Collection]")
st.sidebar.write(":blue[Streamlit]")
st.sidebar.write(":blue[API integration]")
st.sidebar.write(":blue[Data Management using SQL]")

st.sidebar.title(":red[Domain]")
st.sidebar.write(":blue[Social Media]")

st.subheader("Data Collection")
if st.button("Migrate to SQL"):
    Table=table()
    st.write("Table Inserted Successfully")

page = st.radio("Go to", ["Channel Data", "Playlist Data", "Video Data", "Comment Data"])

if page=="Channel Data":
    Channel_Data=fetch_channel_data()
elif page=="Playlist Data":
    Playlist_Data=fetch_playlist_data()
elif page=="Video Data":
    Playlist_Data=fetch_video_data()
elif page=="Comment Data":
    Playlist_Data=fetch_comment_data()

Question=st.selectbox("Select the Question",("1.What are the names of all the videos and their corresponding channels?",
                        "2.Which channel have the most number of videos and how many vidoes do they have?",
                        "3.What are the top 10 most viewed videos and their respective channels?",
                        "4.How many comments were made on each video, and what are their names?",
                        "5.Which videos have the highest number of likes, and what are their channel names",
                        "6.What is the total number of likes and dislikes for each video, and what are video name",
                        "7.What is the total number of views for each channel, and what are their channel name",
                        "8.What are the names of all the channels that have published videos in the year 2022?",
                        "9.What is the average duration of all videos in each channel, and what are their channel name",
                        "10.Which videos have the highest number of comments, and what are their channel names"))

if Question=="1.What are the names of all the videos and their corresponding channels?":
    query='''select channel_name as "Channel_Name",Title as "Video_Name" from videos_de'''
    curr.execute(query)
    A=curr.fetchall()
    my_db.commit()
    df=pd.DataFrame(A,columns=("Channel_Name","Video_Name"))
    st.write('Answer',df)
elif Question=="2.Which channel have the most number of videos and how many vidoes do they have?":
    query='''select channel_name as "Channel_Name",Videos_Count from channel_det order by Videos_Count desc '''
    curr.execute(query)
    A=curr.fetchall()
    my_db.commit()
    col1,col2=st.columns(2)
    with col1:
        df=pd.DataFrame(A,columns=("Channel_Name","Video_Count"))
        st.write('Answer',df)
    col1,col2=st.columns(2)
    with col1:
        vis=px.bar(df,x="Channel_Name",y="Video_Count",title="Channel Videos Count",height=600,width=550)
        st.plotly_chart(vis)
elif Question=="3.What are the top 10 most viewed videos and their respective channels?":
    query='''select channel_name as "Channel_Name",Views from channel_det order by Views desc'''
    curr.execute(query)
    A=curr.fetchall()
    my_db.commit()
    col1,col2=st.columns(2)
    with col1:
        df=pd.DataFrame(A,columns=("Channel_Name","Views"))
        st.write('Answer',df)
    col1,col2=st.columns(2)
    with col1:
        vis=px.bar(df,x="Channel_Name",y="Views",title="Channel Views",
                    height=600,width=550)
        st.plotly_chart(vis)

elif Question=="4.How many comments were made on each video, and what are their names?":
    query='''select channel_name as "Channel_Name",Video_id,Title as "Video Title",Comments as "Number of Comments" from videos_de order by Comments desc'''
    curr.execute(query)
    A=curr.fetchall()
    my_db.commit()

    df=pd.DataFrame(A,columns=("Channel_Name","Video_ID","Video Title","Number of Comments"))
    st.write('Answer',df)
    #Visualization
    query_V='''select channel_name as "Channel_Name",Video_id,Title as "Video Title",Comments as 
    "Number of Comments" from videos_de order by Comments desc limit 10'''
    curr.execute(query_V)
    V=curr.fetchall()
    my_db.commit()
    df_V=pd.DataFrame(V,columns=("Channel_Name","Video_ID","Video Title","Number of Comments"))
    vis=px.bar(df_V,x="Channel_Name",y="Number of Comments",hover_name="Video Title",title="Top Comments Chart",
                        height=600,width=550)
    st.plotly_chart(vis)

elif Question=="5.Which videos have the highest number of likes, and what are their channel names":
    query='''select channel_name as "Channel_Name",Likes from videos_de order by Likes desc'''
    curr.execute(query)
    A=curr.fetchall()
    my_db.commit()
    df=pd.DataFrame(A,columns=("Channel_Name","Likes"))
    st.write('Answer',df)
elif Question=="6.What is the total number of likes and dislikes for each video, and what are video name":
    query='''select channel_name as "Channel_Name",Title as "Video Name",Likes from videos_de'''
    curr.execute(query)
    A=curr.fetchall()
    my_db.commit()
    df=pd.DataFrame(A,columns=("Channel_Name","Video Name","Likes"))
    st.write('Answer',df)
elif Question=="7.What is the total number of views for each channel, and what are their channel name":
    query='''select channel_name as "Channel_Name",Views as "View Count" from channel_det'''
    curr.execute(query)
    A=curr.fetchall()
    my_db.commit()
    df=pd.DataFrame(A,columns=("Channel_Name","View Count"))
    st.write('Answer',df)
elif Question=="8.What are the names of all the channels that have published videos in the year 2022?":
    query='''select Title as Videotitle,Published_Date as "Video_Published_Date",Channel_Name as ChannelName 
            from videos_de where extract(year from Published_Date)=2022 '''
    curr.execute(query)
    A=curr.fetchall()
    my_db.commit()
    df=pd.DataFrame(A,columns=("Video name","Video_Published_Date","Channel Name"))
    st.write('Answer',df)
elif Question=="9.What is the average duration of all videos in each channel, and what are their channel name":
    query='''select Channel_Name,Title as "Video Title",avg(Duration) from videos_de group by Channel_Name '''
    curr.execute(query)
    A=curr.fetchall()
    my_db.commit()
    df=pd.DataFrame(A,columns=("Channel Name","Video Title","Duration"))
    st.write('Answer',df)
elif Question=="10.Which videos have the highest number of comments, and what are their channel names":
    query='''select Title as "Video Title",Channel_Name as "Channel Name",Comments as Comments from videos_de
    where Comments is not null order by Comments desc'''
    curr.execute(query)
    A=curr.fetchall()
    my_db.commit()
    df=pd.DataFrame(A,columns=("Video Title","Channel Name","Comments"))
    st.write('Answer',df)

    #Visualization
    query_V='''select Title as "Video Title",Channel_Name as "Channel Name",Comments as Comments from videos_de
    where Comments is not null order by Comments desc limit 10'''
    curr.execute(query_V)
    V=curr.fetchall()
    my_db.commit()
    df_V=pd.DataFrame(V,columns=("Video Title","Channel Name","Comments"))
    vis=px.bar(df_V,x="Channel Name",y="Comments",hover_name="Video Title",title="Top Comments Count",
                    height=600,width=550)
    st.plotly_chart(vis)
