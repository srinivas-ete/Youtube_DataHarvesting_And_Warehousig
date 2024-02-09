# ------------------------------------------------------------------------------- Packages ------------------------------------------------------------------------------#
from googleapiclient.discovery import build
from pprint import pprint
import pandas as pd
import streamlit as st
from pymongo import MongoClient
import mysql.connector
from datetime import datetime

# ------------------------------------------------------------------------ Connection to Youtube API --------------------------------------------------------------------#
api_service_name = "youtube"
api_version = "v3"
youtube = build(api_service_name, api_version, developerKey='AIzaSyApSbRxbvEPhEgboEX0mVcR6YLrmtvlMcA')
# ------------------------------------------------------------------------- Connection to MongoDB -----------------------------------------------------------------------#
client = MongoClient("mongodb://localhost:27017/")
mydb = client['Youtube_DataBase']
collection = mydb['Youtube_Details']
# ------------------------------------------------------------------------- Connection to MYSQL -------------------------------------------------------------------------#
mysql_connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="srinivas",
    database="Youtube"
)
mysql_cursor = mysql_connection.cursor()

# ------------------------------------------------------------------------ Youtube Class --------------------------------------------------------------------------------#
class youtube_data_Harvesting:
    # ------------------------------------------------------------- Function to get Channel_info ------------------------------------------------------------------------#
    def channel_info(channel_id):
        channel_request = youtube.channels().list(part='snippet,contentDetails,statistics',
                                                  id=channel_id)
        channel_response = channel_request.execute()
        # ---------------------------------------------------------- Retrive data from the corresponding channel --------------------------------------------------------#
        channel_data = {
            'Channel_Name': channel_response['items'][0]['snippet']['title'],
            'Channel_Id': channel_id,
            'Subscription_Count': channel_response['items'][0]['statistics']['subscriberCount'],
            'Channel_Views': channel_response['items'][0]['statistics']['viewCount'],
            'Channel_Description': channel_response['items'][0]['snippet']['description'],
            'Playlist_Id': channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        }
        return channel_data

    # ---------------------------------------------------------------------- Function to get VideoIds -------------------------------------------------------------------#
    def video_id(playlistid, channel_id):
        videos_id = []
        next_page_Token = None
        playlist_id = playlistid
        while True:
            video_request = youtube.playlistItems().list(part='snippet,contentDetails',
                                                         playlistId=playlist_id,
                                                         maxResults=50,
                                                         pageToken=next_page_Token)
            video_response = video_request.execute()
            for item in video_response['items']:
                videos_id.append(item['contentDetails']['videoId'])
            next_page_Token = video_response.get('nextPageToken')

            if not next_page_Token:
                break
        return videos_id

    # -------------------------------------------------------------------- Function to get Video_info based on videoId's ------------------------------------------------#
    def video_info(videos):
        videos_info = {}
        for i in videos:
            vid_request = youtube.videos().list(part='snippet,contentDetails,statistics',
                                                id=i)
            vid_response = vid_request.execute()
            caption = {'true': 'Available', 'false': 'Not Available'}

            # convert PT15M33S to 00:15:33 format using Timedelta function in pandas
            def time(t):
                a = pd.Timedelta(t)
                b = str(a).split()[-1]
                return b

            # ----------------------------------------------------------- Retrive data from the VideoId's ---------------------------------------------------------------#
            video_content = {
                'Video_Id': vid_response['items'][0]['id'],
                'Video_Name': vid_response['items'][0]['snippet']['title'],
                'Video_Description': vid_response['items'][0]['snippet']['description'],
                'Tags': vid_response['items'][0]['snippet'].get('tags', []),
                'PublishedAt': vid_response['items'][0]['snippet']['publishedAt'],
                'View_Count': vid_response['items'][0]['statistics']['viewCount'],
                'Like_Count': vid_response['items'][0]['statistics'].get('likeCount', 0),
                'Dislike_Count': vid_response['items'][0]['statistics'].get('dislikeCount', 0),
                'Favourite_Count': vid_response['items'][0]['statistics'].get('favoriteCount', 0),
                'Comment_Count': vid_response['items'][0]['statistics'].get('commentCount', 0),
                'Duration': time(vid_response['items'][0]['contentDetails']['duration']),
                'Thmubnail': vid_response['items'][0]['snippet']['thumbnails']['default']['url'],
                'Caption_Status': caption[vid_response['items'][0]['contentDetails']['caption']],
                'Comments': {}
            }

            # ------------------------------------------------------------ Retrive comments_info ------------------------------------------------------------------------#
            try:
                comment_request = youtube.commentThreads().list(part='snippet,id',
                                                                videoId=i,
                                                                maxResults=100)
                comment_response = comment_request.execute()
                comment = comment_response['items']
                for c in comment:
                    comment_id = c['id']
                    comment_text = c['snippet']['topLevelComment']['snippet']['textDisplay']
                    comment_author = c['snippet']['topLevelComment']['snippet']['authorDisplayName']
                    comment_published_at = c['snippet']['topLevelComment']['snippet']['publishedAt']

                    comment_data = {
                        "Comment_Id": comment_id,
                        "Comment_Text": comment_text,
                        "Comment_Author": comment_author,
                        "Comment_PublishedAt": comment_published_at
                    }
                    video_content['Comments'][comment_id] = comment_data
                videos_info[i] = video_content
            except:
                pass
        return videos_info

    # ----------------------------------------------------------- Function to get all the required data -----------------------------------------------------------------#
    def main_data(channel_id):
        channel = youtube_data_Harvesting.channel_info(channel_id)  # Function call of channel_info
        playlist_id = channel['Playlist_Id']
        videos = youtube_data_Harvesting.video_id(playlist_id, channel_id)  # Function call of VideoId's
        videosandcomments = youtube_data_Harvesting.video_info(videos)  # Function call of video_info

        final = {
            'Channel_Name': channel, **videosandcomments
        }

        return final

    # ------------------------------------------------------------ Function to store data to MySQL ----------------------------------------------------------------------#
    def store_to_mysql(data):
        # ---------------------------------------------------------- Table creation of channel --------------------------------------------------------------------------#
        create_channel_table = """ create table if not exists channel(
                                channel_id varchar(255) primary key,
                                channel_name varchar(255),
                                subscripiton_count int,
                                channel_views int,
                                channel_description text,
                                playlist_id varchar(255)
                                )"""
        # ----------------------------------------------------------------- Table creation of Playlist ------------------------------------------------------------------#
        create_playlist_table = """
                                create table if not exists playlist(
                                playlist_id varchar(255) primary key,
                                channel_id varchar(255),
                                foreign key (channel_id) references channel(channel_id)
                            )"""
        # ------------------------------------------------------------------- Table creation of Video ------------------------------------------------------------------#
        create_video_table = """ create table if not exists video(
                                video_id varchar(255) primary key,
                                video_name varchar(255),
                                video_description text,
                                tags text,
                                published_at datetime,
                                view_count int,
                                like_count int,
                                dislike_count int,
                                favourite_count int,
                                comment_count int,
                                duration time,
                                thumbnail varchar(255),
                                caption_status varchar(255),
                                playlist_id varchar(255),
                                foreign key (playlist_id) references playlist(playlist_id)
                                )"""
        # ------------------------------------------------------------ Table creation of Comment -----------------------------------------------------------------------#
        create_comment_table = """ create table if not exists comment(
                                comment_id varchar(255) primary key,
                                comment_text text,
                                comment_author varchar(255),
                                comment_publishedat datetime,
                                video_id varchar(255),
                                foreign key (video_id) references video(video_id)
                                )"""

        mysql_cursor.execute(create_channel_table)
        mysql_cursor.execute(create_playlist_table)
        mysql_cursor.execute(create_video_table)
        mysql_cursor.execute(create_comment_table)

        mysql_connection.commit()

        # ----------------------------------------------------- Insert the values into particular tables ---------------------------------------------------------------#
        insert_channel = """ 
                        insert into channel values(%s,%s,%s,%s,%s,%s)
                    """
        insert_playlist = """
                        insert into playlist values(%s,%s)
                    """
        insert_video = """
                        insert into video values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """
        insert_comment = """ 
                        insert into comment values(%s,%s,%s,%s,%s)
                    """
        try:
            channel_values = (
                data['Channel_Name']['Channel_Id'],
                data['Channel_Name']['Channel_Name'],
                data['Channel_Name']['Subscription_Count'],
                data['Channel_Name']['Channel_Views'],
                data['Channel_Name']['Channel_Description'],
                data['Channel_Name']['Playlist_Id']
            )
            mysql_cursor.execute(insert_channel, channel_values)

            playlist_values = (
                data['Channel_Name']['Playlist_Id'],
                data['Channel_Name']['Channel_Id']
            )
            mysql_cursor.execute(insert_playlist, playlist_values)

            for video_id, video_data in data.items():
                if video_id != 'Channel_Name':
                    video_values = (
                        video_data['Video_Id'],
                        video_data['Video_Name'],
                        video_data['Video_Description'],
                        ','.join(video_data['Tags']),
                        datetime.strptime(video_data['PublishedAt'], '%Y-%m-%dT%H:%M:%SZ'),
                        video_data['View_Count'],
                        video_data['Like_Count'],
                        video_data['Dislike_Count'],
                        video_data['Favourite_Count'],
                        video_data['Comment_Count'],
                        video_data['Duration'],
                        video_data['Thmubnail'],
                        video_data['Caption_Status'],
                        data['Channel_Name']['Playlist_Id']
                    )
                    mysql_cursor.execute(insert_video, video_values)

                    for comment_id, comment_data in video_data['Comments'].items():
                        comment_values = (
                            comment_id,
                            comment_data['Comment_Text'],
                            comment_data['Comment_Author'],
                            datetime.strptime(comment_data['Comment_PublishedAt'], '%Y-%m-%dT%H:%M:%SZ'),
                            video_data['Video_Id']
                        )
                        mysql_cursor.execute(insert_comment, comment_values)
        except:
            st.write('Channel is already in MySQL Database..')
            pass

        mysql_connection.commit()
        mysql_connection.close()

    # --------------------------------------------------------------- Function to execute the queries ------------------------------------------------------------------#
    def execute_query(query):
        mysql_cursor.execute(query)
        columns = [desc[0] for desc in mysql_cursor.description]
        result = mysql_cursor.fetchall()
        return columns, result


# --------------------------------------------------------- Function to display the query result in streamlit ----------------------------------------------------------#
def display_query_result(columns, result):
    if result:
        df = pd.DataFrame(result, columns=columns)
        st.table(df)
    else:
        st.warning("No results found.")


# -------------------------------------------- Streamlit Application ---------------------------------------------------------------------------------------------------#
st.title(":blue[YouTube Data Harvesting And Warehousing]")

option = st.sidebar.selectbox(label='Select an option',
                              options=['Data Retrieve from YouTube', 'Store data to MongoDB', 'Store data to MySQL',
                                       'SQL Queries', 'Exit'])

if option == 'Data Retrieve from YouTube':
    channel_id = st.text_input('Enter channel ID:')
    submit = st.button(label='Submit')

    if submit:
        data = youtube_data_Harvesting.main_data(channel_id)
        st.json(data)
        st.success("Success")


elif option == 'Store data to MongoDB':
    channel_id = st.text_input('Enter channel ID:')
    submit = st.button(label='Collect and Store Data')

    if submit:
        data = youtube_data_Harvesting.main_data(channel_id)
        # st.json(data)
        st.success("Data retrieved successfully from YouTube", icon="✅")

        existing_data = collection.find_one({'Channel_Name.Channel_Id': channel_id})
        if existing_data:
            # Update existing document
            collection.update_one({'Channel_Name.Channel_Id': channel_id}, {'$set': data})
            st.success("Updated successfully!")
        else:
            # Insert new document
            collection.insert_one(data)
            st.success("Inserted successfully!", icon="✅")

        st.success("Data stored in MongoDB", icon="✅")


elif option == 'Store data to MySQL':
    channel_id = st.text_input('Enter channel ID:')
    submit = st.button(label='Transform to SQL')

    if submit:
        data = youtube_data_Harvesting.main_data(channel_id)
        # st.json(data)
        youtube_data_Harvesting.store_to_mysql(data)
        st.success("Data stored in MySQL.", icon="✅")

elif option == 'SQL Queries':
    q = st.selectbox('Select a Query',
                     options=['Select a Query',
                              '1. What are the names of all the videos and their corresponding channels?',
                              '2. Which channels have the most number of videos, and how many videos do they have?',
                              '3. What are the top 10 most viewed videos and their respective channels?',
                              '4. How many comments were made on each video, and what are their corresponding video names?',
                              '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                              '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                              '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                              '8. What are the names of all the channels that have published videos in the year 2022?',
                              '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                              '10. Which videos have the highest number of comments, and what are their corresponding channel names?'
                              ])

    # ------------------------------------------------------------------ Quries -----------------------------------------------------------------------------------------#
    if q == '1. What are the names of all the videos and their corresponding channels?':
        # Function call to execute the queries
        columns, query = youtube_data_Harvesting.execute_query(""" SELECT video.video_name, channel.channel_name
                                                                FROM video
                                                                JOIN playlist ON video.playlist_id = playlist.playlist_id
                                                                JOIN channel ON playlist.channel_id = channel.channel_id;
                                                            """)
        display_query_result(columns, query)  # Function call to display the query result

    if q == '2. Which channels have the most number of videos, and how many videos do they have?':
        columns, query = youtube_data_Harvesting.execute_query(""" SELECT channel.channel_name, COUNT(video.video_id) AS Video_Count
                                                                FROM channel
                                                                JOIN playlist ON channel.channel_id = playlist.channel_id
                                                                JOIN video ON playlist.playlist_id = video.playlist_id
                                                                GROUP BY channel.channel_id
                                                                ORDER BY Video_Count DESC
                                                                LIMIT 1; 
                                                            """)
        display_query_result(columns, query)

    if q == '3. What are the top 10 most viewed videos and their respective channels?':
        columns, query = youtube_data_Harvesting.execute_query(""" SELECT video.video_name, channel.channel_name, video.view_count
                                                                FROM video
                                                                JOIN playlist ON video.playlist_id = Playlist.Playlist_Id
                                                                JOIN channel ON playlist.channel_id = channel.channel_id
                                                                ORDER BY video.view_count DESC
                                                                LIMIT 10;     
                                                            """)
        display_query_result(columns, query)

    if q == '4. How many comments were made on each video, and what are their corresponding video names?':
        columns, query = youtube_data_Harvesting.execute_query(""" SELECT video.video_name, COUNT(comment.comment_id) AS Comment_Count
                                                                FROM video
                                                                LEFT JOIN comment ON video.video_id = comment.video_id
                                                                GROUP BY video.video_id, video.video_name;
                                                            """)
        display_query_result(columns, query)

    if q == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        columns, query = youtube_data_Harvesting.execute_query(""" SELECT video.video_name, channel.channel_name, video.like_count
                                                                FROM video
                                                                JOIN playlist ON video.playlist_id = playlist.playlist_id
                                                                JOIN channel ON playlist.channel_id = channel.channel_id
                                                                ORDER BY video.like_count DESC
                                                                LIMIT 1;
                                                            """)
        display_query_result(columns, query)

    if q == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        columns, query = youtube_data_Harvesting.execute_query(""" SELECT video.video_name, SUM(video.like_count) AS Total_Likes, SUM(video.dislike_count) AS Total_Dislikes
                                                                FROM video
                                                                GROUP BY video.video_id, video.video_name;
                                                            """)
        display_query_result(columns, query)

    if q == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        columns, query = youtube_data_Harvesting.execute_query(""" SELECT channel.channel_name, SUM(video.view_count) AS Total_Views
                                                                FROM channel
                                                                JOIN playlist ON channel.channel_id = playlist.channel_id
                                                                JOIN video ON playlist.playlist_id = video.playlist_id
                                                                GROUP BY channel.channel_id, channel.channel_name;
                                                            """)
        display_query_result(columns, query)

    if q == '8. What are the names of all the channels that have published videos in the year 2022?':
        columns, query = youtube_data_Harvesting.execute_query(""" SELECT DISTINCT channel.channel_name
                                                                FROM channel
                                                                JOIN playlist ON channel.channel_id = playlist.channel_id
                                                                JOIN video ON playlist.playlist_id = video.playlist_id
                                                                WHERE YEAR(video.published_at) = 2022;
                                                            """)
        display_query_result(columns, query)

    if q == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        columns, query = youtube_data_Harvesting.execute_query(""" SELECT channel.channel_name, AVG(video.duration) AS Average_Duration
                                                                FROM channel
                                                                JOIN playlist ON channel.channel_id = playlist.channel_id
                                                                JOIN video ON playlist.playlist_id = video.playlist_id
                                                                GROUP BY channel.channel_id, channel.channel_name;
                                                            """)
        display_query_result(columns, query)

    if q == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        columns, query = youtube_data_Harvesting.execute_query(""" SELECT video.video_name,channel.channel_name, max(video.comment_count) as comment_count 
                                                                FROM video
                                                                JOIN playlist on video.playlist_id=playlist.playlist_id
                                                                JOIN channel on playlist.channel_id=channel.channel_id
                                                                GROUP BY video.video_id,video.video_name,channel.channel_id,channel.channel_name
                                                                ORDER BY comment_count desc
                                                                LIMIT 1;
                                                            """)
        display_query_result(columns, query)

elif option == 'Exit':
    st.stop()

