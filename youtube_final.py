#import streamlit as st
import streamlit as st
import pymysql
import pandas as pd
from googleapiclient.discovery import build
import isodate  # Install using `pip install isodate`
from datetime import datetime
st.set_page_config(
page_title="YouTube Data Harvesting and Warehousing")
st.title("You Tube Data Harvesting And Warehousing")
st.write("""
A Streamlit-based application to retrieve YouTube channel data using the YouTube API, store it in MySQL, 
and analyze it through a user-friendly interface.
""")
menu = st.sidebar.selectbox(
    "Navigation",
    ("Home", "Collect and Store Data", "Database Management", "Query"))
#import streamlit as st
#DATABASE CONNECTION
dataBase = pymysql.connect(
    host="localhost",
    user="root",
    password="Srym@1819",
    database="youtube2"  
)
cursorObject = dataBase.cursor()
# API CINNECTION  FOR CREATE A YOUTUBE CLIENT
def api_connection():
    api_id="AIzaSyDi4nF7hp1MOVK9eUq0EwYOwYYUFUTFnII"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=api_id)
    return youtube
youtube=api_connection()
 # FUNCTION TO GET CHANNEL INFORMATION      
def channel_info(Channel_id):
    request=youtube.channels().list(part="snippet,contentDetails,statistics",id=Channel_id)
    response=request.execute()
    for i in response["items"]:
        data=dict(channel_name=i["snippet"]["title"],
        channel_id=i["id"],
        channel_subscription_count=i["statistics"]["subscriberCount"],
        channel_viewcount=i["statistics"]["viewCount"],
        channel_description=i["snippet"]["description"],
        playlist_id=i["contentDetails"]["relatedPlaylists"]["uploads"],
        videoCount=i["statistics"]["videoCount"])
        
        return data
#FUNCTION FOR RETRIVE ALL VIDEO IDS USING CHANNEL ID
def video_id(channel_id):
    Video_id=[]
    request=youtube.channels().list(part="contentDetails",id=channel_id)
    response=request.execute()
    playlist_id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    playlist_id

    next_page_token=None
    while True:
        response1=youtube.playlistItems().list(part="snippet",playlistId=playlist_id,maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1["items"])):
            id=response1["items"][i]["snippet"]["resourceId"]["videoId"]
            Video_id.append(id)
        next_page_token=response1.get("nextPageToken")
        if next_page_token is None:
            break
    return Video_id


# ✅ Function to Convert YouTube Duration (ISO 8601) to Seconds
def convert_duration(iso_duration):
    try:
        duration = isodate.parse_duration(iso_duration)
        return int(duration.total_seconds())  # Convert to seconds
    except:
        return None  # Return None if duration is missing or invalid
# FUNCTION TO GET ALL VIDEO INFORMATION USING VIDEO ID
def video_data(video_ids):
    video_data=[]
    for v_id in video_ids:
        request2=youtube.videos().list(part="snippet,statistics,contentDetails",id=v_id)
        response2=request2.execute()
        for item in response2["items"]:
            data=dict(
            video_id=item["id"],
            channel_id=item["snippet"]["channelId"],
            channel_title=item["snippet"]["channelTitle"],
            tags=item.get("tags"),
            video_name=item["snippet"]["title"],
            Video_Description=item["snippet"]["description"],
            published_at=item["snippet"]["publishedAt"][:-1],
            duration=convert_duration(item["contentDetails"]["duration"]),
            view_count=item["statistics"].get("viewCount",0),
            like_count=item["statistics"].get("likeCount",0),
            dislike_count=item["statistics"].get("commentCount",0),
            thumbnails=item["snippet"]["thumbnails"]["default"]["url"])
            video_data.append(data) 
    return(video_data)
#comment details
def comment_info(video_ids):
    #nextpagetoken=None
    #while True:
    try:
        comment_data=[]
            
        for v_id in video_ids:
            
            request=youtube.commentThreads().list(part="snippet",videoId=v_id,maxResults=50)#,pageToken=nextpagetoken)
            response=request.execute()
            for item in response["items"]:
                data=dict(comment_id=item["id"],
                video_id=item["snippet"]["videoId"],
                channel_id=item["snippet"]["channelId"],               
                comment_author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                comment_text=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                published_at=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"][:-1])
                comment_data.append(data)
                #nextpagetoken=response.get("nextpageToken")
                #if nextpagetoken is None:
                    #   break
        return comment_data        
            
    except:
        pass
    #return comment_data
#dataframe convertion of channel_id
def channel_df(channel_id):
    channel_details=channel_info(channel_id)
    channel_details={k:[v] for k,v in channel_details.items()}
    channel_datas=pd.DataFrame(channel_details)
    return channel_datas

def video_df(channel_id):
    video_ids=video_id(channel_id)
    v_data=video_data(video_ids)
    video_data_df=pd.DataFrame(v_data)
    return video_data_df
def comment_df(channel_id):
    video_ids=video_id(channel_id)
    comment_datas=comment_info(video_ids)
    comment_data_df=pd.DataFrame(comment_datas)
    return comment_data_df


    #INSERTION OF CHANNEL DATA INTO SQL
def channel_insert(channel_datas):
    global dataBase,cursorObject
    if not dataBase or not cursorObject:
        st.error("❌ Database connection failed. Cannot insert comment data.")
        return
    sql_query = """INSERT  IGNORE INTO channel(channel_name,channel_id, channel_subscription_count, 
                channel_viewcount, channel_description, playlist_id, videoCount) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                channel_name = VALUES(channel_name),
                channel_subscription_count = VALUES(channel_subscription_count),
                channel_viewcount = VALUES(channel_viewcount),
                channel_description = VALUES(channel_description),
                playlist_id = VALUES(playlist_id),
                videoCount = VALUES(videoCount);"""
    for index,row in channel_datas.iterrows():

        cursorObject.execute(sql_query,(row["channel_name"],row["channel_id"],row["channel_subscription_count"],
                                        row["channel_viewcount"],row["channel_description"],row["playlist_id"],row["videoCount"]))

            
    #cursorObject.executemany(sql_query, channel_data)
    dataBase.commit()
# insertion of video table
def video_insert(video_data_df):
    global dataBase,cursorObject
    if not dataBase or not cursorObject:
        st.error("❌ Database connection failed. Cannot insert comment data.")
        return
    video_sql_query = """INSERT INTO video(video_id,channel_id,channel_title,
                    tags,video_name,Video_Description,published_at,duration,view_count,like_count,
                    dislike_count,thumbnails) 
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE 
                channel_title=VALUES(channel_title), tags=VALUES(tags), 
                video_name=VALUES(video_name), Video_Description=VALUES(Video_Description), 
                published_at=VALUES(published_at), duration=VALUES(duration), 
                view_count=VALUES(view_count), like_count=VALUES(like_count), 
                dislike_count=VALUES(dislike_count), thumbnails=VALUES(thumbnails);"""
    for index,row in video_data_df.iterrows():
        cursorObject.execute(video_sql_query,(row["video_id"],row["channel_id"],row["channel_title"],row["tags"],row["video_name"],row["Video_Description"],
                                            row["published_at"],row["duration"],row["view_count"],row["like_count"],
                                            row["dislike_count"],row["thumbnails"]))


    #cursorObject.execute(video_sql_query, video_data)
    dataBase.commit()
def comment_insert(comment_data_df):    
    global dataBase,cursorObject
    if not dataBase or not cursorObject:
        st.error("❌ Database connection failed. Cannot insert comment data.")
        return
    comment_query="""INSERT INTO comment(comment_id,video_id,channel_id,comment_author,
                        comment_text,published_at) 
                VALUES (%s,%s,%s,%s,%s,%s) 
                ON DUPLICATE KEY UPDATE
                comment_text = VALUES(comment_text),
                comment_author = VALUES(comment_author)"""
    for index,row in comment_data_df.iterrows():
        
        cursorObject.execute(comment_query,(row["comment_id"],row["video_id"],row["channel_id"],
                            row["comment_author"],row["comment_text"],row["published_at"]))
        
    dataBase.commit()

#home page  function in streamlit    
def homepage():

    st.header("Welcome to the YouTube Data Harvesting Tool")
    st.write("""
    This tool allows you to:
    - Retrieve data from YouTube channels,  videos, and comments.
    - Store the retrieved data in a MySQL database.
    - Query and analyze the stored data.
    """)
#collect_store_data function in streamlit
def collect_store_data():
    st.header("Collect and Store Data")

    channel_id=st.text_input(label="ENTER CHANNEL ID")
    if channel_id:
        st.session_state["channel_id"] = channel_id
        #channel_data=channel_info(channel_id)
        channel_datas=channel_df(channel_id)
        video_data_df=video_df(channel_id)
        comment_data_df=comment_df(channel_id)
        #dataBase,cursorObject=connect_to_mysql()
        if not dataBase or not cursorObject:
            st.error("❌ Database connection failed. Cannot store data.")
            return  
        channel_insert(channel_datas)
        video_insert(video_data_df)

        comment_insert(comment_data_df)
        if not channel_datas.empty:
            st.success("✅ Channel data collected successfully!")
        else:
            st.error("❌ Channel data not collected. Please check the Channel ID.")

def fetch_channel_data(channel_id):
    global cursorObject
    query="select * from channel where channel_id=%s"
    cursorObject.execute(query,(channel_id,))
    data = cursorObject.fetchall()
    if data:
        columns = ["channel_name", "channel_id", "channel_subscription_count", "channel_viewcount", "channel_description", "playlist_id", "videoCount"]
        return pd.DataFrame(data, columns=columns)
    return pd.DataFrame() 
def fetch_video_data(channel_id):
    global cursorObject
    query="select * from video where channel_id=%s"
    cursorObject.execute(query,(channel_id,))
    data=cursorObject.fetchall()
    if data:
        columns=["video_id","channel_id","channel_title","tags","video_name","Video_Description","published_at",
                 "duration","view_count","like_count","dislike_count","thumbnails"]
        return pd.DataFrame(data,columns=columns)
    return pd.DataFrame()
def fetch_comment_data(channel_id):
    global cursorObject
    query="select * from comment where channel_id=%s"
    cursorObject.execute(query,(channel_id,))
    data=cursorObject.fetchall()
    if data:
        columns=["comment_id","video_id","channel_id","comment_author","comment_text","published_at"]
        return pd.DataFrame(data,columns=columns)
    return pd.DataFrame()
def database_management():
    st.header("Database Management")
    channel_id =st.session_state.get("channel_id", "")
    if not channel_id:
        st.warning("⚠️ No Channel ID available. Please collect and store data first.")
        return
        #dataBase, cursorObject = connect_to_mysql()
    fetch_channel_datas=fetch_channel_data(channel_id)
    fetch_video_data_df = fetch_video_data(channel_id)
    fetch_comment_data_df=fetch_comment_data(channel_id)
    #dataBase.close()

        
    # Display Channel Details
    if fetch_channel_datas.empty:
        st.warning("No channel data found in the database for this Channel ID.")
    else:
        st.subheader("Channel Details")
        st.dataframe(fetch_channel_datas)

    # Display Video Details
    if fetch_video_data_df.empty:
        st.warning("⚠️ No video data found in the database for this Channel ID.")
    else:
        if st.button("Show video details"):  # ✅ Button appears only if data exists
            st.subheader("Video Details")
            st.dataframe(fetch_video_data_df)

    # ✅ Logic for Displaying Comments
    if st.button("Show comment details", key="comment_button"):  # ✅ If button is clicked
        if fetch_comment_data_df.empty:  
            st.warning("⚠️ No comment data found in the database for this Channel ID.")
        else:
            st.subheader("Comment Details")
            st.dataframe(fetch_comment_data_df)
    #else:  # ✅ If button is NOT clicked
        #st.warning("Click 'Show comment details' to view comments.")

            
if menu=="Home":
    homepage()
    
elif menu=="Collect and Store Data":
    
    collect_store_data()
elif menu=="Database Management":
    database_management()
#
    
elif menu=="Query":
    st.subheader("Query and Visualize Data")

    if dataBase is None or cursorObject is None:
        print("❌ Database connection failed.")
    
    else:
        query_option=["Names of all the videos and their corresponding channels",
        "Channels have the most number of videos, and total videos count",
        "Top 10 most viewed videos and their respective channels",
        "Video name and their comment count",
        "Videos have the highest number of likes, and their corresponding channel names",
        "Total number of likes and dislikes for each video, and their corresponding video names",
        "Total number of views for each channel, and their corresponding channel names",
        "Names of all the channels that have published videos in the year 2022",
        "The average duration of all videos in each channel, and their corresponding channel names",
        "videos have the highest number of comments, and what are their corresponding channel names"]
    selected_query=st.selectbox("Choose a Query:", query_option)
    query_mapping={"Names of all the videos and their corresponding channels":"select video.video_name, channel.channel_name FROM video JOIN channel on video.channel_id = channel.channel_id",
        "Channels have the most number of videos, and total videos count":"select channel_name,videoCount from channel where videoCount=(select max(videoCount) from channel)",
        "Top 10 most viewed videos and their respective channels":"select channel.channel_name,video.video_id,video.video_name from video join channel where channel.channel_id=video.channel_id order by  view_count desc limit 10",
        "Video name and their comment count":"SELECT video.video_name, video.video_id, COUNT(comment.comment_id) AS total_comments FROM video LEFT JOIN comment ON video.video_id = comment.video_id GROUP BY video.video_id, video.video_name",
        "Videos have the highest number of likes, and their corresponding channel names":
        "select video.video_name,channel.channel_name,channel.channel_id from video join channel where like_count=(select max(like_count) from video)",
        "Total number of likes and dislikes for each video, and their corresponding video names":
        "select video_name,sum(like_count)as total_likes,sum(dislike_count) as total_dislikes from video group by video_id",
        "Total number of views for each channel, and their corresponding channel names":
        "select channel.channel_name,sum(video.view_count) as total_views from video join channel on channel.channel_id=video.channel_id group by channel.channel_id",
        "Names of all the channels that have published videos in the year 2022":
        "select channel.channel_name from channel join video on channel.channel_id=video.channel_id where left(video.published_at,4)='2022' group by channel_name",
        "The average duration of all videos in each channel, and their corresponding channel names":
        "select channel.channel_name,avg(video.duration) as avg_duration from video join channel on channel.channel_id=video.channel_id group by channel.channel_id",
        "videos have the highest number of comments, and what are their corresponding channel names":
        "select c.channel_name,v.video_name,v.video_id,count(com.comment_id) as comment_count from video v join channel c on v.channel_id=c.channel_id join comment com on  v.video_id=com.video_id group by c.channel_name,v.video_id,v.video_name order by comment_count desc limit 5"}
    #st.button("Run Query")
    if st.button("Run query"):    
        query=query_mapping.get(selected_query,"")
        if query:
            cursorObject.execute(query)
            results = cursorObject.fetchall()
            column_names = [desc[0] for desc in cursorObject.description]
            column_df=pd.DataFrame(results,columns=column_names)

            st.dataframe(column_df)    # Display results in a table
        else:
            st.error("Invalid Query Selection")