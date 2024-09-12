# Importing the neccesary libraries and the functions provided by the library
import pandas as pd
import streamlit as st
from pymysql import connect
from sqlalchemy import create_engine
from googleapiclient.discovery import build
from sqlalchemy.exc import IntegrityError
import time, random

# Conneting to YouTube API to extract data
api_service_name = "youtube"
api_version = "v3"
api_key = ""
youtube = build(api_service_name, api_version, developerKey = api_key)

# MySQL connection
mydb = connect(
    host="localhost",
    user="root",
    password="",
    database = "YouTube_database"
    )
mycursor = mydb.cursor()

# Extracting channel data
def channel_data(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    channel_data = {
        "channnel_id": channel_id,
        "channel_name": response['items'][0]['snippet']['title'],
        "description": response['items'][0]['snippet']['description'],
        "published_at": response['items'][0]['snippet']['publishedAt'][:-1],
        "video_count": response['items'][0]['statistics']['videoCount'],
        "subscriber_count": response['items'][0]['statistics']['subscriberCount'],
        "view_count": response['items'][0]['statistics']['viewCount'],
        "playlist_id": response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        }
    return(channel_data)

# Converting ISO 8601 duration format to HH:MM:SS format for the duration data in the video data
def convert(input_):
    input_ = input_[2:]
    alpha=''
    digit =''
    for i in input_:
        if i.isalpha():
            alpha=alpha+i+' '
        if i.isdigit():
            digit=digit+i
        else:
            digit=digit+' '
    alpha=alpha.split()
    digit=digit.split()
    my_dict = {}
    my_dict = dict(zip(alpha,digit))
    out = []

    if 'H' not in my_dict.keys():
        out.append('00')
    else:
        if len(my_dict['H'])<2:
            out.append('0'+my_dict['H'])
        else:
            out.append(my_dict['H'])

    if 'M' not in my_dict.keys():
        out.append('00')
    else:
        if len(my_dict['M'])<2:
            out.append('0'+my_dict['M'])
        else:
            out.append(my_dict['M'])


    if 'S' not in my_dict.keys():
        out.append('00')
    else:
        if len(my_dict['S'])<2:
            out.append('0'+my_dict['S'])
        else:
            out.append(my_dict['S'])
    return ':'.join(out)

# Extracting video data
def video_data(playlist_id):
    videos = []
    next_page_token = None
    
    while True:
        request = youtube.playlistItems().list(
            part='snippet,contentDetails',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token  # Use the nextPageToken for pagination
        )
        response = request.execute()
        
        # Process the videos in the current response
        for item in response.get('items', []):
            video_id = item['contentDetails']['videoId']
            video_request = youtube.videos().list(
                part='snippet,statistics,contentDetails',
                id=video_id
            )
            video_response = video_request.execute()
            
            if 'items' in video_response and len(video_response['items']) > 0:
                video_data = {
                        'video_id': video_response['items'][0]['id'],
                        'playlist_id': playlist_id,
                        'title': video_response['items'][0]['snippet']['title'],
                        'description': video_response['items'][0]['snippet']['description'],
                        'published_at': video_response['items'][0]['snippet']['publishedAt'][:-1],
                        'views': video_response['items'][0]['statistics'].get('viewCount', 'N/A'),
                        'likes': video_response['items'][0]['statistics'].get('likeCount', 'N/A'),
                        'favorites': video_response['items'][0]['statistics'].get('favoriteCount', 'N/A'),
                        'comments': video_response['items'][0]['statistics'].get('commentCount', 'N/A'),
                        'duration': convert(video_response['items'][0]['contentDetails'].get('duration', 'N/A')),
                        'thumbnail': video_response['items'][0]['snippet']['thumbnails']['default']['url'] if 'thumbnails' in video_response['items'][0]['snippet'] else 'N/A',
                        'caption_status': video_response['items'][0]['contentDetails'].get('caption', 'N/A')
                }
                videos.append(video_data)
        
        # Check if there is a next page
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break  # Exit the loop if no more pages
    
    return videos

# Extracting comment data
def comment_data(video_id):
    comments = []
    try:
        response = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            textFormat='plainText',
            maxResults=100  
        ).execute()
        
        while response:
            for item in response['items']:
                comment_id = item['snippet']['topLevelComment']['id']
                comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
                author = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
                published_at = item['snippet']['topLevelComment']['snippet']['publishedAt'][:-1]
                comments.append({
                    'comment_id': comment_id,
                    'video_id': video_id,
                    'author': author,
                    'comment': comment,
                    'published_at': published_at
                })
                
            if 'nextPageToken' in response:
                response = youtube.commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    textFormat='plainText',
                    maxResults=100,
                    pageToken=response['nextPageToken']
                ).execute()
            else:
                break
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    
    return comments

# Creating table to store the extracted data
def table_creation():
    channel_table = mycursor.execute("""
            CREATE TABLE IF NOT EXISTS Channels (
            channel_id VARCHAR(255) PRIMARY KEY, 
            channel_name VARCHAR(255), 
            description TEXT, 
            published_at DATETIME, 
            video_count INT, 
            subscriber_count INT, 
            view_count INT, 
            playlist_id VARCHAR(255))""")
    mydb.commit()

    comment_table = mycursor.execute("""
            CREATE TABLE IF NOT EXISTS Comments ( 
            comment_id VARCHAR(255) PRIMARY KEY,
            video_id VARCHAR(255),  
            author VARCHAR(255),
            comment TEXT,
            published_at DATETIME)""")
    mydb.commit()

    video_table = mycursor.execute("""
            CREATE TABLE IF NOT EXISTS Videos (
            video_id VARCHAR(255) PRIMARY KEY,
            playlist_id VARCHAR(255),
            title VARCHAR(255),
            description TEXT,
            published_at DATETIME,
            views INT,
            likes INT,
            favorites INT,
            comments INT,
            duration TIME,
            thumbnail VARCHAR(255),
            caption_status VARCHAR(255))""")
    mydb.commit()

# Function to insert the video data into the videos table
def insert_into_videos(playlist_id):
    data = video_data(playlist_id)
    df = pd.DataFrame(data)

    db_config = {
        'user': 'root',
        'password': '',
        'host': 'localhost',
        'database': 'YouTube_database'
        }
    
    database_url = 'mysql://root:Prakash%40980321@localhost:3306/YouTube_database'
    engine = create_engine(database_url)
    try:
        df.to_sql('videos', con=engine, if_exists='append', index=False)
    except IntegrityError as e:
        print(f"An IntegrityError occurred: {e}")
    except Exception as e:
            print(f"An unexpected error occurred while inserting videos: {e}")

# Function to insert the comments data into the comments table
def insert_into_comments(video_id):
    data = comment_data(video_id)
    df = pd.DataFrame(data)

    db_config = {
        'user': 'root',
        'password': '',
        'host': 'localhost',
        'database': 'YouTube_database'
        }
    
    database_url = 'mysql://root:Prakash%40980321@localhost:3306/YouTube_database'
    engine = create_engine(database_url)

    try:
        df.to_sql('comments', con=engine, if_exists='append', index=False)
    except IntegrityError as e:
        print(f"An IntegrityError occurred: {e}")
    except Exception as e:
            print(f"An unexpected error occurred while inserting comments: {e}")

# Function to insert the data into the table
def insert_into_channels(channel_id):
    data = channel_data(channel_id)
    sql = "INSERT INTO channels (channel_id, channel_name, description, published_at, video_count, subscriber_count, view_count, playlist_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    val = tuple(data.values())
    try:
        mycursor.execute(sql, val)
        mydb.commit()
        # Fetch and store videos
        insert_into_videos(data['playlist_id'])
        # Fetch and store comments for each video
        video_ids = [video['video_id'] for video in video_data(data['playlist_id'])]
        for video_id in video_ids:
            insert_into_comments(video_id)
        st.success("Data fetched and stored successfully.")
    except Exception as e:
        st.warning("This channel data already exists")

# Function to connect to SQLDB to run the queries
def run_query(query):
    db_config = {
        'user': 'root',
        'password': '',
        'host': 'localhost',
        'database': 'YouTube_database'
        }
    
    database_url = 'mysql://root:Prakash%40980321@localhost:3306/YouTube_database'    
    engine = create_engine(database_url)
    
    with engine.connect() as connection:
        return pd.read_sql(query, connection)
  
def main():
    st.sidebar.title("Navigation")
    selection = st.sidebar.radio("Go to", ["Home", "Data Extraction", "Visualization", "About"])

    if selection == "Home":
        st.write("""
            <img src="https://www.gstatic.com/youtube/img/branding/youtubelogo/svg/youtubelogo.svg" style="width: 200px; height: 50px;">
            """, unsafe_allow_html=True)
        st.title("Data Harvesting and Warehousing")
        st.write("""
        **🌟 Overview**

        Discover how our tool can help you gather and analyze YouTube data effortlessly. Whether you're a data enthusiast or just curious about YouTube metrics, this application is designed to help you gather and understand YouTube data effortlessly.

        **🔍 What You Can Do:**
        - **Extract Channel Data:** Fetch detailed statistics about YouTube channels.
        - **Retrieve Video Information:** Access data on individual videos including views, likes, and comments.
        - **Analyze Comments:** Gather and analyze comments from videos to gain insights into viewer engagement.
        - **Run Queries:** Utilize our 10 pre-defined queries to uncover detailed insights into your YouTube data.

        **🚀 How to Get Started:**
        1. **Enter a Channel ID** to start extracting data.
        2. **Explore Data Queries** to analyze and understand the fetched data.
        3. **Visualize Data** in the *Visualization* section (currently a fun, fictional feature! 😉).

        **💬 Have Questions?**
        - Check out the **About section** for more details about this application.
        - Reach out via [LinkedIn](https://www.linkedin.com/in/prakash-b-4b509a321) for any queries or feedback.

        **🎉 Thank You for Visiting!**
        Enjoy exploring and let us know how we can make your data journey better.
        """)
    
    elif selection == "Data Extraction":
        st.title("YouTube Data Extraction")
    
        c_id = st.text_input("Enter Channel ID:", help="You can find the channel ID in the URL of the YouTube channel.")

        if st.button("Fetch and Store Data"):
            if c_id:
                table_creation()   
                # Fetch and store data
                insert_into_channels(c_id)
                data = channel_data(c_id)
            else:
                st.error("Enter a Valid channel id")
        
        # Query options
        query_option = st.selectbox("Select a query:", [
            "Video Names and Corresponding Channels",
            "Channels with Most Videos",
            "Top 10 Most Viewed Videos",
            "Number of Comments per Video",
            "Videos with Highest Likes",
            "Total Likes and Dislikes per Video",
            "Total Views per Channel",
            "Channels with Videos in 2022",
            "Average Duration of Videos per Channel",
            "Videos with Most Comments"
        ])
        
        if st.button("Run Query"):
            query_result = ""
            
            if query_option == "Video Names and Corresponding Channels":
                query_result = run_query("""
                    SELECT Videos.title AS video_title, Channels.channel_name
                    FROM Videos
                    JOIN Channels ON Videos.playlist_id = Channels.playlist_id
                """)
            
            elif query_option == "Channels with Most Videos":
                query_result = run_query("""
                    SELECT Channels.channel_name, COUNT(Videos.video_id) AS video_count
                    FROM Channels
                    JOIN Videos ON Channels.playlist_id = Videos.playlist_id
                    GROUP BY Channels.channel_name
                    ORDER BY video_count DESC
                """)
            
            elif query_option == "Top 10 Most Viewed Videos":
                query_result = run_query("""
                    SELECT Videos.title AS video_title, Channels.channel_name, Videos.views
                    FROM Videos
                    JOIN Channels ON Videos.playlist_id = Channels.playlist_id
                    ORDER BY Videos.views DESC
                    LIMIT 10
                """)
            
            elif query_option == "Number of Comments per Video":
                query_result = run_query("""
                    SELECT Videos.title AS video_title, COUNT(Comments.comment_id) AS comment_count
                    FROM Videos
                    JOIN Comments ON Videos.video_id = Comments.video_id
                    GROUP BY Videos.title
                """)
            
            elif query_option == "Videos with Highest Likes":
                query_result = run_query("""
                    SELECT Videos.title AS video_title, Channels.channel_name, Videos.likes
                    FROM Videos
                    JOIN Channels ON Videos.playlist_id = Channels.playlist_id
                    ORDER BY Videos.likes DESC
                """)
            
            elif query_option == "Total Likes and Dislikes per Video":
                query_result = run_query("""
                    SELECT Videos.title AS video_title, Videos.likes, Videos.favorites AS dislikes
                    FROM Videos
                """)
            
            elif query_option == "Total Views per Channel":
                query_result = run_query("""
                    SELECT Channels.channel_name, SUM(Videos.views) AS total_views
                    FROM Channels
                    JOIN Videos ON Channels.playlist_id = Videos.playlist_id
                    GROUP BY Channels.channel_name
                """)
            
            elif query_option == "Channels with Videos in 2022":
                query_result = run_query("""
                    SELECT DISTINCT Channels.channel_name
                    FROM Channels
                    JOIN Videos ON Channels.playlist_id = Videos.playlist_id
                    WHERE YEAR(Videos.published_at) = 2022
                """)
            
            elif query_option == "Average Duration of Videos per Channel":
                query_result = run_query("""
                    SELECT Channels.channel_name, AVG(TIME_TO_SEC(Videos.duration)) AS avg_duration
                    FROM Channels
                    JOIN Videos ON Channels.playlist_id = Videos.playlist_id
                    GROUP BY Channels.channel_name
                """)
            
            elif query_option == "Videos with Most Comments":
                query_result = run_query("""
                    SELECT Videos.title AS video_title, Channels.channel_name, COUNT(Comments.comment_id) AS comment_count
                    FROM Videos
                    JOIN Channels ON Videos.playlist_id = Channels.playlist_id
                    JOIN Comments ON Videos.video_id = Comments.video_id
                    GROUP BY Videos.title, Channels.channel_name
                    ORDER BY comment_count DESC
                    LIMIT 10
                """)

            # Display the query result
            if not query_result.empty:
                st.dataframe(query_result)
            else:
                st.write("No data found.")

    elif selection == "Visualization":
        st.title("Visualization")

        # Display an initial message with a button to check premium features
        st.write("""
        **🚀 Welcome to the Visualization Section!**

        We're thrilled you're here to explore the data! Currently, our advanced visualization features are offered through a *premium version only*.

        Want to dive deeper into your data? Click the button below to see our “premium” features 
        """)
        
        # Button to simulate premium subscription action
        if st.button("Subscribe to Premium"):
            st.write("Processing your request...")
            progress_bar = st.progress(0)
            for percent_complete in range(0, 101, 10):
                time.sleep(random.uniform(0.2, 1.0))  # Simulate time delay
                progress_bar.progress(percent_complete)
            st.write("🎉 Congrats! You've 'subscribed' to premium! Just kidding, but thanks for clicking! 😄")
            st.write("""
            **🔔 Premium Alert!**
                     
            The "Premium Version" is a fun, imaginary feature. It doesn’t really exist!
                     
            Since visualization isn't a requirement for this project, I thought it would be fun to add a playful twist to this section. Enjoy the fun and explore the current features! 😜

            **What’s Next?**
            - Dive into the data queries we currently have.
            - Have a laugh with us about our imaginary features and let us know your thoughts!
            - Make sure to check out the About section for detailed information about how this application works!
                     
            **Stay Connected:**
            - For updates, fun data stories, and more, connect with me on [LinkedIn](https://www.linkedin.com/in/prakash-b-4b509a321).

            **Thanks for your sense of humor and curiosity!** 😄
            """)

    elif selection == "About":
        st.title("About This Application")
        
        st.write("""
        **Welcome to the YouTube Data Harvesting and Warehousing Application!** 🎥

        This application is designed to help users fetch and store YouTube data using the YouTube API and storing it in MySQL Workbench.
                                    
        **Key Features:**
        - **Data Extraction:** Seamlessly fetch comprehensive data from YouTube channels, including video statistics, comments, and more.
        - **Data Storage:** Efficiently store extracted data in a MySQL database, facilitating easy access and management.
        - **Querying Capabilities:** Run various pre-defined SQL queries to gain insights into the data and generate meaningful analytics.

        **How It Works:**
        1. **Enter a Channel ID:** Provide the Channel ID of the YouTube channel you want to analyze.
        2. **Data Fetching:** The application retrieves data from the specified channel, including video details and comments.
        3. **Data Storage and Analysis:** Extracted data is stored in a MySQL database, and users can run queries to analyze the data.

        **Technical Details:**
        - **API Integration:** Utilizes the YouTube Data API v3 for data retrieval.
        - **Database Management:** Employs MySQL for data storage and management.
        - **Data Visualization:** While visualization features are simulated, the application includes a playful message about premium features to engage users.

        **Developer Information:**
        - **Name:** Prakash B
        - **LinkedIn Profile:** [Linkedin](https://www.linkedin.com/in/prakash-b-4b509a321)
        - **GitHub Profile:** [Github](https://github.com/21-Prakash)
        - **Email:** pprakash7285@gmail.com

        **Acknowledgments:**
        - Special thanks to **Ms.Shadiya** for their guidance and support.
        - A big shoutout to ChatGPT for providing assistance and suggestions throughout the development of this project.
        - Appreciation to the YouTube API documentation for its comprehensive resources.

        **Contact:**
        - For further information or collaboration opportunities, please reach out to me via [Linkedin](https://www.linkedin.com/in/prakash-b-4b509a321) or email.

        **Disclaimer:**
        - This application is created for educational purposes and is not officially affiliated with YouTube or Google.

        **Thank you for exploring this project!** 🚀
        """)
    
if __name__ == "__main__":
    main()
