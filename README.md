# YouTube-Data-Harvesting-and-Warehousing-using-SQL-and-Streamlit

## Project Overview
This project involves creating a Streamlit application to harvest and analyze data from multiple YouTube channels. The application allows users to input YouTube channel IDs, retrieve detailed channel and video data using the YouTube API, store the data in a SQL database, and run queries to analyze and visualize the data.

## Key Features
* Data Extraction: Fetch detailed statistics from YouTube channels including channel name, subscribers, video count, and more.
* Video and Comment Data: Extract information on videos and comments, including likes, views, and comment text.
* Data Storage: Store the extracted data in a MySQL or PostgreSQL database.
* Query Execution: Run predefined SQL queries to get insights into the data, such as top videos, most commented videos, and more.
* Streamlit Interface: User-friendly interface built with Streamlit for data input, extraction, and visualization.

## Domain
Social Media

# Installation and Setup
## Prerequisites
* Python 3.8 or higher
* MySQL or PostgreSQL database
* Google API key for YouTube Data API

## Install Required Libraries
You can install the required libraries using pip.

## Setting Up the Database
1. Create a MySQL or PostgreSQL Database.
2. Configure Your Database Connection.
3. Update the database connection details in the mydb connection parameters and database_url in the script to match your database configuration.

## Google API Key
You will need a Google API key for accessing the YouTube Data API. Update the api_key variable in the script with your key.

## Extract and Analyze Data:

* Enter a YouTube channel ID in the Data Extraction section.
* Click "Fetch and Store Data" to retrieve and store data.
* Use the available SQL queries to analyze the data.

## Available Queries
* Video Names and Corresponding Channels
* Channels with Most Videos
* Top 10 Most Viewed Videos
* Number of Comments per Video
* Videos with Highest Likes
* Total Likes and Dislikes per Video
* Total Views per Channel
* Channels with Videos in 2022
* Average Duration of Videos per Channel
* Videos with Most Comments

## Acknowledgments
* Ms. Shadiya for guidance and support.
* ChatGPT for providing assistance and suggestions.
* YouTube API Documentation for comprehensive resources.

## Contact
For more information or collaboration opportunities, please reach out:
* Name: Prakash B
* LinkedIn: Prakash B
* GitHub: 21-Prakash
* Email: pprakash7285@gmail.com

Thank you for exploring this project! 🚀
