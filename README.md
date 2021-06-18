# Data Modeling with Postgres

##### Introduction
Skarkify is a startup, and they want to analyze the data they have been collecting on songs and the user activities from their new music streaming app. The particular interest is in answering what songs are the users more inclined to listening. Currently, they have a directory of JSON logs on user activity on the app and a directory with JSON metadata on the songs in their app. 

For the purpose of analyzing this, Postgres database has been created with tables designed to optimize the queries on song play analysis. Database schema has been created and ETL pipeline for the analysis as well. 

##### Project Description
Used Data Modeling with Postgres and ETL pipeline using python for analyzing the data. Fact and Dimension tables are defined for a star schema for a particular 

##### Files
1. **test.ipynb** displays the first few rows of each table to let you check your database.
2. **create_tables.py** drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
3. **etl.ipynb** reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
4. **etl.py** reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.
5. **sql_queries.py** contains all your sql queries, and is imported into the last three files above.
6. **README.md** provides discussion on your project.

# Schema for Analysis
Star schema is created which is optimized for queries on song play analysis using the song and log datasets. The tables are:

##### Fact Table
1. **songplays** - records in log data associated with song plays i.e. records with page NextSong
    - *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

##### Dimension Tables
2. **users** - users in the app
    - *user_id, first_name, last_name, gender, level*
    
3. **songs** - songs in music database
    - *song_id, title, artist_id, year, duration*
    
4. **artists** - artists in music database
    - *artist_id, name, location, latitude, longitude*
    
5. **time** - timestamps of records in **songplays** broken down into specific units
    - *start_time, hour, day, week, month, year, weekday*
    

##### To run it:
Firstly, we will need to run - **python3 create_tables.py** each time, and check if it is connected to the Sparkify database of **test.ipynb**. Then, we restart **test.ipynb** before running **python3 etl.py**. Similarly, we go for running the **etl.ipynb**.