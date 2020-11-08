Sparkify, a startup stramping app, wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. They would like to be able to analyze their streaming data, particularly what songs the users are listening to. The only place the data resides right now is in form of JSON files, making it clunky to do any analysis. 

The purpose of this ETL application is to create and load a star schema to parse and load the JSON files. 
This will allow Sparkify analysis to quickly and easily run required queries. 

The details of contents of this project are as below:
1) folder data: Contains two folders
    i) song_data: consists of JSON files that include details of songs such as artists, duration etc.
    ii) log_data: consits of JSON files that include details of listening activities of Sparkify users.
    The link to /data folder is https://r766469c826263xjupyterllyjhwqkl.udacity-student-workspaces.com/lab/tree/data
2) sql_queries.py: This is a central place for all database queries needed for data processing, including creating and dropping tables and all queries needed for data manipulation.
3) create_tables.py: This has the code for creating databases and tables. The main function re-creates database and tables. It uses sql_queries.py to drop and create required tables. 
4) test.ipynb: Calls create_tables.py and provides a place for the developer to check if data was been inserted into tables.
5) etl.ipynb: An interative environment for developers to extract data from JSON, massage it as and when needed and insert into the fact table songplays and all the dimensions such as users. 
6) etl.py: Python version of etl.ipynb. It has enhanced quality controls such as confirmation of successful insertion of data. It also generates logs on screen to keep the user appraised of progress. 
7) execute_etl_py.ipynb: This provides a one stop solution to create database schema and run and test the ETL process.

To run, please execute execute_etl_py.ip. Its best to shut down all kernals before re-running any of the scripts.