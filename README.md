INTRODUCTION:
The goal of this project is to develp an ETL pipeline that retrieves data from S3, processes the data using Spark and load the data as a set of dimentional tables back into S3. The data is transformed into a group of dimension tables to analise songs and user activity data collected by Sparkify. This makes use of models to design and create tables to optimise queries on datasets made available by Sparkify as log data and song data in JSON formats stored in an S3 bucket. At the end of this project, the Sparkify analysis team is able to retrieve results based on queries on what songs its users are listening to. In summary, data is retrieved from S3, processed with SPark and loaded into S3. A "facts" and "dimensions" tables is created based on SQL queries to load data from staging tables into the analytics tables.

STEPS:
Files made available to execute this project are: 
- etl.py: This file contains all processes for an ETL procedure for all fact and dim tables considered.
- dl.cfg: This file contains information on the data lake access credentials.

IMPORTANT PROCESSES:
- Retrieve Data Lake(DL) access credentials for DL processing services eg. AWS access credentials.
- Create Spark session.
- Retrieve data from S3 bucket.
- Build ETL processes and Pipelines in etl.py
- Test queries.


TABLES CREATED:
- Facts: Songplays = Made up of songplay events for the datasets given to inform business decision.
- Dim: users = Made up of users registered to the Sparkify app.
       songs = Made up of all available songs in the Sparkify music dB
     artists = Made up of all artists of the songs in the Sparkify misuc dB
        time = Made up of the timestamps of song plays by users of the Sparkify app.
        
QUERIES:
All select, join, aggregation and known available postgres queries can be applied on the Redshift cluster.