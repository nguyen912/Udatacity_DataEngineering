##I. Introduction
###1. About Sparkify
   - Sparkify is a start-up with their music streaming app. They want to analyze the data they've been collecting on songs and user activities on their app, so on understanding user using behavior.
###2. Problem
   - Move all processes and data onto the cloud.
###3. Solution
   - Build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.
##II. Database schema design
![](music_streaming.drawio.png)
##III. File organization
   - File create_table.py is where creating fact and dimension tables for the star schema in Redshift.
   - File etl.py is where loading data from S3 into staging tables on Redshift and then process that data into analytics tables on Redshift.
   - File sql_queries.py is where writing SQL statements, which will be imported into the two other files above.