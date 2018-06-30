# w205-final-project
## Submitted by Daniel Lee, KC Tobin, Chandan Gope

This project uses Twitter streaming, filters streams with #vacation, and analyzes them to compute trending locations with associated sentiments.
Follow these steps to run the scripts - 
1) Make sure the Linux instance has these installed  - Spark, Postgres, Tweepy, psycopg2, nltk, git
2) Run the psql command lines provided in aws_code/psql_commands_createtables.txt - This will create the necessary tables required by the scripts
3) In a terminal run python aws_code/TweetRead.py - This will start the Twitter streaming on port 5555
4) In another terminal run spark-submit aws_code/SparkDemo2.py - This will start Spark streaming listening on port 5555
5) At this point, the table tweets_analytics will start getting populated every 10 seconds
6) On a remote computer(such as your desktop), run the python notebook provided in remote_code/tweetanalytics_v4.ipynb. You will need to adjust the hosturl param. This will get the tweets from the timewindow specified in the code, process those tweets, and add the information to location_analytics
7) Now start a Postgres connection in Tableau, connect to the hosturl where the above mentioned scripts are running, and connect to the table location_analytics
8) You can now visualize the data in table location_analytics 
