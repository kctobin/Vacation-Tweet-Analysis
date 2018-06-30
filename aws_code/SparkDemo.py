import time
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT



def sendPartition(iter):
        conn = psycopg2.connect(database="postgres", user="power_user", password="$poweruserpassword", host="localhost", port="5432")
        cur = conn.cursor()
	for tweet in iter:
		json_tweet = json.loads(tweet)
		requiredKeys = ['id', 'created_at', 'text', 'timestamp_ms', 'quote_count',\
                        'reply_count', 'is_quote_status', 'favorite_count', 'retweeted',\
                        'coordinates', 'retweet_count']
		if all(k in json_tweet for k in requiredKeys):
			if(json_tweet['id'] != 'null' and  not json_tweet['text'].startswith("RT")):
				st0 = "INSERT INTO tweets "
				st1 = "(ID,CREATED_AT,TEXT,TIMESTAMP_MS,QUOTE_COUNT,REPLY_COUNT,"
				st2 = "IS_QUOTE_STATUS,FAVORITE_COUNT,RETWEETED,COORDINATES,RETWEET_COUNT) "
				st4 = "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				st5 = st0+st1+st2+st4
				#cur.execute(st5, (json_tweet['id'], json_tweet['created_at'], json_tweet['text'].encode('utf-8'),
				#	json_tweet['timestamp_ms'], json_tweet['quote_count'], json_tweet['reply_count'], json_tweet['is_quote_status'],
				#	json_tweet['favorite_count'], json_tweet['retweeted'], json_tweet['coordinates'], json_tweet['retweet_count']) )
				cur.execute("INSERT INTO tweets (ID,TEXT) VALUES (%s,%s)", (json_tweet['id'], json_tweet['text'].encode('utf-8')))
				conn.commit()
				print("\nINSERTED RECORD INTO TWEETS\n\n")
		else:
			print("\n\n\n\KEYS NOT FOUND!!!!!\n\n\n")
	conn.close()



sc = SparkContext("local[2]", "Twitter Demo")
ssc = StreamingContext(sc, 10)
sc.setLogLevel("ERROR")


IP = "localhost"
Port = 5555

lines = ssc.socketTextStream(IP, Port)
lines.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))
ssc.start()
ssc.awaitTermination()
