import time
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from nltk.tag import pos_tag
from nltk.sentiment.vader import SentimentIntensityAnalyzer


def extractNouns(tweet):
	vacation_stop_words = ['vacation', 'holiday', 'hotel', 'travel', 'tourism', 'beach,', 'sun,', 'sea', 'camera',
                          'summer', 'winter', 'mountain', 'river', 'island', 'rain', 'cruise', 'camera', 'nature']
	cleaned_tweet = [t.replace("#", "") for t in tweet.split() if not t.startswith('http')]
	cleaned_tweet2 = [t.strip().rstrip('\u2026') for t in cleaned_tweet  if t not in vacation_stop_words]
	cleaned_tweet3 = [t.strip().rstrip('...') for t in cleaned_tweet2]
	if(len(cleaned_tweet3) > 0):
		cleaned_tweet3 = filter(None, cleaned_tweet3)
		print(cleaned_tweet3)
		tagged_tweet = pos_tag(cleaned_tweet3)
		return ",".join([word for word,pos in tagged_tweet if pos == 'NNP' or pos == 'NN'])
	else:
		return ","

def extractSentiment(tweet):
	if( len(tweet.split()) > 0 ):
		sid = SentimentIntensityAnalyzer()
		ss = sid.polarity_scores(tweet)
		return str(ss['compound'])
	else:
		return "0"


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
				tweetText = json_tweet['text'].encode('utf-8') 
				nouns =  extractNouns(tweetText)
				sentimentScore = extractSentiment(tweetText)
				updateTime = time.strftime('%Y-%m-%d %H:%M:%S')
				cur.execute("INSERT INTO tweets_analytics (ID,UPDATE_TIME,TEXT,NOUNS,SENTIMENT) VALUES (%s,%s,%s,%s,%s)",
						(json_tweet['id'], updateTime, tweetText, nouns, sentimentScore))
				conn.commit()
				print("\nINSERTED INTO tweets_analytics\n")
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
