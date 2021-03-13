import json
import re
from pymongo import MongoClient

class FiltrationEngine():

	def filterAndSave(self):

		mongoURI = "mongodb+srv://sunny:sunnysunny@tweets.zh3nm.mongodb.net/myMongoTweet?retryWrites=true&w=majority"
		conn = MongoClient(mongoURI)
		db = conn.myMongoTweet
		collection = db.tweets
		regex = r'(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)'

		for i in range(1, 41):
			file = open("tweetCollection"+str(i)+".txt","r")
			for frag in file:
				tweet = json.loads(frag)
				tweet["tweet_text"] = re.sub(regex, '', tweet["tweet_text"]) 
				tweet["tweet_text"] = tweet["tweet_text"].strip()
				tweet["location"] = re.sub(regex, '', tweet["location"]) 
				tweet["location"] = tweet["location"].strip()
				tweet["user_desc"] = re.sub(regex, '', tweet["user_desc"]) 
				tweet["user_desc"] = tweet["user_desc"].strip()
				
				tweet = {
                	"tweet_id": tweet["tweet_id"],
                	"tweet_text": tweet["tweet_text"],
                	"tweet_lang": tweet["tweet_lang"],
                	"user_id": tweet["user_id"],
                	"user_name": tweet["user_name"],
                	"user_desc": tweet["user_desc"],
                	"user_followers_count": tweet["user_followers_count"],
                	"friends_count": tweet["friends_count"],
                	"favourites_count": tweet["favourites_count"],
                	"statuses_count": tweet["statuses_count"],
                	"time": tweet["time"],
                	"location": tweet["location"]
            	}

				insertionOutput = collection.insert_one(tweet)
				print(insertionOutput)

			file.close()

		print("\nAll Tweets successfully stored in MongoDB!")

if __name__ == '__main__':

	filtration_engine = FiltrationEngine()
	filtration_engine.filterAndSave()