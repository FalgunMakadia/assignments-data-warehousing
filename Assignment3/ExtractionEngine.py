from tweepy import OAuthHandler
from tweepy import API
from tweepy import Cursor
import re
import json

ACCESS_TOKEN = "1359026656229294080-M1WvPqJ1L2iy1yKRxcr6m63JfXULOP"
ACCESS_TOKEN_SECRET = "gaUq4iUS3bIuUkm7bAerzXSqvn05bjV9KsIRayeuaukHF"
API_KEY = "h85olcUYAbQ63Dj2CCMQJNOBk"
API_KEY_SECRET = "nqyQyvUburQgamRUZ1TfbJLEHzxGPpHwjieApr5TDrjilqjogW"

class AuthenticateTwitter():

	def authenticate_twitter(self):
		auth = OAuthHandler(API_KEY ,API_KEY_SECRET)
		auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
		return auth

class TwitterAPI():
	def __init__(self):
		self.auth = AuthenticateTwitter().authenticate_twitter()
		self.twitter_api = API(self.auth, wait_on_rate_limit=True)

	def get_and_store_tweets(self, count):
		# Get Tweets
		date_since = "2020-01-01"
		keywords = 'covid OR emergency OR immune OR vaccine OR flu OR snow'
		tweets = Cursor(self.twitter_api.search, q=keywords, lang="en", since=date_since, tweet_mode="extended").items(count)
		count = 0
		
		
		for n in range(1, 4001):
			for i in tweets:
					with open('tweetCollection'+str((count//100)+1)+'.txt', 'a+') as file:
						count+=1

						if i.full_text.startswith('RT'):
							rtStatus = i.retweeted_status
							text = rtStatus._json["full_text"]
						else:
							text = i.full_text

						data = {
							"tweet_id": i.id_str,
							"tweet_text": text,
							"tweet_lang": i.lang,
							"user_id": i.user.id_str,
							"user_name": i.user.name,
							"user_desc": i.user.description,
							"user_followers_count": i.user.followers_count,
							"friends_count": i.user.friends_count,
							"favourites_count": i.user.favourites_count,
							"statuses_count": i.user.statuses_count,
							"time": str(i.created_at),
							"location": i.user.location if i.user.location else 'Undefined'
						}
						file.write(json.dumps(data) + "\n")
						print(str(count)+' tweet(s) wrote!')
						break

		
if __name__ == '__main__':

	api = TwitterAPI()
	api.get_and_store_tweets(4000)