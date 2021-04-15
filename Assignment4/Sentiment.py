import json
import re
import string
from prettytable import PrettyTable
import csv
import time

# x = PrettyTable()
# x.field_names = ["Tweet No", "Tweet Text", "Match", "Polarity"]

regex = r'(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)'

positiveWordlist = []
with open('positiveWords.txt') as postiveFile:
    positiveWordlist = [positive.strip() for positive in postiveFile.readlines() ]

negativeWordlist = []
with open('negativeWords.txt',encoding="ISO-8859-1") as negativeFile:
    negativeWordlist = [negative.strip() for negative in negativeFile.readlines()]

positive_and_negative_words = positiveWordlist + negativeWordlist

final_tokenized = []
matched_words = []
bag_of_words_for_tweet = []
polarity = ""
polarity_count = 0
count = 0

def createBagOfWords(cleaned_text):
	bag_of_words_for_tweet = {}
	tokenized_tweet = cleaned_text.split(" ")

	for word in tokenized_tweet:
		if word in list(bag_of_words_for_tweet.keys()):
			bag_of_words_for_tweet[word] += 1
		else:
			bag_of_words_for_tweet[word] = 1

	return bag_of_words_for_tweet


tweet_file = open("cleanTweets.txt","r")

with open('SentimentOutput.csv','w') as sentiment:
	csv_writer = csv.writer(sentiment, lineterminator = '\n')
	csv_writer.writerow(['Tweet No', 'Tweet Text', 'Match', 'Polarity'])
	csv_writer.writerow([])

	print("Processing..." + "\n")
	time.sleep(2)
	print("This will take 40-60 seconds!")

	for frag in tweet_file:
	
		count += 1
		matched_words = []
		polarity_count = 0

		tweet = json.loads(frag)
		text = tweet["tweet_text"]

		lower_case = text.lower()
		cleaned_text = lower_case.translate(str.maketrans('', '', string.punctuation))

		bag_of_words_for_tweet = createBagOfWords(cleaned_text)
		
		for word in bag_of_words_for_tweet.keys():
			if word in positiveWordlist:
				matched_words.append(word)
				word_freq = bag_of_words_for_tweet[word]
				polarity_count = polarity_count + (word_freq * 1)

		for word in bag_of_words_for_tweet.keys():
			if word in negativeWordlist:
				matched_words.append(word)
				word_freq = bag_of_words_for_tweet[word]
				polarity_count = polarity_count + (word_freq * -1)
		
		for word in bag_of_words_for_tweet.keys():
			if word not in positive_and_negative_words:
				word_freq = bag_of_words_for_tweet[word]
				polarity_count = polarity_count + (word_freq * 0)

		if(polarity_count > 0):
			polarity = "positive"
		elif(polarity_count == 0):
			polarity = "neutral"
		elif(polarity_count < 0):
			polarity = "negative"

		temp = ","
		if len(matched_words) > 0:
			match = temp.join(matched_words)
		else:
			match = "No Match"

		data = {
			"tweet_no": count,
			"tweet_text": cleaned_text,
			"match": match,
			"polarity": polarity
		}

		csv_writer.writerow([data['tweet_no'],data['tweet_text'],data['match'],data['polarity']])

		# x.add_row([data["tweet_no"], "Tweet Text", data["match"], data["polarity"]])

# print(x)
print("\n\n" + "Output of Problem 2 is saved in SentimentOutput.csv" + "\n")
sentiment.close()