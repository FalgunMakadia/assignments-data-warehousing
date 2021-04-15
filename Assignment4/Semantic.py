import json
import re
from prettytable import PrettyTable
import math
import csv

total_documents = 5500
documents_containing_flu = 0
documents_containing_snow = 0
documents_containing_cold = 0
n_by_df_flu = 0
n_by_df_snow = 0
n_by_df_cold = 0
log_10_flu = 0
log_10_snow = 0
log_10_cold = 0
cold_count = 0
document_count = 0
find_document_count = 0
document_number_with_max_freq = 0
document_with_max_freq = ""
f_by_m_list = []



# Problem 3.a

x = PrettyTable()
x.field_names = ["Search Query", "Document containing term (df)", "N/df", "Log10(N/df)"]

tweet_file = open("cleanTweets.txt","r")

with open('SemanticOutput1.csv','w') as semantic:
	csv_writer = csv.writer(semantic,lineterminator = '\n')
	csv_writer.writerow(['Total Documents', total_documents])
	csv_writer.writerow([])
	csv_writer.writerow(['Search Query', 'Document containing term (df)', 'Total Documents(N)/Number of documents term appeared (df)', 'Log10(N/df)'])
	csv_writer.writerow([])

	for frag in tweet_file:

		tweet = json.loads(frag)
		tweet_text = tweet["tweet_text"]
		tweet_text = tweet_text.lower()

		tokenized_tweet = tweet_text.split(" ")

		if "flu" in tokenized_tweet:
			documents_containing_flu += 1
		elif "snow" in tokenized_tweet:
			documents_containing_snow += 1
		elif "cold" in tokenized_tweet:
			documents_containing_cold += 1

	n_by_df_flu = round(total_documents/documents_containing_flu, 2)
	n_by_df_snow = round(total_documents/documents_containing_snow, 2)
	n_by_df_cold = round(total_documents/documents_containing_cold, 2)

	log_10_flu = round(math.log10(n_by_df_flu), 2)
	log_10_snow = round(math.log10(n_by_df_snow), 2)
	log_10_cold = round(math.log10(n_by_df_cold), 2)

	csv_writer.writerow(["Flu", documents_containing_flu, n_by_df_flu, log_10_flu])
	csv_writer.writerow(["Snow", documents_containing_snow, n_by_df_snow, log_10_snow])
	csv_writer.writerow(["Cold", documents_containing_cold, n_by_df_cold, log_10_cold])

	x.add_row(["Flu", documents_containing_flu, n_by_df_flu, log_10_flu])
	x.add_row(["Snow", documents_containing_snow, n_by_df_snow, log_10_snow])
	x.add_row(["Cold", documents_containing_cold, n_by_df_cold, log_10_cold])

print("\n" + "Total Documents (Tweets): " + str(total_documents) + "\n")
print(x)
print("\n\n" + ">>> Output of Problem 3.a (above table) is saved as SemanticOutput1.csv" + "\n")
semantic.close()
tweet_file.close()



# Problem 3.b and 3.c

def sortListOfLists(sub_li):
    return(sorted(sub_li, key = lambda x: x[0]))  

tweet_file = open("cleanTweets.txt","r")

with open('SemanticOutput2.csv','w') as semantic:

	search_term = "cold"

	title = "Cold appeared in " + str(documents_containing_cold) + " documents"
	csv_writer = csv.writer(semantic,lineterminator = '\n')
	csv_writer.writerow(['Term', search_term])
	csv_writer.writerow([])
	csv_writer.writerow([title, 'Total Words (m)', 'Frequency (f)'])
	csv_writer.writerow([])

	for frag in tweet_file:

		cold_count = 0
		document_count += 1
		current_document = "Document #" + str(document_count)

		tweet = json.loads(frag)
		tweet_text = tweet["tweet_text"]
		tweet_text = tweet_text.lower()

		tokenized_tweet = tweet_text.split(" ")
		total_words_in_tweet = len(tokenized_tweet)

		if search_term in tokenized_tweet:

			for word in tokenized_tweet:
				if word == "cold":
					cold_count += 1

			f_by_m = cold_count/total_words_in_tweet
			sublist = [f_by_m, document_count]
			f_by_m_list.append(sublist)

			csv_writer.writerow([current_document, total_words_in_tweet, cold_count])
	print(">>> Output of Problem 3.b is saved as SemanticOutput2.csv" + "\n\n")

	sorted_f_by_m_list = sortListOfLists(f_by_m_list)
	document_number_with_max_freq = sorted_f_by_m_list[-1][1]

semantic.close()
tweet_file.close()


tweet_file = open("cleanTweets.txt","r")

for frag in tweet_file:
	find_document_count += 1

	if find_document_count == document_number_with_max_freq:
		document_with_max_freq = frag

		tweet = json.loads(frag)
		tweet_text = tweet["tweet_text"]
		tweet_text = tweet_text.lower()

		print("-------------------------- Output of Problem 3.c --------------------------")
		print("\n" + "Document number having highest relative frequency: " + str(document_number_with_max_freq))
		print("\n" + "Document text (tweet text) having highest relative frequency: " + tweet_text + "\n")
		print("---------------------------------------------------------------------------")

tweet_file.close()