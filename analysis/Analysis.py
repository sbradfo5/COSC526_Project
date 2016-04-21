import nltk
from nltk.corpus import twitter_samples
import os
import json
import numpy
import sys

#Functions:

def get_words_in_tweets(tweets):
		all_words = []
		for (words, sentiment) in tweets:
				all_words.extend(words)
		return all_words

def get_word_features(wordlist):
		wordlist = nltk.FreqDist(wordlist)
		word_features = wordlist.keys()
		return word_features

def extract_features(document):
		document_words = set(document)
		features = {}
		for word in word_features:
				features['contains(%s)' % word] = (word in document_words)
		return features

fh = open('trainingset.txt', 'r')

# Create candidate - index mapping for naive bayes training
candidateMap = dict([ ('Donald Trump', 0), ('Bernie Sanders', 1), ('Hillary Clinton', 2 ), \
										 ('Ted Cruz', 3), ('Marco Rubio', 4), ('John Kasich', 5), ('Ben Carson', 6), \
										 ('Roque De La Fuente', 7) ])

candidateMapRev = dict([ (0, 'Donald Trump'), (1, 'Bernie Sanders'), (2, 'Hillary Clinton'), \
												(3, 'Ted Cruz'), (4, 'Marco Rubio'), (5, 'John Kasich'), (6, 'Ben Carson'), \
												(7, 'Roque De La Fuente') ])

allFeatures = []
for line in fh:
		words = line.split()
		subject = ' '.join(words[1:])
		nums = words[0].split('@')
		intFeatures = map(int, nums)
		features = {}
		for i in range(0, 8):
				features[candidateMapRev[i]] = intFeatures[i]
		allFeatures.append( (features, subject) )
fh.close()

training = allFeatures[:len(allFeatures) / 2]
testing = allFeatures[len(allFeatures) / 2:]

subject_classifier = nltk.NaiveBayesClassifier.train(training)

fh = open('filter.txt', 'r')
labelSubjects = []
for line in fh:
		labelSubject = line.split(',')
		label = labelSubject[0].strip()
		subject = labelSubject[1].strip()
		labelSubjects.append( (label, subject) )

fh.close()

#seniment classifier
pos_tweets = twitter_samples.strings('positive_tweets.json')
neg_tweets = twitter_samples.strings('negative_tweets.json')
pos_tuples = []
neg_tuples = []
for string in pos_tweets:
		pos_tuples.append((string, 'positive'))
for string in neg_tweets:
		neg_tuples.append((string, 'negative'))

tweets = []
#filtering out words that are user names, less than 3 characters, and http addresses
for (words, sentiment) in pos_tuples + neg_tuples:
		words_filtered = [e.lower() for e in words.split() if len(e) >= 3 and e[0] != '@' and e[0:4] != 'http']
		tweets.append((words_filtered, sentiment))

word_features = get_word_features(get_words_in_tweets(tweets))

print("Training time")
training_set = nltk.classify.apply_features(extract_features, tweets)
sentiment_classifier = nltk.NaiveBayesClassifier.train(training_set)

wdir = sys.argv[1]
rdir = sys.argv[2]
filesList = os.listdir(rdir)

sortedFilesList = sorted(filesList)

print("starting actual analysis")
for fileName in sortedFilesList:
		tuples = []
		wfh = open(wdir + '/' + fileName.replace('.json','') + '_results.txt', 'w')
		rfh = open(rdir + '/' + fileName, 'r')

		allFeatures = []
		
		data = json.load(rfh)
		for tweetDict in data['tweets']:
				sfeatures = [0, 0, 0, 0, 0, 0, 0, 0]
				tweet = tweetDict['text']
				
				# Extract features to put into classifiers
				tweet = tweet.lower()
				if tweet[0:2] == 'rt':
					retweet = '1'
				else:
					retweet = '0'
				for labelSubject in labelSubjects:
						lc = tweet.count(labelSubject[0].lower())
						if lc > 0:
							if labelSubject[1] != 'democratic' and labelSubject[1] != 'ignore' and labelSubject[1] != 'republican':
										sfeatures[candidateMap[labelSubject[1]]] += lc
							elif labelSubject[1] == 'democratic':
									sfeatures[1] += lc
									sfeatures[2] += lc
							elif labelSubject[1] == 'republican':
									sfeatures[0] += lc
									sfeatures[3] += lc
									sfeatures[4] += lc
									sfeatures[5] += lc
									sfeatures[6] += lc
									sfeatures[7] += lc
		
				subjFeatures = {}
				for i in range(0, 8):
						subjFeatures[candidateMapRev[i]] = sfeatures[i]

				subject = subject_classifier.classify(subjFeatures)
				sentiment = sentiment_classifier.classify(extract_features(tweet.split()))
				result = (tweetDict['screen_name'], retweet, tweetDict['created_at'], subject, sentiment)
				tuples.append(result)

		for val in tuples:
				wfh.write(str(val) + "\n")

		wfh.close()
		rfh.close()
		print("Finished a file!")

