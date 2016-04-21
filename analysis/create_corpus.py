import sys
import json
import os
from pprint import pprint as pp

# Create candidate - index mapping for naive bayes training
candidateMap = dict([ ('Donald Trump', 0), ('Bernie Sanders', 1), ('Hillary Clinton', 2 ), \
	('Ted Cruz', 3), ('Marco Rubio', 4), ('John Kasich', 5), ('Ben Carson', 6), \
	('Roque De La Fuente', 7) ])

candidateMapRev = dict([ (0, 'Donald Trump'), (1, 'Bernie Sanders'), (2, 'Hillary Clinton'), \
	(3, 'Ted Cruz'), (4, 'Marco Rubio'), (5, 'John Kasich'), (6, 'Ben Carson'), \
	(7, 'Roque De La Fuente') ])

# Create list of (label, subject) tuples from filter.txt
fh = open('filter.txt', 'r')
labelSubjects = []
for line in fh:
	labelSubject = line.split(',')
	label = labelSubject[0].strip()
	subject = labelSubject[1].strip()
	labelSubjects.append( (label, subject) )
fh.close()

filesInDir = os.listdir('subject_training_cs526')
featureset = []
tweets = []
for trainingFile in filesInDir:
	fh = open('subject_training_cs526/' + trainingFile, 'r')
	data = json.load(fh)

	for tweetDict in data['tweets']:
		features = [0, 0, 0, 0, 0, 0, 0, 0]
		tweet = tweetDict['text']
		tweet = tweet.lower()
		for labelSubject in labelSubjects:
			lc = tweet.count(labelSubject[0].lower())
			if lc > 0:
				if labelSubject[1] != 'democratic' and labelSubject[1] != 'ignore' and labelSubject[1] != 'republican':
					features[candidateMap[labelSubject[1]]] += lc
				elif labelSubject[1] == 'democratic':
					features[1] += lc
					features[2] += lc
				elif labelSubject[1] == 'republican':
					features[0] += lc
					features[3] += lc
					features[4] += lc
					features[5] += lc
					features[6] += lc
					features[7] += lc
		featureset.append(features)
		tweets.append(tweet)
	fh.close()

i = 0
trainingset = []
trainingSetFile = open('trainingset.txt', 'w')
for features in featureset:
	maxIndex = -1
	# some logic for deciding subject
	maxVal = 0
	count = 0
	for val in features:
		if val > maxVal:
			maxVal = val
			maxIndex = count
		count += 1
	if features.count(maxVal) > 1: # tie
		subject = 'unknown'
	else:
		subject = candidateMapRev[maxIndex]

	trainingset.append( (features, subject) )
	trainingSetFile.write('@'.join(str(x) for x in features) + " " + subject + "\n")
	i += 1
