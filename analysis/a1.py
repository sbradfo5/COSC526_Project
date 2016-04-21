from collections import defaultdict
from pyspark import SparkContext, SparkConf
from os import listdir

import random

# Globals
candidates = ['Ted Cruz', 'Marco Rubio', 'Donald Trump', 'Bernie Sanders', \
	'Hillary Clinton', 'John Kasich', 'Ben Carson', 'Roque De La Fuente']

republicans = ['Ted Cruz', 'Marco Rubio', 'Donald Trump', 'John Kasich', 'Ben Carson']
democrats = ['Hillary Clinton', 'Bernie Sanders', 'Roque De La Fuente']

# Function Definitions

# Convert lines of the file to a tuple using eval
def line2tuple(line):
	# eval is fine here, because we control the input
	return eval(line)

# create (key, value) pair
def map_user_to_data(tup):
	user = tup[0]
	data = (tup[1], tup[2], tup[3], tup[4])
	return (user, data)

# Function for getting a user's candidate ratings
def rate_candidates(pair, candidates):
	candidateCountList = {}
	for candidate in candidates:
		candidateCountList[candidate] = 0
	for tweet in pair[1]:
		c = tweet[2]
		s = tweet[3]
		if s == 'negative':
			candidateCountList[c] -= 1
		else:
			candidateCountList[c] += 1
	return (pair[0], candidateCountList)

# Function for merging (user, listOfDictionaries) pairs into (user, dictionary)
def merge_dictionaries(userTup):
	user = userTup[0]
	dictionaryList = userTup[1]
	
	dd = defaultdict(list)
	for d in dictionaryList:
		for key, value in d.iteritems():
			dd[key].append(value)

	sumd = {}
	for key, value in dd.iteritems():
		sumd[key] = sum(value)

	return (user, sumd)

def get_preferred_candidate(userTup):
	user = userTup[0]
	ratings = userTup[1]
	
	maxRating = max(ratings.values())

	if maxRating <= 0:
		return ('nobody', 1)

	possibleCandidates = [c for c, r in ratings.iteritems() if r == maxRating] 
		
	if len(possibleCandidates) > 1:
		candidate = random.choice(possibleCandidates)
	else:
		candidate = possibleCandidates[0]

	return (candidate, 1)

def get_most_disliked_candidate(userTup):
	user = userTup[0]
	ratings = userTup[1]

	minRating = min(ratings.values())

	if minRating >= 0:
		return ('nobody', 1)

	possibleCandidates = [c for c, r in ratings.iteritems() if r == minRating]

	if len(possibleCandidates) > 1:
		candidate = random.choice(possibleCandidates)
	else:
		candidate = possibleCandidates[0]

	return (candidate, 1)

def determine_party(candidateCountPair):
	candidate = candidateCountPair[0]
	count = candidateCountPair[1]
	
	if candidate in republicans:
		party = 'republican'
	elif candidate in democrats:
		party = 'democrat'
	else:
		party = 'none'

	return (party, count)

def only_republicans(candidateRatingsDict):
	d = {}
	for candidate, rating in candidateRatingsDict.iteritems():
		if candidate in republicans:
			d[candidate] = rating
	return d;

def only_democrats(candidateRatingsDict):
	d = {}
	for candidate, rating in candidateRatingsDict.iteritems():
		if candidate in democrats:
			d[candidate] = rating

	return d;

def only_two(candidateRatingsDict):
	d = {}
	for candidate, rating in candidateRatingsDict.iteritems():
		if candidate in ['Hillary Clinton', 'Donald Trump']:
			d[candidate] = rating
	return d;
 
# Set up spark object
conf = SparkConf().setAppName('a1')
sc = SparkContext(conf=conf)
sc.setCheckpointDir('checkpoints')

# Create empty rdd
overallUserCandidateRatings = sc.parallelize([])

tweetResultFiles = listdir('Results')
tweetResultFiles = map(lambda f: 'Results/' + f, tweetResultFiles)

#numUsers = 0
listOfRdds = []
for tweetResultFile in tweetResultFiles:
	# Load text file into rdd and remove duplicate lines
	allLines = sc.textFile(tweetResultFile).distinct()
	# throw away tweets where we don't know the subject
	allTuples = allLines.map(line2tuple).filter(lambda tup: tup[3] != 'unknown')

	# create a (key, value) pairs where key = user and 
	# value = ( retweet, timestamp, subject, sentiment )
	userDataPairs = allTuples.map(map_user_to_data)

	# Create (key, value) pairs where key = user and
	# value = [ (candidate1, rating1), (candidate2, rating2), ... ]
	userAllDataPairs = userDataPairs.groupByKey().sortByKey().mapValues(list)
	userCandidateRatings = userAllDataPairs.map(lambda p: rate_candidates(p, candidates))

	#	numUsers += userAllDataPairs.keys().count()

	listOfRdds.append(userCandidateRatings)

overallUserCandidateRatings = sc.union(listOfRdds)

groupedUserCandidateRatings = overallUserCandidateRatings.groupByKey().mapValues(list)

finalUserCandidateRatings = groupedUserCandidateRatings.map(merge_dictionaries)

numUsers = finalUserCandidateRatings.keys().count()

# first graph: preferred candidate x, user base percentage y
preferredCounts = finalUserCandidateRatings.map(get_preferred_candidate)

preferredCountsFinal = preferredCounts.groupByKey().mapValues(list).mapValues(len)

percentPreferred = preferredCountsFinal.filter(lambda p: p[0] != 'nobody').mapValues(lambda x: (float(x) / numUsers) * 100)
print 'all candidates percent user base ' + str(percentPreferred.collect())
print ''

# second graph: most disliked candidate x, user base percentage y
hateCounts = finalUserCandidateRatings.map(get_most_disliked_candidate)
hateCountsFinal = hateCounts.groupByKey().mapValues(list).mapValues(len)

percentHated = hateCountsFinal.filter(lambda p: p[0] != 'nobody').mapValues(lambda x: (float(x) / numUsers) * 100)
print 'all disliked candidates percent user base ' + str(percentHated.collect())

print 'User count: ' + str(numUsers)

# third graph: political party x, user base percentage y
partyCounts = preferredCountsFinal.map(determine_party)

partyCountsFinal = partyCounts.groupByKey().mapValues(list).mapValues(sum)
percentPreferred = partyCountsFinal.filter(lambda p: p[0] != 'none').mapValues(lambda x: (float(x) / numUsers) * 100)

print 'political party x, user base y ' + str(percentPreferred.collect())

# fourth graph: political party dislike, user base percentage y
partyHateCounts = hateCountsFinal.map(determine_party)
partyHateCountsFinal = partyHateCounts.groupByKey().mapValues(list).mapValues(sum)
percentHated = partyHateCountsFinal.filter(lambda p: p[0] != 'none').mapValues(lambda x: (float(x) / numUsers) * 100)

print 'political party x, percent user base dislike ' + str(percentHated.collect())

# fifth graph: only republicans x, user base percentage y

republicanCandidateRatings = finalUserCandidateRatings.mapValues(only_republicans)

preferredCounts = republicanCandidateRatings.map(get_preferred_candidate)

preferredCountsFinal = preferredCounts.groupByKey().mapValues(list).mapValues(len)

percentPreferred = preferredCountsFinal.filter(lambda p: p[0] != 'nobody').mapValues(lambda x: (float(x) / numUsers) * 100)

print 'republicans x, percent users y ' + str(percentPreferred.collect())

# sixth graph: only democrats x, user base percentage y
democratCandidateRatings = finalUserCandidateRatings.mapValues(only_democrats)

preferredCounts = democratCandidateRatings.map(get_preferred_candidate)

preferredCountsFinal = preferredCounts.groupByKey().mapValues(list).mapValues(len)

percentPreferred = preferredCountsFinal.filter(lambda p: p[0] != 'nobody').mapValues(lambda x: (float(x) / numUsers) * 100)

print 'democrats x, percent users y ' + str(percentPreferred.collect())

# seventh graph: best two (our data set suggests Trump vs Hillary)
bestTwoRatings = finalUserCandidateRatings.mapValues(only_two)

preferredCounts = bestTwoRatings.map(get_preferred_candidate)

preferredCountsFinal = preferredCounts.groupByKey().mapValues(list).mapValues(len)

percentPreferred = preferredCountsFinal.filter(lambda p: p[0] != 'nobody').mapValues(lambda x: (float(x) / numUsers) * 100)

print 'best two x, percent users y ' + str(percentPreferred.collect())
