import nltk

fh = open('trainingset.txt', 'r')

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

classifier = nltk.NaiveBayesClassifier.train(training)

print nltk.classify.accuracy(classifier, testing)
