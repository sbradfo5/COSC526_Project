import tweepy
import json
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from pprint import pprint as pp

consumer_key = '27bHwdMqcA1TZ4ORWfqk8Y4Tb'
consumer_secret = 'OCccKz5sOJKC2JnPltcf7K19KnqqWRpBfj4VOXQIG2SaQNSGkG'
access_token = '1224784879-IndZyLcDRfkUZOAFgO05deOKN49Mim2e5sSbBHh'
access_secret = '69zxPMVJH7RwVYeAED56EzaGky6QfHGGt6Pl2nt1vwfyT'

class MyStreamListener(tweepy.StreamListener):
	def on_status(self, status):
		if not hasattr(self, 'tweetDict'):
			self.tweetDict = {}
			self.tweetDict['tweets'] = []

		if not hasattr(self, 'file_handle'):
			self.file_handle = open('election_tweets.json', 'w')

		if not hasattr(self, 'tweet_count'):
			self.tweet_count = 0
		else:
			self.tweet_count += 1

		pp(self.tweet_count)

		if status.lang == u'en':
			newTweet = {}
			newTweet["screen_name"] = status.user.screen_name
			newTweet["text"] = status.text
			newTweet["created_at"] = str(status.created_at)
			self.tweetDict['tweets'].append(newTweet.copy())

		if self.tweet_count == 25000:
			gauth = GoogleAuth()

			gauth.LoadCredentialsFile("mycreds.txt")
			if gauth.credentials is None:
				# Authenticate if they're not there
					gauth.LocalWebserverAuth()
			elif gauth.access_token_expired:
					# Refresh them if expired
					gauth.Refresh()
			else:
					# Initialize the saved creds
					gauth.Authorize()
			# Save the current credentials to a file
			gauth.SaveCredentialsFile("mycreds.txt")

			drive = GoogleDrive(gauth)
			self.file_handle.write(json.dumps(self.tweetDict, ensure_ascii=False).encode('utf8'))
			self.file_handle.close()
			gfile = drive.CreateFile({'title': 'election_tweets.json', 'mimeType': 'text/json', "parents": [{"kind": "drive#fileLink", "id": "0B9MBq1W8GAECbkJGaUFpMlJ5bnc"}]})
			gfile.SetContentFile('election_tweets.json')
			gfile.Upload()
			self.tweet_count = 0
			self.file_handle = open('election_tweets.json', 'w')
			self.tweetDict['tweets'][:] = []

	def on_error(self, status_code):
		if status_code == 420:
			return False

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
streamListener = MyStreamListener()
stream = tweepy.Stream(auth = auth, listener = streamListener)

tracking = []
fh = open('filter.txt', 'r')
for line in fh:
	tracking.append(line)
fh.close()

stream.filter(track=tracking)
