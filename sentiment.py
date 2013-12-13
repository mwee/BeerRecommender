import urllib, urllib2
import json

def get_sentiment(sentence):
	url ='http://sentiment.vivekn.com/api/text/'
	values = {'txt' : sentence}

	data = urllib.urlencode(values)
	req = urllib2.Request(url, data)
	response = urllib2.urlopen(req)

	# print response.geturl()
	# print response.info()
	sentiment_response = response.read()
	# print sentiment

	json_sent = json.loads(sentiment_response)
	confidence = json_sent['result']['confidence']
	sentiment = json_sent['result']['sentiment']

	return sentiment, confidence

def scale_sentiment(sentiment, confidence):
	if sentiment == 'Neutral':
		return 3
	else if sentiment == 'Positive':
		if confidence > 60:
			return 5
		else:
			return 4
	else:
		if confidence > 60:
			return 1
		else:
			return 2
