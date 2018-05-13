import twitter
from google.cloud import datastore
import datetime

def create_client(project_id):
    return datastore.Client(project_id)

def add_tweet(client, tweet, user_name, created_at, tid,
              urls, Hashtags, country, place):
    key = client.key('tweet', tid)

    task = datastore.Entity(key)

    task.update({
        'tid': tid,
        'Actualtweet': tweet,
        'userName': user_name,
        'Hashtags': Hashtags,
        'urls': urls,
        'country': country,
        'City': place,
        'created_at': created_at,
    })
    client.put(task)

    return task.key

'''
api = twitter.Api(consumer_key=consumer_key,
                  consumer_secret=consumer_secret,
                  access_token_key=access_token,
                  access_token_secret=access_token_secret)
'''

consumer_key = 'NRakIcmUOCWbogbPsnHySFK4Q'
consumer_secret = 'owhDurASz56POtXpzG8mnZFzkdX5Hb2XV6O3aoHktbmHvyQACs'
access_token = '3039814004-mjCJdEaYjzQsc18CLr8kx1cKcjucoy7iFxaU0Pt'
access_token_secret = 'MmQHM6ULTRbpPAuMzwDjNbuAAq5Rlk0juB3ksCcKoB95P'
PROJECT_ID = 'cloud-202303'
'''
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)
'''

gCloudClient = create_client(PROJECT_ID)

api = twitter.Api(consumer_key=consumer_key,
                  consumer_secret=consumer_secret,
                  access_token_key=access_token,
                  access_token_secret=access_token_secret)

'''
public_tweets = []
# grab tweets from the home timeline of the auth'd account.
try:
  if last_id:
    public_tweets = api.GetHomeTimeline(count=200, since_id=last_id)
  else:
    public_tweets = api.GetHomeTimeline(count=20)
    logging.warning("Could not get last tweet id from datastore.")
except Exception as e:
  logging.warning("Error getting tweets: %s", e)
'''
tweetSet = set([])
#'San%20Jose', 'oakland', 'berkley', 'Palo%20Alto', 'Mountain%20View', 'cupertino',
#'fremont', 'sunnyvale', 'sacramento'
cities = ['Palo%20Alto', 'Mountain%20View', 'cupertino']
for city in cities:
    #'San Francisco #hiring OR #resume OR #JobSearch OR #Hiring OR #Careers OR #NowHiring'
    query ='q=' + city + '%20%23hiring%20OR%20%23resume%20OR%20%23JobSearch%20OR%20%23Hiring%20OR%20%23Careers%20OR%20%23NowHiring'
    maxDate = datetime.datetime.strptime('2018-04-30', '%Y-%m-%d')
    startDate = datetime.datetime.strptime('2018-04-22', '%Y-%m-%d')
    totalUniqueProcessedTweets = 0
    while startDate < maxDate:
        searched_tweets = []
        max_tweets = 100
        tmpStDate = startDate
        tmpEndDate = tmpStDate + datetime.timedelta(days = 1)
        print "Query Fired: ",  query+"%20since%3A" + str(tmpStDate.date()) + "%20until%3A" + str(tmpEndDate.date()) + "&result_type=recent&count=100"
        uniqueTweetsFound = set([])
        currUniqueTweetCount = 0
        while len(searched_tweets) < max_tweets:
            count = max_tweets - len(searched_tweets)
            try:
                #new_tweets = api.search(q=query, count=count, max_id=str(last_id - 1)
                #                        ,include_entities = True, tweet_mode='extended')
                new_tweets = api.GetSearch(raw_query=query+"%20since%3A" + str(tmpStDate.date()) + "%20until%3A" + str(tmpEndDate.date()) + "&result_type=recent&count=100")
                if not new_tweets:
                    break
                searched_tweets.extend(new_tweets)
                uniqueTweetsFound = uniqueTweetsFound.union(set(map(lambda x: x.id, searched_tweets)))
                if len(uniqueTweetsFound) > currUniqueTweetCount:
                    currUniqueTweetCount = len(uniqueTweetsFound)
                else:
                    break
                last_id = new_tweets[-1].id
            except Exception as e:
                # depending on TweepError.code, one may want to retry or wait
                # to keep things simple, we will give up on an error
                 print("Error getting tweets: %s", e)
                 break
        startDate = startDate + datetime.timedelta(days = 1)
        print "Tweets Fetched " , len(searched_tweets)
        for count, tweet in enumerate(searched_tweets):
            #import pdb; pdb.set_trace()
            text = tweet.text
            user = tweet.user.screen_name
            created_at = tweet.created_at
            tid = tweet.id
            tweetSet.add(tid)
            country, place = "", ""
            if tweet._json.get('place', None) is not None:
                country = tweet._json['place'].get('country', None)
                place = tweet._json['place'].get('name', None)


            #urls = tweet.urls
            print add_tweet(gCloudClient, text, user, created_at, tid,
                            map(lambda x: x['expanded_url'],
                            tweet._json['entities']['urls']) ,
                            map(lambda x: x['text'],
                            tweet._json['entities']['hashtags']), country, place)
        print 'Distinct Tweets processed till now is: ', len(tweetSet)
