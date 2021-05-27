#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import tweepy as tw
import time
from datetime import datetime


# In[ ]:


def authentication(authfile):
    with open(authfile, "r") as f:
        keys = f.readlines()
    f.close()

    consumer_key = keys[0].replace("\n", "")
    consumer_secret = keys[1].replace("\n", "")

    access_token = keys[2].replace("\n", "")
    access_token_secret = keys[3].replace("\n", "")

    auth = tw.auth.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    return tw.API(auth)


# In[ ]:


api = authentication('./auth.k')


# In[ ]:


results = pd.DataFrame(columns=['username', 'location', 'following',
                                'followers', 'whentweetcreated',
                                'whenacctcreated', 'retweets',
                                'text', 'hashtags'])

# In[ ]:


dataset_name = "sputnik_biontech_sinovac"
search_words = "biontech OR sputnik OR sinovac"
language = "en"  # tr or en

program_start = time.time()
exact_program_start = datetime.now()

for i in range(6):

    run_start = time.time()
    exact_run_start = datetime.now()

    print('Run {} started '
          'at {}'.format(i, exact_run_start.strftime("%H:%M:%S")))

    tweets = tw.Cursor(api.search, q=search_words, lang=language,
                       since='2021-05-15', tweet_mode='extended').items(2500)

    tweet_list = [tweet for tweet in tweets]

    tweetsCount = 0
    for tweet in tweet_list:
        username = tweet.user.screen_name
        location = tweet.user.location
        following = tweet.user.friends_count
        followers = tweet.user.followers_count
        whentweetcreated = tweet.created_at
        whenacctcreated = tweet.user.created_at
        retweets = tweet.retweet_count
        hashtags = tweet.entities['hashtags']
        try:
            text = tweet.retweeted_status.full_text
        except AttributeError:
            text = tweet.full_text

        myTweet = [username, location, following,
                   followers,
                   whentweetcreated, whenacctcreated,
                   retweets, text, hashtags]
        results.loc[len(results)] = myTweet
        tweetsCount += 1

    run_end = time.time()
    exact_run_end = datetime.now()
    duration_run = round((run_end-run_start)/60, 2)

    print('Run {} completed'
          'at {}'.format(i, exact_run_end.strftime("%H:%M:%S")))

    print('Number of tweets scraped for run {} is {}'.format(i, tweetsCount))

    print('Time take for run '
          '{} to complete is {} mins'.format(i, duration_run))

    time.sleep(920)  # 15 mins cooldown because of error 429

timestamp = datetime.today().strftime('%Y%m%d_%H%M')

filename = './datasets/raw/' + timestamp + '_' + dataset_name + '.csv'

results.to_csv(filename, index=False)

program_end = time.time()

totaltime = program_end - program_start
print('Total time taken to scrap is {} minutes.'.format(round(totaltime)/60))


# In[ ]:


results.head()
