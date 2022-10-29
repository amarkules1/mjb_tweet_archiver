import pandas as pd
import requests
import json
from requests.structures import CaseInsensitiveDict
from sqlalchemy import create_engine


def main():
    print('starting')
    secret_sauce = json.load(open('secret_sauce.json',))
    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    headers["Authorization"] = "Bearer " + secret_sauce['twitter_bearer_token']
    tweets_url = "https://api.twitter.com/2/users/412833880/tweets?max_results=50&tweet.fields=created_at,id,referenced_tweets,text&media.fields=url"
    response = requests.get(tweets_url, headers=headers)
    live_tweets = tweets_to_df(response)
    conn = create_engine(secret_sauce['db_conn_string']).connect()
    existing_tweets = pd.read_sql("select * from michael_burry_tweets", conn)
    new_tweets = live_tweets[~live_tweets['tweet_id'].isin(existing_tweets['tweet_id'])]
    print(new_tweets)
    for row, tweet in new_tweets.iterrows():
        conn.execute(f"insert into michael_burry_tweets(tweet_id, tweet_time, tweet_text) "
                     f"values('{tweet['tweet_id']}', '{tweet['tweet_time']}', '{tweet['tweet_text']}')")
    print("complete")




def tweets_to_df(tweets_response):
    data = json.loads(tweets_response.text)['data']
    return pd.DataFrame(data={'tweet_id': [t['id'] for t in data],
                            'tweet_time': [t['created_at'] for t in data],
                            'tweet_text': [t['text'] for t in data]})


if __name__ == '__main__':
    # execute only if run as the entry point into the program
    main()
