# -*- coding: utf-8 -*-
"""
Created on Mon May 19 09:23:32 2014

@author: CONTJ073
"""

# Import module
from sys import argv, stderr

def stream_geo_tweets(file_name, query_term, n_total_tweet):

    # Import modules
    from csv import writer
    from tweepy import OAuthHandler, StreamListener, streaming
    
    # Authorize connection to Twitter
    auth = OAuthHandler('NhRbHxhDD12d7lsgwTwC6A', 'XdbhJTESBkhA2MBbDbANcXMpB7K4XBRehCgMANBQiQ')
    auth.set_access_token('884709397-OXJWKwfmzknOUBcaEfPe8WJAxFm5nz1V6oAnj3U6', 'gC369XBOQXha706iLwf6HgIBaLg3B8zYaBprnfyp3kEmj')
    
    # Open the file to which tweets will be written
    with open(file_name, 'wb') as f:
        writer(f).writerow(['tweet', 'latitude', 'longitude'])
    
    # Define a custom class to listen to Twitter's stream
    class CustomStreamListener(StreamListener):
        
        def __init__(self, api=None):
            super(CustomStreamListener, self).__init__()
            self.n_tweet = 0
        
        def on_status(self, status):   
            try:
                # If a tweet has coordinates
                if status.geo is not None:
                    latitude, longitude = status.geo['coordinates']
                # Otherwise, if a tweet was written by a user with coordinates
                elif status.place is not None and \
                    getattr(status, 'coordinates'):
                    longitude, latitude = status.place.coordinates[0][0]
                # If a tweet is associated with coordinates, then write it
                if 'latitude' in vars() and 'longitude' in vars():
                    with open(file_name, 'ab') as f:
                        row = [status.text.encode('UTF8'), latitude, longitude]
                        writer(f).writerow(row)
                    self.n_tweet += 1
                if self.n_tweet == n_total_tweet:
                    return False
            except Exception, e:
                print stderr, 'Encountered exception:', e
            
        def on_error(self, status_code):
            print stderr, 'Encountered error with status code:', status_code
            return True
        
        def on_timeout(self):
            print stderr, 'Timeout...'
            return True
    
    # Listen to the Twitter stream, filtering by query terms
    streaming_api = streaming.Stream(auth, CustomStreamListener(), timeout=60)
    streaming_api.filter(track=query_term)

def analyze_tweets(file_name, query_term, n_total_tweet):
    
    # Import modules
    from csv import reader
    from pandas import DataFrame
    from time import sleep
    from random import random
    
    # Initialize an empty DataFrame
    df = DataFrame({'state': [None] * n_total_tweet,
                    'u_o_h': [None] * n_total_tweet})
    
    # Iterate through all tweets
    with open(file_name, 'rb') as f:
        f.next()
        for i, row in enumerate(reader(f)):
            # Extract tweet and geo data
            tweet = row[0]
            latitude, longitude = [float(j) for j in row[1:]]
            # Tag state and determine user or hashtag
            df['state'][i] = reverse_geocode(latitude, longitude)
            df['u_o_h'][i] = user_or_hashtag(tweet, query_term)
            # Sleep for at least 200ms to avoid upsetting the Google Maps API
            sleep(0.2 + random())
    
    # Return DataFrame
    return df

def reverse_geocode(latitude, longitude):
    
    # Import module
    from pygeocoder import Geocoder

    # Perform reverse geocoding
    try:
        geo_obj = Geocoder.reverse_geocode(latitude, longitude)
    except Exception, e:
        print stderr, 'Encountered exception:', e
        return None
    
    # Declare address object
    address = geo_obj.data[0]['address_components']
    
    # Iterate through the elements of the address object
    for i in xrange(len(address)):
        #  Return the current element if it contains the state abbreviation
        if 'administrative_area_level_1' in address[i]['types']:
            return address[i]['short_name'].encode('UTF-8')

def user_or_hashtag(tweet, query_term):
    
    # Import module
    from re import search
    
    # Determine whether the mention is user, hashtag, or none
    if search('@' + query_term, tweet.lower()):
        return 'user'
    elif search('#' + query_term, tweet.lower()):
        return 'hash'
    else:
        return 'none'

def agg_by_state(df):
    
    # Import modules
    from csv import reader
    from pandas import DataFrame
    
    # Define lambda functions for aggregation
    count_user = lambda x: sum(x == 'user')
    count_hash = lambda x: sum(x == 'hash')
    count_none = lambda x: sum(x == 'none')
    count_user_hash = lambda x: (count_user(x) / count_hash(x)) \
                                if count_hash(x) > 0 else 0
    
    # Create an aggregation dictionary
    agg_dict = {'count': len, 'n_user': count_user, 'n_hash': count_hash,
                'n_none': count_none, 'user_hash': count_user_hash}

    # Perform aggregation by state
    grouped = df.groupby(by='state', as_index=False)
    df = grouped['u_o_h'].agg(agg_dict)
    
    # Load state data
    with open('J:\WDPRO\BPM\us_states.csv', 'r') as f:
        states = {}
        for abbrev, name in reader(f):
           states[abbrev] = name
    states = DataFrame(data=states.values(), index=states.keys())
    
    # Restrict results to US states
    df = df[df.state.isin(states.index)]
    
    # Join the full state name
    df = df.join(states, on='state')
    df.rename(columns={0: 'state_name'}, inplace=True)
    df['state_name'] = [i.lower() for i in df['state_name']]
    
    # Rank the states
    df['count_rank'] = df['count'].rank(ascending=False)
    
    # Return DataFrame
    return df
    
def main(query_term):

    # Declarations
    file_name = 'geo_twitter.csv'
    n_total_tweet = 20
    
    # Stream tweets with coordinates
    stream_geo_tweets(file_name, query_term, n_total_tweet)
    
    # Analyze tweets (geocode and identify hashtag/user references)
    df = analyze_tweets(file_name, query_term, n_total_tweet)
    
    # Aggregate results by state
    df = agg_by_state(df)

    # Save results to a CSV file
    df.to_csv(path_or_buf='geo_twitter_stats.csv', index=None)
    
if __name__ == '__main__':

    main(argv[1])
