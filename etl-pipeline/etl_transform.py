import pandas as pd


def transform_user(user_data):
    transformed_user = {
        'user_id': user_data['user']['id_str'],
        'name': user_data['user']['name'],
        'screen_name': user_data['user']['screen_name'],
        'location': user_data['user']['location'],
        'url': user_data['user']['url'],
        'description': user_data['user']['description'],
        'protected': user_data['user']['protected'],
        'followers_count': user_data['user']['followers_count'],
        'friends_count': user_data['user']['friends_count'],
        'listed_count': user_data['user']['listed_count'],
        'created_at': user_data['user']['created_at'],
        'favourites_count': user_data['user']['favourites_count'],
        'lang': user_data['user']['lang'],
        'verified': user_data['user']['verified'],
        'statuses_count': user_data['user']['statuses_count'],
        'profile_image_url': user_data['user']['profile_image_url']
    }
    return transformed_user

def transform_tweet(tweet_data):
    transformed_tweet = {
        'tweet_id': tweet_data['id'],
        'created_at': tweet_data['created_at'],
        'text': tweet_data['text'],
        'source': tweet_data['source'],
        'truncated': tweet_data['truncated'],
        'in_reply_to_status_id': tweet_data['in_reply_to_status_id_str'],
        'in_reply_to_user_id': tweet_data['in_reply_to_user_id_str'],
        'user_id': tweet_data['user']['id_str']
    }
    return transformed_tweet

def transform_hashtags(tweet_data):
    tweet_id = tweet_data['id_str']
    hashtags = tweet_data['entities']['hashtags']
    transformed_hashtags = [{'tweet_id': tweet_id, 'hashtag': hashtag['text']} for hashtag in hashtags]
    return transformed_hashtags

def transform_urls(tweet_data):
    tweet_id = tweet_data['id_str']
    urls = tweet_data['entities']['urls']
    transformed_urls = [{'tweet_id': tweet_id, 'url': url['expanded_url']} for url in urls]
    return transformed_urls

def transform_data(extracted_df):
    transformed_users = []
    transformed_tweets = []
    transformed_hashtags = []
    transformed_urls = []

    for index, row in extracted_df.iterrows():
        transformed_user = transform_user(row)
        transformed_users.append(transformed_user)
        transformed_tweet = transform_tweet(row)
        transformed_tweets.append(transformed_tweet)
        transformed_hashtags.extend(transform_hashtags(row))
        transformed_urls.extend(transform_urls(row))
    
    users_df = pd.DataFrame(transformed_users)
    tweets_df = pd.DataFrame(transformed_tweets)
    hashtags_df = pd.DataFrame(transformed_hashtags)
    urls_df = pd.DataFrame(transformed_urls)
    
    return users_df, tweets_df, hashtags_df, urls_df