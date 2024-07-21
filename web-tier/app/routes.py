import os
import re
import sys
from flask import Blueprint, request, jsonify
import subprocess
from psycopg2 import extras
from .config import connect_to_db, DB_CONFIG

main = Blueprint('main', __name__)

@main.route('/')
def index():
    return jsonify({"message": "Welcome to the ETL Flask App!"})



@main.route('/run_etl', methods=['GET'])
def run_etl():
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        script_path = os.path.join(current_dir, '..', '..', 'etl-pipeline', 'app.py')
        python_executable = sys.executable
        result = subprocess.run([python_executable, script_path], capture_output=True, text=True)
        if result.returncode != 0:
            return jsonify({"error": result.stderr}), 500
        return jsonify({"message": "ETL process completed successfully!"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
    

@main.route('/q2', methods=['GET'])
def get_tweets():
    user_id = request.args.get('user_id')
    phrase = request.args.get('phrase')
    hashtag = request.args.get('hashtag')
    tweet_type = request.args.get('type') 

    query = """
        SELECT 
            t.tweet_id,
            t.created_at,
            t.text AS tweet_text,
            t.source,
            t.truncated,
            t.in_reply_to_status_id,
            t.in_reply_to_user_id,
            u.user_id,
            u.name AS user_name,
            u.screen_name AS user_screen_name,
            u.location AS user_location,
            u.url AS user_url,
            u.description AS user_description,
            u.protected AS user_protected,
            u.followers_count AS user_followers_count,
            u.friends_count AS user_friends_count,
            u.listed_count AS user_listed_count,
            u.created_at AS user_created_at,
            u.favourites_count AS user_favourites_count,
            u.lang AS user_lang,
            u.verified AS user_verified,
            u.statuses_count AS user_statuses_count,
            u.profile_image_url AS user_profile_image_url,
            array_agg(DISTINCT th.hashtag) AS hashtags,
            array_agg(DISTINCT tu.url) AS urls
        FROM 
            tweets t
        LEFT JOIN 
            users u ON t.user_id = u.user_id
        LEFT JOIN 
            tweet_hashtags th ON t.tweet_id = th.tweet_id
        LEFT JOIN 
            tweet_urls tu ON t.tweet_id = tu.tweet_id
    """
    
    conditions = []
    params = []

    if user_id:
        conditions.append("t.user_id = %s")
        params.append(user_id)
    
    if phrase:
        conditions.append("t.text LIKE %s")
        params.append(f'%{phrase}%')
    
    if hashtag:
        conditions.append("EXISTS (SELECT 1 FROM tweet_hashtags th WHERE th.tweet_id = t.tweet_id AND th.hashtag = %s)")
        params.append(hashtag)
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    query += " GROUP BY t.tweet_id, u.user_id;"

    try:
        conn = connect_to_db(DB_CONFIG)
        cursor = conn.cursor(cursor_factory=extras.DictCursor)
        cursor.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        tweets = [dict(row) for row in rows]
        
        if tweet_type:
            if tweet_type == 'reply':
                tweets = [tweet for tweet in tweets if tweet['in_reply_to_status_id'] is not None]
            elif tweet_type == 'retweet':
                tweets = [tweet for tweet in tweets if tweet['tweet_text'].startswith('RT @')]
            elif tweet_type == 'both':
                tweets = [tweet for tweet in tweets if tweet['in_reply_to_status_id'] is not None and tweet['tweet_text'].startswith('RT @')]

        if phrase:
            pattern = re.compile(re.escape(phrase))
            for tweet in tweets:
                tweet_text = tweet.get('tweet_text', '')
                matches = list(pattern.finditer(tweet_text))
                tweet['phrase_matches'] = len(matches)
        
        return jsonify(tweets)
    
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": "An error occurred while retrieving tweets."+ e}), 500