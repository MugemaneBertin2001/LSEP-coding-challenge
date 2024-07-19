import subprocess
from flask import Flask, jsonify
import psycopg2
from config import DB_CONFIG, connect_to_db

app = Flask(__name__)

@app.route('/')
def index():
    return jsonify({"message": "Welcome to the ETL Flask App!"})

@app.route('/run_etl', methods=['GET'])
def run_etl():
    try:
        result = subprocess.run(['python', '../etl-pipeline/app.py'], capture_output=True, text=True)
        if result.returncode != 0:
            return jsonify({"error": result.stderr}), 500
        return jsonify({"message": "ETL process completed successfully!"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/q', methods=['GET'])
def get_tweets():
    conn = connect_to_db(DB_CONFIG)
    if conn is None:
        return jsonify({"error": "Database connection failed."}), 500

    try:
        cur = conn.cursor()
        query = """
        SELECT
            t.tweet_id,
            t.created_at,
            t.text,
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
            ARRAY_AGG(DISTINCT th.hashtag) AS hashtags,
            ARRAY_AGG(DISTINCT tu.url) AS urls
        FROM tweets t
        JOIN users u ON t.user_id = u.user_id
        LEFT JOIN tweet_hashtags th ON t.tweet_id = th.tweet_id
        LEFT JOIN tweet_urls tu ON t.tweet_id = tu.tweet_id
        GROUP BY t.tweet_id, u.user_id;
        """
        cur.execute(query)
        rows = cur.fetchall()

        tweets = []
        for row in rows:
            tweet = {
                'tweet_id': row[0],
                'created_at': row[1].isoformat(),
                'text': row[2],
                'source': row[3],
                'truncated': row[4],
                'in_reply_to_status_id': row[5],
                'in_reply_to_user_id': row[6],
                'user': {
                    'user_id': row[7],
                    'user_name': row[8],
                    'user_screen_name': row[9],
                    'user_location': row[10],
                    'user_url': row[11],
                    'user_description': row[12],
                    'user_protected': row[13],
                    'user_followers_count': row[14],
                    'user_friends_count': row[15],
                    'user_listed_count': row[16],
                    'user_created_at': row[17].isoformat(),
                    'user_favourites_count': row[18],
                    'user_lang': row[19],
                    'user_verified': row[20],
                    'user_statuses_count': row[21],
                    'user_profile_image_url': row[22]
                },
                'hashtags': row[23],
                'urls': row[24]
            }
            tweets.append(tweet)

        return jsonify(tweets)

    except Exception as e:
        print("Error during data retrieval:", e)
        return jsonify({"error": str(e)}), 500

    finally:
        if conn and not conn.closed:
            conn.close()


if __name__ == "__main__":
    app.run(debug=True)
