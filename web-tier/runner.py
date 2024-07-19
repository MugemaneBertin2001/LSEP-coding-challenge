import subprocess
from flask import Flask, jsonify
import psycopg2
from config import DB_CONFIG

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
    try:
        conn = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
        )
        cur = conn.cursor()
        cur.execute("SELECT * FROM tweets")
        rows = cur.fetchall()

        tweets = []
        for row in rows:
            tweet = {
                'tweet_id': row[0],
                'created_at': row[1].isoformat() if row[1] else None,
                'tweet_text': row[2],
                'source': row[3],
                'truncated': row[4],
                'in_reply_to_status_id': row[5],
                'in_reply_to_user_id': row[6],
                'user_id': row[7]
            }
            tweets.append(tweet)

        cur.close()
        conn.close()

        return jsonify(tweets)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
