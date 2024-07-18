import psycopg2
from psycopg2 import sql
from db_utils import reconnect_if_closed
from config import DB_CONFIG

def load_users(conn, users_data):
    """
    Load users data into the 'users' table in PostgreSQL.

    Args:
    - conn (psycopg2.extensions.connection): PostgreSQL database connection object.
    - users_data (pd.DataFrame): DataFrame containing transformed users data to be loaded.

    Returns:
    - None

    Raises:
    - psycopg2.Error: If there is an error during the SQL execution.
    """
    conn = reconnect_if_closed(conn, DB_CONFIG)
    if conn is None:
        print("Error: Unable to reconnect to the database.")
        return

    try:
        cur = conn.cursor()

        for _, user_data in users_data.iterrows():
            insert_query = sql.SQL("""
                INSERT INTO users (
                    user_id, name, screen_name, location, url, description,
                    protected, followers_count, friends_count, listed_count,
                    created_at, favourites_count, lang, verified, statuses_count,
                    profile_image_url
                ) VALUES (
                    %(user_id)s, %(name)s, %(screen_name)s, %(location)s, %(url)s, %(description)s,
                    %(protected)s, %(followers_count)s, %(friends_count)s, %(listed_count)s,
                    %(created_at)s, %(favourites_count)s, %(lang)s, %(verified)s, %(statuses_count)s,
                    %(profile_image_url)s
                )
                ON CONFLICT (user_id) DO UPDATE
                SET 
                    name = EXCLUDED.name,
                    screen_name = EXCLUDED.screen_name,
                    location = EXCLUDED.location,
                    url = EXCLUDED.url,
                    description = EXCLUDED.description,
                    protected = EXCLUDED.protected,
                    followers_count = EXCLUDED.followers_count,
                    friends_count = EXCLUDED.friends_count,
                    listed_count = EXCLUDED.listed_count,
                    created_at = EXCLUDED.created_at,
                    favourites_count = EXCLUDED.favourites_count,
                    lang = EXCLUDED.lang,
                    verified = EXCLUDED.verified,
                    statuses_count = EXCLUDED.statuses_count,
                    profile_image_url = EXCLUDED.profile_image_url
            """)
            cur.execute(insert_query, user_data.to_dict())

        conn.commit()
        print("Users data loaded successfully!")

    except psycopg2.Error as e:
        print("Error loading users data:", e)
        conn.rollback()

    finally:
        try:
            cur.close()
        except Exception as e:
            print("Error closing cursor:", e)
            
            
def load_tweets(conn, tweets_data):
    """
    Load tweets data into the 'tweets' table in PostgreSQL.

    Args:
    - conn (psycopg2.extensions.connection): PostgreSQL database connection object.
    - tweets_data (pd.DataFrame): DataFrame containing transformed tweets data to be loaded.

    Returns:
    - None

    Raises:
    - psycopg2.Error: If there is an error during the SQL execution.
    """
    conn = reconnect_if_closed(conn, DB_CONFIG)
    if conn is None:
        print("Error: Unable to reconnect to the database.")
        return

    try:
        cur = conn.cursor()

        for _, tweet_data in tweets_data.iterrows():
            insert_query = sql.SQL("""
                INSERT INTO tweets (
                    tweet_id, created_at, text, source, truncated,
                    in_reply_to_status_id, in_reply_to_user_id, user_id
                ) VALUES (
                    %(tweet_id)s, %(created_at)s, %(text)s, %(source)s, %(truncated)s,
                    %(in_reply_to_status_id)s, %(in_reply_to_user_id)s, %(user_id)s
                )
                ON CONFLICT (tweet_id) DO NOTHING
            """)
            cur.execute(insert_query, tweet_data.to_dict())

        conn.commit()
        print("Tweets data loaded successfully!")

    except psycopg2.Error as e:
        print("Error loading tweets data:", e)
        conn.rollback()

    finally:
        try:
            cur.close()
        except Exception as e:
            print("Error closing cursor:", e)
            
            
def load_tweet_hashtags(conn, hashtags_df):
    """
    Load tweet hashtags data in batch into the 'tweet_hashtags' table in PostgreSQL.

    Args:
    - conn (psycopg2.extensions.connection): PostgreSQL database connection object.
    - hashtags_df (pd.DataFrame): DataFrame containing 'tweet_id' and 'hashtag' columns.

    Returns:
    - None
    """
    conn = reconnect_if_closed(conn, DB_CONFIG)
    if conn is None:
        print("Error: Unable to reconnect to the database.")
        return

    try:
        cur = conn.cursor()

        for index, row in hashtags_df.iterrows():
            tweet_id = row['tweet_id']
            hashtag = row['hashtag']

            insert_query = sql.SQL("""
                INSERT INTO tweet_hashtags (tweet_id, hashtag)
                VALUES (%(tweet_id)s, %(hashtag)s)
                ON CONFLICT DO NOTHING
            """)

            hashtag_data = {'tweet_id': tweet_id, 'hashtag': hashtag}

            cur.execute(insert_query, hashtag_data)

        conn.commit()
        print("Tweet hashtags data loaded successfully!")

    except psycopg2.Error as e:
        print("Error loading tweet hashtags data:", e)
        conn.rollback()

    finally:
        try:
            cur.close()
        except Exception as e:
            print("Error closing cursor:", e)
       
def load_tweet_urls(conn, urls_data):
    """
    Load tweet URLs data into the 'tweet_urls' table in PostgreSQL.

    Args:
    - conn (psycopg2.extensions.connection): PostgreSQL database connection object.
    - urls_data (pd.DataFrame): DataFrame containing tweet_id and URL data.

    Returns:
    - None

    Raises:
    - psycopg2.Error: If there is an error during the SQL execution.
    """
    conn = reconnect_if_closed(conn, DB_CONFIG)
    if conn is None:
        print("Error: Unable to reconnect to the database.")
        return

    try:
        cur = conn.cursor()

        for index, row in urls_data.iterrows():
            insert_query = sql.SQL("""
                INSERT INTO tweet_urls (tweet_id, url)
                VALUES (%(tweet_id)s, %(url)s)
                ON CONFLICT DO NOTHING
            """)
            cur.execute(insert_query, row.to_dict())

        conn.commit()
        print("Tweet URLs data loaded successfully!")

    except psycopg2.Error as e:
        print("Error loading tweet URLs data:", e)
        conn.rollback()

    finally:
        try:
            cur.close()
        except Exception as e:
            print("Error closing cursor:", e)
