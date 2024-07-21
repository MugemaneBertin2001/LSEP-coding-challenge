import os
from config import DB_CONFIG
from db_utils import connect_to_db, create_tables
from etl_extract import extract_from_json
from etl_transform import transform_data
from etl_load import load_users, load_tweets, load_tweet_hashtags, load_tweet_urls

def main():
    current_directory = os.getcwd()
    dataset_directory = os.path.join(current_directory, '..', 'LSEP-coding-challenge','dataset')
    dataset_directory = os.path.normpath(dataset_directory)
    file_path = os.path.join(dataset_directory, 'query2_ref.json')
    extracted_data = extract_from_json(file_path)
    
    if extracted_data is None:
        print("Data extraction failed.")
        return
    
    conn = connect_to_db(DB_CONFIG)
    if conn is None:
        print("Database connection failed. Check your credentials and try again.")
        return
    
    try:
        create_tables(conn)
        print("Tables created successfully!")
        load_data(conn, extracted_data)
    
    except Exception as e:
        print("Error during data load:", e)
    
    finally:
        if conn and not conn.closed:
            conn.close()
            print("Database connection closed.")

def load_data(conn, extracted_data):
    try:
        users_df, tweets_df, hashtags_df, urls_df = transform_data(extracted_data)
        load_users(conn, users_df)
        load_tweets(conn, tweets_df)
        load_tweet_hashtags_batch(conn, hashtags_df)
        load_tweet_urls_batch(conn, urls_df)
        print("All data loaded successfully!")
    
    except Exception as e:
        print("Error during data load:", e)

def load_tweet_hashtags_batch(conn, hashtags_df):
    """
    Load tweet hashtags data in batch into the 'tweet_hashtags' table in PostgreSQL.

    Args:
    - conn (psycopg2.extensions.connection): PostgreSQL database connection object.
    - hashtags_df (pd.DataFrame): DataFrame containing tweet_id and hashtag data.

    Returns:
    - None
    """
    load_tweet_hashtags(conn, hashtags_df)

def load_tweet_urls_batch(conn, urls_df):
    """
    Batch load tweet URLs data into the 'tweet_urls' table in PostgreSQL.

    This function facilitates batch processing of tweet URLs data by calling
    the `load_tweet_urls` function for each row in the provided DataFrame (`urls_df`).

    Args:
    - conn (psycopg2.extensions.connection): PostgreSQL database connection object.
    - urls_df (pd.DataFrame): DataFrame containing tweet_id and URL data to be loaded.

    Returns:
    - None

    Raises:
    - None

    Note:
    - This function acts as a wrapper to sequentially load each URL from `urls_df`
      using the `load_tweet_urls` function.

    Example usage:
    >>> load_tweet_urls_batch(conn, urls_df)
    """
    load_tweet_urls(conn, urls_df)


if __name__ == "__main__":
    main()
