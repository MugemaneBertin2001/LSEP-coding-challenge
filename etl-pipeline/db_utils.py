import psycopg2




def create_tables(conn):
    commands = (
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            name TEXT,
            screen_name TEXT,
            location TEXT,
            url TEXT,
            description TEXT,
            protected BOOLEAN,
            followers_count INTEGER,
            friends_count INTEGER,
            listed_count INTEGER,
            created_at TIMESTAMP,
            favourites_count INTEGER,
            lang TEXT,
            verified BOOLEAN,
            statuses_count INTEGER,
            profile_image_url TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS tweets (
            tweet_id TEXT PRIMARY KEY,
            created_at TIMESTAMP,
            text TEXT,
            source TEXT,
            truncated BOOLEAN,
            in_reply_to_status_id TEXT,
            in_reply_to_user_id TEXT,
            user_id TEXT REFERENCES users(user_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS tweet_hashtags (
            id SERIAL PRIMARY KEY,
            tweet_id TEXT REFERENCES tweets(tweet_id),
            hashtag TEXT
        );
        """,
        """
       CREATE TABLE IF NOT EXISTS tweet_urls (
            id SERIAL PRIMARY KEY,
            tweet_id TEXT REFERENCES tweets(tweet_id),
            url TEXT
        );
        """
    )
    
    try:
        cur = conn.cursor()
        
        for command in commands:
            cur.execute(command)
        
        # Commit changes
        conn.commit()
        
        # Close communication with the PostgreSQL database
        cur.close()
        
    except psycopg2.Error as e:
        print("Error executing SQL statement:", e)
        conn.rollback()
        
    finally:
        conn.close()

# Function to establish connection to PostgreSQL
def connect_to_db(db_config):
    try:
        conn = psycopg2.connect(
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            host=db_config['host'],
            port=db_config['port']
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"Error connecting to the database: {e}")
        return None

def reconnect_if_closed(conn, db_config):
    if conn.closed:
        print("Reconnecting to the database...")
        conn = connect_to_db(db_config)
    return conn