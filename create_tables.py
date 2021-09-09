import psycopg2


def create_database():
    """
    Establishes database connection, creates database tweetsdb
    :return:Cursor and connection reference to the database tweetsdb (cur,con)
    """
    # Check if database 'tweetsdb' already exists
    try:
        conn = psycopg2.connect("host=localhost dbname=tweetsdb user=postgres password=123")
    except:
        # Create connection to the database so we can create our own database
        conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=123")

        # To be able to create tables/databases by transactions
        conn.set_session(autocommit=True)

        # Open cursor
        cur = conn.cursor()

        # Create database 'tweetsdb'
        cur.execute("CREATE DATABASE  tweetsdb WITH ENCODING 'utf8' TEMPLATE template0")

        # Close connection to default database
        conn.close()

        # Create connection the new database
        conn = psycopg2.connect("host=localhost dbname=tweetsdb user=postgres password=123")

    # Open new cursor to perform database operations
    cur = conn.cursor()

    return cur, conn


def create_tables(cur, conn):
    """
    Creates tables: Tweets,Authors and Public_metrics and Context_annotations
    :param cur: Cursor to database used to perform sql commands on database
    :param conn: Database connection reference
    :return:
    """


    # Check whether table 'authors' exists and create it if it doesn't
    cur.execute("select exists(select * from information_schema.tables where table_name='authors')")
    if not cur.fetchone()[0]:
        cur.execute(
            """CREATE TABLE Authors 
            (
            username varchar(100),
            created_at timestamp,
            verified boolean,
            name varchar(100),
            author_id varchar(30),
            PRIMARY KEY (author_id)
            )"""
        )

    # Check whether table 'tweets' exists and create it if it doesn't
    cur.execute("select exists(select * from information_schema.tables where table_name='tweets')")
    if not cur.fetchone()[0]:
        cur.execute(
            """CREATE TABLE Tweets 
            (
            text varchar(1000),
            tweet_id varchar(20),
            author_id varchar(20),
            possibly_sensitive boolean,
            language varchar(30),
            date_of_creation timestamp,
            PRIMARY KEY (tweet_id),
            FOREIGN KEY (author_id) REFERENCES Authors(author_id)
            )"""
        )

    # Check whether table 'public_metrics' exists and create it if it doesn't
    cur.execute("select exists(select * from information_schema.tables where table_name='public_metrics')")
    if not cur.fetchone()[0]:
        cur.execute(
            """CREATE TABLE Public_metrics 
            (
            tweet_id varchar(20),
            retweets_count integer,
            replies_count integer,
            like_count integer,
            quote_count integer,
            PRIMARY KEY (tweet_id),
            FOREIGN KEY (tweet_id) REFERENCES Tweets(tweet_id)
            )"""
        )

    # Check whether table 'context_annotations' exists and create it if it doesn't
    cur.execute("select exists(select * from information_schema.tables where table_name='context_annotations')")
    if not cur.fetchone()[0]:
        cur.execute(
            """CREATE TABLE Context_annotations
            (
            tweet_id varchar(20),
            domain_name varchar(50),
            entity_name varchar(50),
            FOREIGN KEY (tweet_id) REFERENCES Tweets(tweet_id)
            )"""
        )

    conn.commit()


def main():
    # Create database
    cur, conn = create_database()

    # Create tables
    create_tables(cur, conn)


if __name__ == "__main__":
    main()
