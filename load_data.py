import psycopg2
import pandas as pd
import os
from io import StringIO
import sys


def establish_database_connection():
    """
    Establish connection to the database 'tweetsdb' and returns conn and cur
    :return:
        - conn - reference to the database
        - cur - cursor used to perform sql commands to the database
    """
    conn = psycopg2.connect("host=localhost dbname=tweetsdb user=postgres password=123")
    cur = conn.cursor()

    return conn, cur


# Define a function that handles and parses psycopg2 exceptions
def psycopg2_exception(err):
    '''
    Gives insights of the error that occurred

    :param err: information of the error that occurred
    :return:
    '''
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()
    # get the line number when exception occurred
    line_n = traceback.tb_lineno
    # print the connect() error
    print("\npsycopg2 ERROR:", err, "on line number:", line_n)
    print("psycopg2 traceback:", traceback, "-- type:", err_type)
    # psycopg2 extensions.Diagnostics object attribute
    print("\nextensions.Diagnostics:", err.diag)
    # print the pgcode and pgerror exceptions
    print("pgerror:", err.pgerror)
    print("pgcode:", err.pgcode, "\n")


def copy_from_dataFile_StringIO(conn, cur, df_data, table_name):
    '''
    :param conn: reference to the database
    :param cur: cursor used to perform sql commands to the database
    :param df_data:
    :param table_name:
    :return:
    '''
    # Create buffer
    buffer = StringIO()
    # Set the position in the buffer at the beginning

    # Import data into the buffer
    df_data.to_csv(buffer, header=False, index=False, sep='\t')
    buffer.seek(0)

    try:
        # Copy data into table
        cur.copy_from(buffer, table_name, sep='\t')
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as err:
        # pass exception to function
        psycopg2_exception(err)



def main(df_tweets_data, df_context_annotations_cleaned, df_tweets_includes, df_public_metrics):
    # Establish database connections
    conn, cur = establish_database_connection()

    # Import data to 'authors' table
    copy_from_dataFile_StringIO(conn, cur, df_tweets_includes, 'authors')
    # Import data to 'tweets' table
    copy_from_dataFile_StringIO(conn, cur, df_tweets_data, 'tweets')
    # Import data to 'public_metrics' table
    copy_from_dataFile_StringIO(conn, cur, df_public_metrics, 'public_metrics')
    # Import data to 'context_annotations' table
    copy_from_dataFile_StringIO(conn, cur, df_context_annotations_cleaned, 'context_annotations')


if __name__ == "__main__":
    main()
