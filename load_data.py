import psycopg2
import pandas as pd
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
def show_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()
    # get the line number when exception occured
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
    buffer = StringIO()
    buffer.seek(0)
    df_data.to_csv(buffer, header=False, index=False, sep='\t')
    try:
        cur.copy_from(buffer, table_name, sep='\t')
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as err:
        # pass exception to function,
        show_psycopg2_exception(err)


def main(df_tweets_data, df_context_annotations_cleaned, df_tweets_includes, df_public_metrics):
    conn, cur = establish_database_connection()

    copy_from_dataFile_StringIO(conn, cur, df_tweets_includes, 'authors')
    copy_from_dataFile_StringIO(conn, cur, df_tweets_data, 'tweets')
    copy_from_dataFile_StringIO(conn, cur, df_public_metrics, 'public_metrics')
    copy_from_dataFile_StringIO(conn, cur, df_context_annotations_cleaned, 'context_annotations')

if __name__ == "__main__":
    main()
