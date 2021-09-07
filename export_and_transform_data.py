import requests
import json
import pandas as pd

# bearer token is a "password" that is needed for requesting API
bearer_token = "AAAAAAAAAAAAAAAAAAAAACvwRgEAAAAAP7bqakxMuZ4lwpWuJ0VnjCfebo4" \
               "%3DlqMS726jO2c6wTkGvywLeXwQJEPJf3hjfGpWuYyIK6dJs0dR8t "

# parameters that determine the results of the query
query_params = {
    'query': '"The Sims"',
    'max_results': 10,
    'tweet.fields': 'text,author_id,public_metrics,context_annotations,possibly_sensitive,lang,created_at',
    'user.fields': 'name,username,verified,created_at',
    'expansions': 'author_id'
}


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r


def connect_to_endpoint(params):
    """
    Connect to endpoint and retrieve recent
    tweets that includes phrase "The Sims" inside of them

    :param dictionary params: Parameters that determine the results of the query
    :return:tweets and data corresponding to them
    :rtype: json
    :raise: Exception: Error occurred during the GET request on, the error has been saved in error_log.json
     """

    response = requests.get("https://api.twitter.com/2/tweets/search/recent",
                            auth=bearer_oauth, params=params)

    if response.status_code != 200:
        # create error file if it doesn't exist
        open('error_log.json', 'w+')

        # save error into the error_log
        x = response.json()
        json.dump(x, open('error_log.json', 'r+'))

        raise Exception(f"Something went wrong during the GET request\nStatus code : {response.status_code}"
                        f" <----Should be 200\nFor specific information about the status code go to "
                        f"\nhttps://developer.twitter.com/en/docs/twitter-ads-api/response-codes\nError description"
                        f" has been saved in error_log.json")

    print("Exporting data from the source completed")
    return response.json()


def transform_and_clear_data(tweets):
    """
    Extract individual pieces of different data from tweets, convert them to dataframes and clean
    'context_annotations' part using function 'extract_and_clear_context_annotations_field'

    :param dictionary tweets: tweet data
    :return:
        -  df_tweets_data - 'data' part of the tweet data
        -  df_context_annotations_cleaned - 'context_annotations' part of the tweet data after transforming it
        -  df_tweets_includes - 'includes' part of the tweet data
        -  df_public_metrics - 'public_metrics' part of the tweet data
    :rtype: pandas dataframes
    """
    # Extract the 'data' part of the tweet
    tweets_data_part = tweets['data']

    # Convert 'tweets_data_part' to dataframe
    df_tweets_data = pd.json_normalize(tweets_data_part)

    # Create dataframe with only context_annotations and id, so it will be possible to merge this data back together
    df_context_annotations = pd.concat([df_tweets_data['id'], df_tweets_data['context_annotations']], axis=1)

    # Delete the 'context_annotations' field from the original dataframe
    df_tweets_data.__delitem__('context_annotations')

    # Extract domain name and entity name from every row of the context_annotations dataframe
    # and merge all Series into dataframe

    list_context_annotations_cleaned = df_context_annotations.apply(
        extract_and_clear_context_annotations_field, axis=1).explode().dropna()

    df_context_annotations_cleaned = pd.DataFrame(list_context_annotations_cleaned.to_list())

    # Extract the 'includes' part of the data into dataframe
    df_tweets_includes = pd.DataFrame(tweets['includes']['users'])

    # Extract the 'Public metrics' part from df_data

    df_public_metrics = df_tweets_data[['id',
                                        'public_metrics.retweet_count',
                                        'public_metrics.reply_count',
                                        'public_metrics.like_count',
                                        'public_metrics.quote_count'
                                        ]]

    df_tweets_data.__delitem__('public_metrics.retweet_count')
    df_tweets_data.__delitem__('public_metrics.reply_count')
    df_tweets_data.__delitem__('public_metrics.like_count')
    df_tweets_data.__delitem__('public_metrics.quote_count')

    # Clean newlines from tweets
    df_tweets_data['text'] = df_tweets_data['text'].replace(r'\n', ' ', regex=True)

    # Organize df_tweets
    df_tweets_data = df_tweets_data[['text',
                                     'id',
                                     'author_id',
                                     'possibly_sensitive',
                                     'lang',
                                     'created_at'
                                     ]]

    # Organize df_authors
    df_tweets_includes = df_tweets_includes[['username',
                                             'created_at',
                                             'verified',
                                             'name',
                                             'id'
                                             ]]


    return df_tweets_data, df_context_annotations_cleaned, df_tweets_includes, df_public_metrics


def extract_and_clear_context_annotations_field(row_context_annotations_and_id):
    """
     The 'context_annotations' field contains information about the contexts that are included in the tweet.
     We want to be sure that our tweets contains words 'The Sims' and that those tweets are about the Video Game,
     not other entity that holds the same name. So we ensure that every tweet, in the domain section
     has the name 'Video Game' and the entity name is 'The Sims'

     Be aware that tweets can contain many contexts. Also the information about other Entities that are often
     written in the same tweets that contain 'The Sims' entity can be useful, so we also extract them.

    :param: Series row_context_annotations_and_id: row of the context_annotations field from the tweets we exported.
    Also contains the id of the tweet, that it is related to, so it will be easier to fuse context_annotations field
    with the rest of the tweet information back together, after we clean it.
    :return:lists_of_rows_after_cleaning: a
    list of at least one Series containing tweet id, entity name, domain name extracted from the context_annotations.
    :rtype:list
    """

    # Boolean to check whether tweet contains The Sims Video Game entity
    contains_The_Sims = False

    # Create a list for rows that had been cleaned
    list_of_rows_after_cleaning = []
    print(row_context_annotations_and_id['context_annotations'])
    if row_context_annotations_and_id['context_annotations'] or isinstance(row_context_annotations_and_id,float):
        return None
    else:
        # Iterate over list of the dictionaries inside the context_annotations field
        for domains_and_entities in row_context_annotations_and_id['context_annotations']:

            # Check whether phrase "The Sims" used in the tweet is used in the context of the game
            # If yes, change 'contains_the_sims' to True
            # if not, that means it is something else that was mentioned in the same tweet, we store it's
            # entity and domain name and the id of the tweet it comes from, in pandas Series, then store it in
            # the list

            if domains_and_entities['domain']['name'] == 'Video Game' and \
                    domains_and_entities['entity']['name'] == 'The Sims':
                contains_The_Sims = True
            else:
                row_after_cleaning = pd.Series([row_context_annotations_and_id['id'],
                                            domains_and_entities['domain']['name'],
                                            domains_and_entities['entity']['name']], index=['id', 'domain', 'name'])

                list_of_rows_after_cleaning.append(row_after_cleaning)

    # Check whether the tweet contains The Sims entity, if not - skip it
    if contains_The_Sims and len(list_of_rows_after_cleaning) != 0:

        return list_of_rows_after_cleaning
    else:

        return None


def main():
    # Download data from twitter
    tweet_data = connect_to_endpoint(query_params)

    # Transform and clear data then return data divided into dataframes to main script
    return transform_and_clear_data(tweet_data)


if __name__ == "__main__":
    main()
