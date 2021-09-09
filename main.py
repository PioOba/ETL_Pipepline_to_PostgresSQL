from export_and_transform_data import main as export_and_transform_data_main
from create_tables import main as create_tables_main
from load_data import main as load_data_main

if __name__ == "__main__":

    df_tweets_data, df_context_annotations_cleaned, df_tweets_includes, df_public_metrics = \
        export_and_transform_data_main()

    create_tables_main()
    load_data_main(df_tweets_data, df_context_annotations_cleaned, df_tweets_includes, df_public_metrics)
