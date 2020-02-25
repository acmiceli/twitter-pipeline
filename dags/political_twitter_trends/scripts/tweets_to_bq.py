import datetime
import pandas as pd
import tweepy
import math
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from airflow.models import Connection
from airflow import settings
import json

def create_gcp_conn(gcp_conn_id, project_id, key_path):
    """ Creates airflow to google-cloud-platform connection.

    Args:
        gcp_conn_id: string, names the connection
        project_id: string, names the gcp project
        key_path: string, names path to gcp keys file
    """
    new_conn = Connection(
        conn_id=gcp_conn_id,
        conn_type='google_cloud_platform'
    )
    scopes = [
        "https://www.googleapis.com/auth/cloud-platform"
    ]
    conn_extra = {
        "extra__google_cloud_platform__scope": ",".join(scopes),
        "extra__google_cloud_platform__project": project_id,
        "extra__google_cloud_platform__key_path": key_path
    }
    conn_extra_json = json.dumps(conn_extra)
    new_conn.set_extra(conn_extra_json)

    session = settings.Session()
    if not (session.query(Connection).filter(Connection.conn_id == new_conn.conn_id).first()):
        session.add(new_conn)
        session.commit()
    else:
        pass
    return 'BQ connected'


def get_tweet_info(item):
    """ Extract specified details from single tweet.

    Args:
        item: element of twitter API status object
    
    Returns:
        tweet_info: dict, tweet details
    """
    
    tweet_info = {
                    'tweet_id':        item.id,
                    'name':            item.user.name,
                    'screen_name':     item.user.screen_name,
                    'retweet_count':   item.retweet_count,
                    'text':            item.full_text,
                    'info_pulled_at':  datetime.datetime.now(),
                    'created_at':      item.created_at,
                    'favourite_count': item.favorite_count,
                    'hashtags':        item.entities['hashtags'],
                    'status_count':    item.user.statuses_count,
                    'location':        item.place,
                    'source_device':   item.source
                }

    try:
        tweet_info['retweet_text'] = item.retweeted_status.full_text
    except:
        tweet_info['retweet_text'] = 'None'
    try:
        tweet_info['quote_text'] = item.quoted_status.full_text
        tweet_info['quote_screen_name'] = item.quoted_status.user.screen_name
    except:
        tweet_info['quote_text'] = 'None'
        tweet_info['quote_screen_name'] = 'None'

    return tweet_info

def get_users_tweets(users, min_date, max_date, result_limit, key, secret_key):
    """ For users and date range specified, get get all tweets from timeline.

    Args:
        users: list, twitter usernames (note: do not include '@')
        min_date: string, specifies min_date of tweets retrieved (e.g., '2020-02-04')
        max_date: string, specifies max_date of tweets retrieved (e.g., '2020-02-04')
        result_limit: int, limit of number of tweets returned in single api call
        key: string, twitter api key (view-access)
        secret_key: string, twitter api secret key (view-access)
    Returns:
        tweet_info: dict of tweet details
    """
    
    auth = tweepy.OAuthHandler(key, secret_key)
    max_datetime = datetime.datetime.strptime(max_date, '%Y-%m-%d').date()
    min_datetime = datetime.datetime.strptime(min_date, '%Y-%m-%d').date()
    
    #initialize variables
    max_id = None
    min_id = None
    mydata =  []

    for user in users:
        my_api = tweepy.API(auth)

        statuses   =   my_api.user_timeline(screen_name=user,
                                                            count=result_limit,
                                                            tweet_mode = 'extended',
                                                            include_retweets=True
                                                            )
        for item in statuses: 
            if item.created_at.date() > max_datetime:
                max_id = item.id
                #max_id_date = item.created_at
            elif  min_datetime <= item.created_at.date() <= max_datetime:
                mydata.append(get_tweet_info(item))
                if max_id == None:
                    max_id = item.id
            else: #less than min_datetime
                min_id = item.id
                #min_id_date = item.created_at
                break

        while min_id == None:
            start_id = item.id
            statuses   =   my_api.user_timeline(screen_name=user,
                                                            count=result_limit,
                                                            max_id=start_id,
                                                            tweet_mode = 'extended',
                                                            include_retweets=True
                                                            )
            for item in statuses: 
                if item.created_at.date() > max_datetime:
                    max_id = item.id
                    #max_id_date = item.created_at
                elif min_datetime <= item.created_at.date() <= max_datetime:
                    mydata.append(get_tweet_info(item))
                    if max_id == None:
                        max_id = item.id
                else: #less than min_datetime
                    min_id = item.id
                    #min_id_date = item.created_at
                    break 
            #get another 25 starting with the max... 
        # if min_id is None... then call again... using the bottom of mydata as max_id...

    df = pd.DataFrame(mydata).loc[:,'tweet_id':'favourite_count']
    return df

def upload_df_to_bq(df, project_id, table_id):
    """ Upload pandas data frame to BigQuery

    Args:
        df: pandas data frame
        project_id: string, gcp project id
        table_id: string, destination bq table
        
    """

    df.to_gbq(table_id, project_id=project_id, if_exists='replace')
    return 'table uploaded to ' + project_id + '.' + table_id


def get_tweets_upload_to_bq(users, min_date, max_date, result_limit, key, secret_key, project_id, table_id, **context):
    """ Wrapper function to pass to PythonOperator in Airflow; scrape users' tweets and upload to bq.

    Args:
        users: list, twitter usernames (note: do not include '@')
        min_date: string, specifies min_date of tweets retrieved (e.g., '2020-02-04')
        max_date: string, specifies max_date of tweets retrieved (e.g., '2020-02-04')
        result_limit: int, limit of number of tweets returned in single api call
        key: string, twitter api key (view-access)
        secret_key: string, twitter api secret key (view-access)
        project_id: string, gcp project id
        table_id: string, destination bq table
        
    """

    if context.get("yesterday_ds"):
        df = get_users_tweets(users, context['yesterday_ds'], context['yesterday_ds'], result_limit, key, secret_key)
    else: 
        df = get_users_tweets(users, min_date, max_date, result_limit, key, secret_key)
    upload_df_to_bq(df, project_id, table_id)

    return 'scraped tweets and uploaded to bq'


if __name__ == "__main__":
    # run example in python
    users = ['ewarren', 'berniesanders', 'petebuttigieg', 'joebiden', 'amyklobuchar']
    keys_dict = {
            'key':       '4rFQghkGKjSkK2SuSTSFa46mT',
            'secret_key':     'OJfiCIMYQleB0fls87UllGSLJPPrXKnkJeNxbIxrbbgMj9mOPd'
        }
    result_limit = 200
    min_date = '2020-02-23'
    max_date = '2020-02-23'
    project_id = "twitter-pipeline-269020"
    table_id = 'stg.tweets_ghi'

    # get tweets in date range from users and upload to bq
    get_tweets_upload_to_bq(users, min_date, max_date, result_limit, keys_dict['key'], keys_dict['secret_key'], project_id, table_id)


