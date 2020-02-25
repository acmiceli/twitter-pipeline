DROP TABLE IF EXISTS prod.tweets_dash; create table prod.tweets_dash as
WITH agg AS 
    (SELECT date(created_at) AS date_created,
         name,
         max(favourite_count) AS max_likes,
         max(retweet_count) AS max_retweets,
         count(*) AS total_tweets,
         sum(retweet_count) AS total_retweets,
         sum(favourite_count) AS total_likes
    FROM prod.tweets
    GROUP BY  1, 2 ), most_liked AS 
    (SELECT *
    FROM 
        (SELECT date(created_at) AS date_created,
         name,
         tweet_id AS most_liked_tweet_id,
         favourite_count AS like_count,
         text AS most_liked_tweet_text,
         row_number()
            OVER (partition by date(created_at), name
        ORDER BY  favourite_count desc) AS fav
        FROM prod.tweets )
        WHERE fav = 1 ), most_retweeted AS 
        (SELECT *
        FROM 
            (SELECT date(created_at) AS date_created,
         name,
         tweet_id most_retweeted_tweet_id,
         retweet_count,
         text AS most_retweeted_tweet_text,
         row_number()
                OVER (partition by date(created_at), name
            ORDER BY  retweet_count desc) AS rt
            FROM prod.tweets )
            WHERE rt = 1 )
        
        SELECT a.*,
         round((a.max_likes + a.max_retweets) / a.total_tweets,
         1) AS engagment_per_tweet,
         l.most_liked_tweet_id ,
         l.most_liked_tweet_text,
         r.most_retweeted_tweet_id,
         r.most_retweeted_tweet_text
    FROM agg a
LEFT JOIN most_liked l
    ON a.date_created = l.date_created
        AND a.name = l.name
LEFT JOIN most_retweeted r
    ON a.date_created = r.date_created
        AND a.name = r.name
        ORDER BY 1,2; 