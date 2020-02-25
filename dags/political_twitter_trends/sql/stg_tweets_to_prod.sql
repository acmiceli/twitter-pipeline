INSERT prod.tweets ( tweet_id, name, screen_name, retweet_count, text, info_pulled_at, created_at, favourite_count)
SELECT *
FROM stg.tweets s
WHERE NOT EXISTS 
    (SELECT *
    FROM prod.tweets t
    WHERE t.tweet_id = s.tweet_id)