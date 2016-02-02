/*
    Author : Pik-Mai Hui <phui@yahoo-inc.com>

    This script takes a list of stopwords and a table of user
    and token, and then filter stopwords from the table.

    At the end it also regroups the user-token table into

                blog_id -- bags of post

    relation, which is then easily fed into any streaming job
    to calculate user properties in a user-by-user mannor.
*/

/*
    This forces Hadoop to run 100 reducers,
    which in turn generates 100 parts output.
*/
SET default_parallel 100;


-- pre-constructed stopword list
stpwrds = LOAD '$stpwrd_in' USING PigStorage('\t', '-schema') AS
    token:chararray;


-- pre-constructed user-token table
user_token = LOAD '$user_tokens_in' USING PigStorage('\t', '-schema') AS (
    tid:int,
    uid:chararray,
    token:chararray
);

-- pick out the stopwords
joined = JOIN user_token BY token LEFT, stpwrds BY token;
filtered_joined = FILTER joined BY stpwrds::token IS NULL;
flatten_joined = FOREACH filtered_joined GENERATE
    user_token::token AS token:chararray,
    user_token::tid AS tid:int,
    user_token::uid AS uid:chararray;

-- group by each unique user and tweet, then drop the tweet id
-- post is bag of tokens
grped_posts = FOREACH (GROUP flatten_joined BY (uid,tid)) GENERATE
    group.$0 AS uid:chararray,
    flatten_joined.token AS post:bag{tuple:(token:chararray)};

-- group by blog id to construct bag of posts for each blog id
grped_user_posts = FOREACH (GROUP grped_posts by uid) GENERATE
    group AS uid:chararray,
    grped_posts.post; -- implicit, bag of (post=bag of strings)

STORE grped_user_posts INTO '$out' USING PigStorage('\t', '-schema');
