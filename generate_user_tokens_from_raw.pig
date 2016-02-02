/*
    Author : Pik-Mai Hui <phui@yahoo-inc.com>

    This script generate the following table

            post_id - blog_id - token

    after apply filters on users' number of post and the post content
*/


/*
    This forces Hadoop to run 100 reducers,
    which in turn generates 100 parts output.
*/
SET default_parallel 100;


REGISTER '/home/huip/software/datafu-1.3.0/datafu-pig/build/libs/datafu-pig-incubating-1.3.0.jar';
REGISTER '/home/hadoop/software/pig-0.14.0/contrib/piggybank/java/piggybank.jar';


define Enumerate datafu.pig.bags.Enumerate('1');


data = LOAD '$in' USING PigStorage()
    AS (uid:chararray,
        twts:bag{tuple:(tweet:chararray)});

data = FILTER data BY COUNT(twts) > 7;

data = FOREACH data GENERATE
    uid AS uid:chararray,
    FLATTEN(Enumerate(twts)) AS (tweet:chararray,tid:int);

data = FILTER data BY
    tweet IS NOT NULL AND
    SIZE(tweet) > 2;

data = FOREACH data GENERATE
    tid AS tid:int,
    uid AS uid:chararray,
    LOWER(tweet) AS pcontent:chararray;

tokenized = FOREACH data GENERATE
    tid AS tid:int,
    uid AS uid:chararray,
    FLATTEN(
        TOKENIZE(
            REPLACE(
                REPLACE(
                    pcontent, -- drop common noises
                    '((\\@[\\w\\_]+)|(https*\\:\\/\\/\\S+)|(\\#+[\\w\\_]+[\\w\\\'\\_\\-]*[\\w\\_]+)|(<.+?>))',
                    ' ' -- anything we drop will become spaces
                ), -- drop puntuations and wired symbols
                '[\\?\\.\\-_;,:!\\\'"\\u201C\\u201D\\u2018\\u2019\\u00B4\\u0060\\u002E\\uFE52\\uFF0E]+',
                ' '
            )
        ) -- split around spaces, where noises are dropped all together
    ) AS token:chararray; -- flatten out to token by token


-- only take meaningful tokens
tokenized = FILTER tokenized BY
    token IS NOT NULL AND
    SIZE(token) > 2; -- words with <=2 characters are stopwords


trimed = FOREACH tokenized GENERATE
    tid AS tid:int,
    uid AS uid:chararray,
    FLATTEN(
        REGEX_EXTRACT_ALL(
            token, -- extract words surrounded by \W characters
            '(?:(?:\\W*)((?:\\w+)(?:.+)(?:\\w+))(?:\\W*))'
        )
    ) AS token:chararray;


trimed = FILTER trimed BY
    token IS NOT NULL AND
    SIZE(token) > 2;


STORE trimed INTO '$out' USING PigStorage('\t', '-schema');
