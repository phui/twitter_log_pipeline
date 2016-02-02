/*
    Author : Pik-Mai Hui <phui@yahoo-inc.com>

    This script is used to sort document frequency without
    creating Java objects that represent bags of token

    This script is created because some users generate
    a large amount of content, forcing the JVM to create lots
    of string objects, which leads to JVM heap overflow

    It is observed that calling COUNT() on a GROUP result
    immediately will not generate the objects. Therefore
    we use this short script to count document frequency.

    This is the first step of creating a list of stopwords
*/
data = LOAD '$in' USING PigStorage('\t', '-schema') AS (
    tid:int,
    uid:chararray,
    token:chararray
);


data = FOREACH (GROUP data BY token) GENERATE
    group AS token:chararray,
    COUNT(data) AS df:long; -- data AS data; will give heap overflow

data = ORDER data BY df DESC;

STORE data INTO '$out' USING PigStorage('\t', '-schema');
