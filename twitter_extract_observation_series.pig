REGISTER /home/hadoop/software/pig-0.14.0/lib/json-simple-1.1.jar;
REGISTER /vol/home1/huip/software/elephant-bird/pig/target/elephant-bird-pig-4.11-SNAPSHOT.jar;
REGISTER /vol/home1/huip/software/elephant-bird/core/target/elephant-bird-core-4.11-SNAPSHOT.jar;
REGISTER /vol/home1/huip/software/elephant-bird/hadoop-compat/target/elephant-bird-hadoop-compat-4.11-SNAPSHOT.jar;


raw = LOAD '/truthy/loading/2015-0{8,9}/*.json.gz' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json:map[]);

data = FOREACH raw GENERATE
    json#'user'#'id_str' AS uid:chararray,
    json#'created_at' AS created_at:chararray,
    FLATTEN(json#'entities'#'hashtags') AS tag_data:map[chararray];

data = FOREACH data GENERATE
    uid AS uid:chararray,
    created_at AS created_at:chararray,
    tag_data#'text' AS tag:chararray,
    0 AS from_existing:int;

existing_tags = LOAD '/truthy/loading/2015-07/*.json.gz' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json:map[]);
existing_tags = FOREACH existing_tags GENERATE
    FLATTEN(json#'entities'#'hashtags') AS tag_data:map[chararray];
existing_tags = FOREACH existing_tags GENERATE
    '' AS uid:chararray,
    '' AS created_at:chararray,
    tag_data#'text' AS tag:chararray,
    1 AS from_existing:int;

uni = UNION data, existing_tags;
uni = FILTER uni BY tag IS NOT NULL AND SIZE(tag) > 0;

data_with_existing_tag_and_count = FOREACH (GROUP uni BY tag) GENERATE
    group AS tag:chararray,
    uni.(uid, created_at) AS ts:bag{tuple:(uid:chararray,created_at:chararray)},
    (long) SUM(uni.from_existing) AS from_existing_sum:long;

filtered_data = FILTER data_with_existing_tag_and_count BY from_existing_sum==0L;

filtered_data = FOREACH filtered_data {
    time_series = FOREACH ts GENERATE
        uid AS uid:chararray,
        ToUnixTime(ToDate(created_at, 'EEE MMM dd HH:mm:ss Z yyyy')) AS tstamp;
    time_series = ORDER time_series BY tstamp ASC;
    GENERATE tag, time_series;
}


STORE filtered_data INTO '2015-08_09_adopter_series_full.gz' USING PigStorage('\t', '-schema');
