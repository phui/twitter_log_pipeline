REGISTER /home/hadoop/software/pig-0.14.0/lib/json-simple-1.1.jar;
REGISTER /vol/home1/huip/software/elephant-bird/pig/target/elephant-bird-pig-4.11-SNAPSHOT.jar;
REGISTER /vol/home1/huip/software/elephant-bird/core/target/elephant-bird-core-4.11-SNAPSHOT.jar;
REGISTER /vol/home1/huip/software/elephant-bird/hadoop-compat/target/elephant-bird-hadoop-compat-4.11-SNAPSHOT.jar;

--- register for Python UDF
REGISTER '/home/huip/info_diff/hadoop_code/pig_code/concat_bag_of_string.py' USING jython as funcs;

--- nested load twitter json logs
raw = LOAD '$in' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (json:map[]);

--- only take uid, language and tweet content
data = FOREACH raw GENERATE (long)json#'user'#'id' AS uid, (chararray)json#'lang' AS lang, (chararray)json#'text' AS twt_content;

--- only consider english tweets
data = FILTER data by lang == 'en';

--- parse out set of tokens from all posts by each user
--- group by Pig, call Python UDF for parsing and serialization
user_data = FOREACH (GROUP data BY uid) GENERATE group AS uid:long, funcs.convertBagToStr(data.twt_content) as bag_str;

--- filter out default Nan labal defined in Python UDF ("0") -> SIZE == 1
user_data = FILTER user_data by SIZE(bag_str) > 1;

--- store data
STORE user_data INTO '$out' USING PigStorage();
