/*
    Author : Pik-Mai Hui <phui@yahoo-inc.com>

    This script is following the sorted_df_count.pig and
    finishing the job of creating a set of stopwords based
    on the input data.
*/

-- force single output file
SET default_parallel 1;

token_doc_count = LOAD '$in' USING PigStorage('\t', '-schema') AS (
    token:chararray,
    df:long
); -- this should be already sorted by df in desc order

-- top 250 most frequent stopwords
top_N_stpwrds = LIMIT token_doc_count 250;
top_N_stpwrds = FOREACH top_N_stpwrds GENERATE token AS token:chararray;

-- words that give no correlations, mostly misspelt words
tail_stpwrds = FILTER token_doc_count BY df < 8;
tail_stpwrds = FOREACH tail_stpwrds GENERATE token AS token:chararray;

/*
    Standard stopwords list in English
    source: https://code.google.com/archive/p/stop-words/
*/
std_stpwrds = LOAD 'eng_std_stpwrds.txt' USING PigStorage(',') AS token:chararray;

-- merge all three of them
stpwrds = UNION std_stpwrds, top_N_stpwrds, tail_stpwrds;

STORE stpwrds INTO '$out' USING PigStorage('\t', '-schema');
