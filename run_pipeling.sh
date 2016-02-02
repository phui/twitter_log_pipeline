#!/bin/bash
pig -f filter_raw_source.pig \
    -param in="$1" \
    -param out=2015_09_raw_posts.gz

# this step can possibly consume larger heap space
export HADOOP_OPTS="-Xmx4096m $HADOOP_OPTS"
export HADOOP_CLIENT_OPTS="-Xmx4096m $HADOOP_CLIENT_OPTS"
pig -f generate_user_tokens_from_raw.pig \
    -param in=2015_09_raw_posts.gz/*.gz \
    -param out=2015_09_user_tokens.gz

pig -f sort_df_count.pig \
    -param in=2015_09_user_tokens.gz/*.gz  \
    -param out=2015_09_sorted_token_df.gz

pig -f generate_union_stopwords.pig \
    -param in=2015_09_sorted_token_df.gz/*.gz \
    -param out=2015_09_union_stpwrds.gz

pig -f filter_user_posts.pig \
    -param user_tokens_in=2015_09_user_tokens.gz/*.gz \
    -param stpwrd_in=2015_09_union_stpwrds.gz/*.gz \
    -param out=2015_09_filtered_user_posts.gz
