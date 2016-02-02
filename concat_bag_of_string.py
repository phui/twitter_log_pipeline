import re
from itertools import chain



# token filters
mention_regex = re.compile(r'\@\w+')
http_regex = re.compile(r'https*\:\/\/\S+')
hash_regex = re.compile(r'\#\w+')
digit_check = lambda s: s.isdigit()



# stopword list, copy from stopword corpus in NLTK
stopwords = set([
    u'i', u'me', u'my', u'myself', u'we', u'our', u'ours', u'ourselves',
    u'you', u'your', u'yours', u'yourself', u'yourselves', u'he', u'him',
    u'his', u'himself', u'she', u'her', u'hers', u'herself', u'it', u'its',
    u'itself', u'they', u'them', u'their', u'theirs', u'themselves',
    u'what', u'which', u'who', u'whom', u'this', u'that', u'these',
    u'those', u'am', u'is', u'are', u'was', u'were', u'be', u'been',
    u'being', u'have', u'has', u'had', u'having', u'do', u'does', u'did',
    u'doing', u'a', u'an', u'the', u'and', u'but', u'if', u'or', u'because',
    u'as', u'until', u'while', u'of', u'at', u'by', u'for', u'with',
    u'about', u'against', u'between', u'into', u'through', u'during',
    u'before', u'after', u'above', u'below', u'to', u'from', u'up', u'down',
    u'in', u'out', u'on', u'off', u'over', u'under', u'again', u'further',
    u'then', u'once', u'here', u'there', u'when', u'where', u'why', u'how',
    u'all', u'any', u'both', u'each', u'few', u'more', u'most', u'other',
    u'some', u'such', u'no', u'nor', u'not', u'only', u'own', u'same',
    u'so', u'than', u'too', u'very', u's', u't', u'can', u'will', u'just',
    u'don', u'should', u'now', 'rt' # add "RT" to filter retweet symbol
])



@outputSchema("schema:chararray")
def convertBagToStr(acctBag):
    # flatten list of post tuples into tokens
    tokens = [t[0].split(' ') for t in acctBag]

    # strip and lowercase the tokens
    tokens = set(item.strip('.,!:?-()\"\'\n').lower() for sublist in tokens for item in sublist)

    # filter on all predefined conditions
    tokens = set(token for token in tokens
        if
            len(token) > 1 and # meaningful string
            mention_regex.match(token) is None and # not a mention token
            http_regex.match(token) is None and # not a http url token
            hash_regex.match(token) is None and # not a hashtag token
            not all(map(digit_check, token.split('.'))) and # not a number
            token not in stopwords # not a stopword
    )

    # N choose 2 > 32 implies N > 8
    if len(tokens) <= 8:
        return u'0' # N <= 8 return Nan synbol

    # return Python string serialization of set of unicode
    return str(tokens)
