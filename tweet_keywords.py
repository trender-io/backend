import os
import sys
import time
import json
import glob
import re
from operator import itemgetter
from collections import Counter
import sqlite3

AREAS = ["boston", "new_york", "los_angeles", "san_francisco", "chicago"]

TWEET_QUERY = "SELECT tweet FROM tweets WHERE tstamp BETWEEN ? AND ?"

SENTENCE_DELIM = re.compile(u'[.!?,;:\t\\-\\"\\(\\)\u2019\u2013]')
    

def extract_terms(tweet):
    global stoplist
    terms = []
    sentences = SENTENCE_DELIM.split(tweet.lower())
    nwords = 0
    
    for sentence in sentences:
        if not sentence: 
            continue
        
        # unigrams
        words = sentence.split()
        nwords += len(words)
        terms.extend([w for w in words if w not in stoplist])
        
        # bigrams
        for w1,w2 in zip(words, words[1:]):
            if (not w1 in stoplist) and (not w2 in stoplist):
                terms.append("%s %s" % (w1, w2))
            
    return terms,nwords


def compute_keywords(dbcursor, start, end, limit):
    global baseline
    global thresh
    
    # compute average metrics from baseline to deal with unknown terms
    idf_mean = 0.
    atf_norm_mean = 0.
    
    for m in baseline['metrics'].values():
        idf_mean += m['idf']
        atf_norm_mean += m['atf_norm']
        
    idf_mean /= len(baseline['metrics'])
    atf_norm_mean /= len(baseline['metrics'])
    
    # get tweets for the last hour
    tweets = [row[0] for row in dbcursor.execute(TWEET_QUERY, (start, end))]
    count = Counter()
    nwords = 0.
    
    # extract terms from each tweet and update document metrics
    for t in tweets:
        terms,n = extract_terms(t)
        count.update(terms)
        nwords += n
    
    # filter out terms that do not appear that often
    freq = [(k,c) for k,c in count.items() if c >= thresh]
    
    # first method - compute a trendings core using the normalized term frequency from this
    # document and the average normalized term frequency from the baseline
    ts = sorted([(k, (c / nwords * 1000000) / (baseline['metrics'][k]['atf_norm'] if k in baseline['metrics'] else atf_norm_mean)) for k,c in freq], 
                key=itemgetter(1), reverse=True)[:limit]
    
    # second method - use tf-idf scores to determine term popularity
    tf_idf = sorted([(k, c * (baseline['metrics'][k]['idf'] if k in baseline['metrics'] else idf_mean)) for k,c in freq], 
                    key=itemgetter(1), reverse=True)[:limit]
    
    return {'ts': ts, 'tf_idf': tf_idf}
    

limit = 20
thresh = 10
dbfile = os.path.join('collected', 'twitter.db')
baselinefile = 'twitter_baseline.json'
stoplistfile = 'twitter_stoplist.txt'

if not os.path.exists(dbfile):
    print "MISSING TWITTER DB!"
    sys.exit()
    
if not os.path.exists(baselinefile):
    print "MISSING TWITTER BASELINE!"
    sys.exit()
    
if not os.path.exists(stoplistfile):
    print "MISSING TWITTER STOPLIST!"
    sys.exit()
    
fp = open(baselinefile, 'r')
baseline = json.load(fp)
fp.close()

stoplist = set()
fp = open(stoplistfile, 'r')
for l in fp.readlines():
    stoplist |= set(l.lower().rstrip().split())
stoplist |= set([s.replace("'",'') for s in stoplist if "'" in s])
stoplist |= set([s for s in baseline['metrics'].keys() if baseline['metrics'][s]['df'] / baseline['ndocs'] >= 0.75])
fp.close()

db = sqlite3.connect(dbfile)
cursor = db.cursor()
cursor.execute("PRAGMA journal_mode = WAL")

if cursor.fetchone()[0] != "wal":
    print "Could not set journal_mode!"

now = int(time.time())
print json.dumps(compute_keywords(cursor, now - 3600, now, limit))

db.close()
