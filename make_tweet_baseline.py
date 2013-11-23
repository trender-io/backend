import os
import sys
import time
import math
import json
from operator import itemgetter
import numpy as np
import sqlite3

FREQ_QUERY = "SELECT vals.val,COUNT(*) FROM %s tab,vals WHERE " \
             "tab.val_id = vals.val_id AND " \
             "tab.tstamp BETWEEN ? AND ? GROUP BY vals.val ORDER BY COUNT(*) DESC"


dbfile = os.path.join('collected', 'twitter.db')

if not os.path.exists(dbfile):
    print "There is no DB!"
    sys.exit(-1)

db = sqlite3.connect(dbfile)
cursor = db.cursor()
cursor.execute("PRAGMA journal_mode = WAL")

if cursor.fetchone()[0] != "wal":
    print "Could not set journal_mode!"

# get timestamp of oldest data
cursor.execute("SELECT tstamp FROM word ORDER BY tstamp ASC LIMIT 1")
oldest = cursor.fetchone()[0]
now = int(time.time())

terms = set()
docs = []
hours = range(now - 3600, oldest, -3600)

# make a document per hour of historical tweets and compute metrics
for end,start in zip(hours,hours[1:]):
    f = [row for row in cursor.execute(FREQ_QUERY % 'word', (start, end)) if row[0]]
    nword = float(np.sum(map(itemgetter(1), f)))
    terms |= set(map(itemgetter(0), f))
    
    doc = {}
    for w,c in f:
        doc[w] = {'tf': c, 'tf_norm': c / nword * 1000000}
    docs.append(doc)

ndocs = float(len(docs))
freq = {'ndocs': ndocs, 'metrics': {}}

# make global dict of metrics per term
for t in terms:
    tf = 0
    df = 0
    tf_norm = 0
    
    for d in docs:
        if t in d:
            tf += d[t]['tf']
            tf_norm += d[t]['tf_norm']
            df += 1
            
    freq['metrics'][t] = {'tf': tf, 'df': df, 'atf_norm': tf_norm / ndocs, 'idf': math.log(ndocs / df)}

fp = open('twitter_baseline.json', 'w')
json.dump(freq, fp)
fp.close()

db.close()
