import os
import sys
import time
import json
import glob
import sqlite3

AREAS = ["boston", "new_york", "los_angeles", "san_francisco", "chicago"]

FREQ_QUERY = "SELECT vals.val,COUNT(*) FROM %s tab,vals WHERE " \
             "tab.city_id = (SELECT city_id FROM cities WHERE name=?) AND " \
             "tab.val_id = vals.val_id AND " \
             "tab.tstamp BETWEEN ? AND ? GROUP BY vals.val ORDER BY COUNT(*) DESC"


def run_tfidf(dbcursor, city, start, end):
    # load the baseline corpus if this is the first invocation
    if not docs_:
        for f in glob.glob(os.path.join(corpus_, 'baseline_*.json')):
            fp = open(f, 'r')
            doc = json.load(fp)
            fp.close()
            
            if len(doc) > 0:
                docs_.append(doc)
                
    ndoc = len(docs_)

    return []


# the baseline tweet word corpus for TF-IDF analysis
corpus_ = 'corpus'
docs_ = []

dbfile_ = os.path.join('collected', 'twitter.db')


if not os.path.exists(dbfile_):
    print "MISSING TWITTER DB!"
    sys.exit()
    
db = sqlite3.connect(dbfile_)
cursor = db.cursor()
cursor.execute("PRAGMA journal_mode = WAL")

if cursor.fetchone()[0] != "wal":
    print "Could not set journal_mode!"

kw = {}
now = int(time.time())

# for each city, get the word frequencies and RAKE keywords over a few time periods
for city in AREAS:
    kw[city] = run_tfidf(cursor, city, now - 3600, now)[:20]

print json.dumps(kw)
db.close()
