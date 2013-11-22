import os
import sys
import time
import json
import sqlite3

FREQ_QUERY = "SELECT vals.val,COUNT(*) FROM %s tab,vals WHERE " \
             "tab.val_id = vals.val_id AND " \
             "tab.tstamp BETWEEN ? AND ? GROUP BY vals.val ORDER BY COUNT(*) DESC"


outdir_ = 'corpus'
dbfile_ = os.path.join('collected', 'twitter.db')


if not os.path.exists(outdir_):
    os.makedirs(outdir_)

if not os.path.exists(dbfile_):
    print "There is no DB!"
    sys.exit(-1)

db = sqlite3.connect(dbfile_)
cursor = db.cursor()
cursor.execute("PRAGMA journal_mode = WAL")

if cursor.fetchone()[0] != "wal":
    print "Could not set journal_mode!"

# get timestamp of oldest data
cursor.execute("SELECT tstamp FROM word ORDER BY tstamp ASC LIMIT 1")
oldest = cursor.fetchone()[0]
now = int(time.time())

count = 0
hours = range(now - 3600, oldest, -3600)

for end,start in zip(hours,hours[1:]):
    freq = [row for row in cursor.execute(FREQ_QUERY % 'word', (start, end))]
    
    if len(freq) > 0:
        fp = open(os.path.join(outdir_, 'baseline_%05d.json' % count), 'w')
        json.dump(freq, fp)
        fp.close()
        count += 1

db.close()
