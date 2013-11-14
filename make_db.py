import os
import sqlite3

def build_table(db, file, table, area):
    fp = open(file, 'r')    
    lines = fp.readlines()
    fp.close()
    
    if not lines:
        return
        
    cursor = db.cursor()
        
    for l in lines:
        parts = l.split()
        
        if len(parts) < 2:
            continue
        
        time = parts[0][:parts[0].rfind('.')]
        for p in parts[1:]:
            cursor.execute("INSERT INTO %s VALUES (datetime(?, 'unixepoch'), ?)" % table, (int(time), p))


TABLES = ["word", "hash", "link"]
AREAS = ["boston", "new_york", "los_angeles", "san_francisco", "chicago"]

# globals. coul be passed params
indir_ = 'collected'
dbfile_ = 'twitter.db'

if os.path.exists(dbfile_):
    os.remove(dbfile_)

db = sqlite3.connect(dbfile_)
cursor = db.cursor()

for table in TABLES:
    cursor.execute("CREATE TABLE %s (tstamp TIMESTAMP, val TEXT)" % table)
    db.commit()
    
    for area in AREAS:
        build_table(db, os.path.join(indir_, table + '_' + area + '.txt'), table, area)
        db.commit()
        
db.close()
