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
            cursor.execute("INSERT OR IGNORE INTO vals(val) VALUES (?)", (p,))
            cursor.execute("INSERT INTO %s VALUES " \
                           "(?, (SELECT city_id FROM cities WHERE name=?), (SELECT val_id FROM vals WHERE val=?))" % table, \
                           (int(time), area, p))


TABLES = ["word", "hash"]
AREAS = ["boston", "new_york", "los_angeles", "san_francisco", "chicago"]

# globals. coul be passed params
indir_ = 'collected'
dbfile_ = 'twitter.db'

if os.path.exists(dbfile_):
    os.remove(dbfile_)

db = sqlite3.connect(dbfile_)
cursor = db.cursor()

cursor.execute("CREATE TABLE vals (val_id INTEGER PRIMARY KEY ASC, val TEXT NOT NULL UNIQUE)")
cursor.execute("CREATE TABLE cities (city_id INTEGER PRIMARY KEY ASC, name TEXT NOT NULL UNIQUE)")
for city in AREAS:
    cursor.execute("INSERT INTO cities(name) VALUES (?)", (city,))
db.commit()    

for table in TABLES:
    cursor.execute("CREATE TABLE %s (tstamp INT, city_id INTEGER, val_id INTEGER)" % table)
    db.commit()
    
    for area in AREAS:
        build_table(db, os.path.join(indir_, table + '_' + area + '.txt'), table, area)
        db.commit()
        
db.close()
