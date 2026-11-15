import json
import sqlite3
from ingest import Ingestor
from samples import seed, world

db = seed(sqlite3.connect(':memory:'))
w = world()
ing = Ingestor(db, w)
before = ing.rows('resume')
position = ing.run('resume')
print(json.dumps({'legacy_prefix': before, 'committed': position,
                  'rows': ing.rows('resume'), 'sdk_reads': w.trace},
                 ensure_ascii=False, indent=2))
db.close()
