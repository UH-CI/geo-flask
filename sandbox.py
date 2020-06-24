import db_connect

import sqlalchemy
import json
import atexit

engine = db_connect.get_db_engine()

q = engine.execute("SELECT * FROM gene_gpl_ref")
print(q.fetchall())

db_connect.cleanup_db_engine()

def test():
    print("test")

atexit.register(test)



