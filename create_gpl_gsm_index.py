
import sqlite3
import sys

dbf = "E:/ncbigeo/GEOmetadb.sqlite"
con = sqlite3.connect(dbf)
con.text_factory = lambda b: b.decode(errors = 'ignore')
cur = con.cursor()

index_gen = """
    CREATE INDEX gpl_gsm
    ON gsm(gpl)
""" 

cur.execute(index_gen)