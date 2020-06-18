
import db_connect

engine = db_connect.get_db_engine()

with engine.begin() as con:
    con.execute("DELETE FROM gene_gpl_ref")

db_connect.cleanup_db_engine()
