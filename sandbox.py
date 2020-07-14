import db_connect

import sqlalchemy
import json

engine = db_connect.get_db_engine()

try:
    q = (
            sqlalchemy.text("SELECT * FROM gene_gpl_ref WHERE gene_symbol = 'AL033328'"),
            {"gene": "AL033328"}
        )

    print(q)
    r = None
    with engine.begin() as con:
        r = con.execute(q[0])
    print(r.fetchone())
    db_connect.cleanup_db_engine()
except Exception as e:
    print(e)
    db_connect.cleanup_db_engine()



