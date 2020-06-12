from sshtunnel import SSHTunnelForwarder

import sqlalchemy



# with SSHTunnelForwarder(
#     ("172.31.100.10", 22),
#     ssh_username="ngeo",
#     ssh_password="g3osp@m!"
# ) as tunnel:
SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://ngeo_grant:mJ1H.aO]u*A)@tOBcU@mariadb.db.ci.its.hawaii.edu/extern_ncbi_geo'

# Test if it works
engine = sqlalchemy.create_engine(SQLALCHEMY_DATABASE_URI, echo=True)
print(engine.table_names())