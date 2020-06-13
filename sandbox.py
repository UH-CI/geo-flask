from sshtunnel import SSHTunnelForwarder

import sqlalchemy
import json


config_file = "config.json"

config = None
with open(config_file) as f:
    config = json.load(f)

tunnel = None
if config["tunnel"]["use_tunnel"]:
    tunnel_config = config["tunnel"]["tunnel_config"]
    tunnel = SSHTunnelForwarder(
        (tunnel_config["ssh"], tunnel_config["ssh_port"]),
        ssh_username = tunnel_config["user"],
        ssh_password = tunnel_config["password"],
        remote_bind_address = (tunnel_config["remote"], int(tunnel_config["remote_port"])),
        local_bind_address = (tunnel_config["local"], int(tunnel_config["local_port"])) if tunnel_config["local_port"] is not None else (tunnel_config["local"], )
    )

    tunnel.start()

#create and populate sql configuration from config file
sql_config = {}
sql_config["lang"] = config["lang"]
sql_config["connector"] = config["connector"]
sql_config["password"] = config["password"]
sql_config["db_name"] = config["db_name"]
sql_config["user"] = config["user"]
sql_config["port"] = config["port"] if tunnel is None else tunnel.local_bind_port
sql_config["address"] = config["address"] if tunnel is None else tunnel.local_bind_host


#construct sql alchemy uri
# SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://ngeo_grant:mJ1H.aO]u*A)@tOBcU@localhost:%s/extern_ncbi_geo' % tunnel.local_bind_port

SQLALCHEMY_DATABASE_URI = '%s+%s://%s:%s@%s:%s/%s' % (sql_config["lang"], sql_config["connector"], sql_config["user"], sql_config["password"], sql_config["address"], sql_config["port"], sql_config["db_name"])

print(SQLALCHEMY_DATABASE_URI)

create_table = """
    CREATE TABLE test (
        test int NOT NULL,
        test_2 varchar(255),
        PRIMARY KEY (test, test_2)
    );
"""

drop_table = """
    DROP TABLE test;
"""


#create engine from URI
engine = sqlalchemy.create_engine(SQLALCHEMY_DATABASE_URI)

#connect to engine and do database thing
with engine.connect() as con:
    # con.execute(create_table)

    print(engine.table_names())

engine.dispose()

if tunnel is not None:
    tunnel.stop()
