config_file = "config.json"

engine = None
tunnel = None

def get_db_engine():

    if engine is not None:
        return engine

    config = None
    with open(config_file) as f:
        config = json.load(f)

    
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

        SQLALCHEMY_DATABASE_URI = '%s+%s://%s:%s@%s:%s/%s' % (sql_config["lang"], sql_config["connector"], sql_config["user"], sql_config["password"], sql_config["address"], sql_config["port"], sql_config["db_name"])

        #create engine from URI
        engine = sqlalchemy.create_engine(SQLALCHEMY_DATABASE_URI)

        return engine


def cleanup_db_engine():
    if engine is not None:
        engine.stop()
    if tunnel is not None:
        tunnel.stop()