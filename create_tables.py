import db_connect

def create_gene_gpl_ref():
    engine = db_connect.get_db_engine()
    try:
        with engine.begin() as con:
            create_table = """
                CREATE TABLE gene_gpl_ref (
                    gene_id varchar(50) NOT NULL,
                    gpl varchar(10) NOT NULL,
                    ref_id varchar(255) NOT NULL,
                    PRIMARY KEY (gpl, ref_id)
                    FOREIGN KEY (gene_id) REFERENCES gene_gpl_ref(gene_id)
                );
            """
            con.execute(create_table)
    finally:
        db_connect.cleanup_db_engine()

def create_gene_info():
    engine = db_connect.get_db_engine()
    try:
        with engine.begin() as con:
            create_table = """
                CREATE TABLE gene_info (
                    gene_id varchar(50) NOT NULL,
                    gene_symbol varchar(50) NOT NULL,
                    gene_synonyms varchar(500),
                    gene_description varchar(65535),
                    PRIMARY KEY (gene_id)
                );
            """
            con.execute(create_table)
    finally:
        db_connect.cleanup_db_engine()