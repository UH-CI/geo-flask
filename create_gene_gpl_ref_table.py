import db_connect

engine = db_connect.get_db_engine()
try:
    with engine.begin() as con:
        create_table = """
            CREATE TABLE gene_gpl_ref (
                gene_symbol varchar(50) NOT NULL,
                gene_synonyms varchar(255),
                gene_description varchar(21000),
                gpl varchar(10) NOT NULL,
                ref_id varchar(255) NOT NULL,
                PRIMARY KEY (gene_symbol, gpl)
            );
        """
        con.execute(create_table)
finally:
    db_connect.cleanup_db_engine()