import db_connect

import sqlalchemy
import json

engine = db_connect.get_db_engine()

md = sqlalchemy.MetaData()
table = sqlalchemy.Table('gene_gpl_ref', md, autoload=True, autoload_with=engine)
columns = table.c

for item in columns:
    print(item.name)
    print(item.type)
# gene_symbol = "Bap18"
# try:
#     gene_symbol_queries = [
#         (
#             sqlalchemy.text("SELECT * FROM gene_gpl_ref WHERE gene_symbol = :gene;"),
#             {"gene": gene_symbol}
#         ),
#         (
#             sqlalchemy.text("SELECT * FROM gene_gpl_ref WHERE gene_synonyms LIKE :gene;"),
#             {"gene": "%s%%" % gene_symbol}
#         ),
#         #last resort, wildcard at start of pattern
#         #can just use the find in set operator since can't use the index
#         (
#             sqlalchemy.text("SELECT * FROM gene_gpl_ref WHERE FIND_IN_SET(:gene, gene_synonyms);"),
#             {"gene": gene_symbol}
#         )
#     ]

#     res = None
#     row = None
#     with engine.begin() as con:  
#         for query in gene_symbol_queries:
#             #return value should be similar to cursor results
#             res = con.execute(query[0], **query[1])
#             #fetch first result to check if anything returned
#             row = res.fetchone()
#             #got a result, all entries using the same gene should be consistent so good to go?
#             #use this for now but...
#             #not sure about this because of the inconsistency of orthologs (some had values in the synonyms col where they were the main symbol in others...)
#             #should eventually probably just enumerate gene synonyms to separate columns so can index (need index for any kind of speed over all > 2 billion rows that will eventually exist)
#             #also remember to refactor table structure, and should add in taxonomic id and something to get species
#             if row is not None:
#                 break

#     print(row)
db_connect.cleanup_db_engine()
# except Exception as e:
#     print(e)
#     db_connect.cleanup_db_engine()



