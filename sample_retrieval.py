#one process per, since might have multiples?


import sqlite3
import GEOparse

dbf = "E:/ncbigeo/GEOmetadb.sqlite"
cache = "./cache/"

# test_id = "GSM11"
# id_refs = [12, 8000]



def get_value_from_sample_by_id(gsm, id_ref):
    try:
        gsm = GEOparse.get_GEO(geo = gsm, destdir = cache, silent=True)
    except:
        return [] 

    data = gsm.table
    
    values = data.loc[data["ID_REF"] == id_ref].to_dict("records")
    print(values)

    return values



def get_value_from_sample_by_ids(gsm, id_refs):

    gsm = GEOparse.get_GEO(geo = gsm, destdir = cache)

    #print(gsm.table["ID_REF"])

    data = gsm.table

    values = data.loc[data["ID_REF"].isin(id_refs)].to_dict("records")
    
    return values



# print(get_value_from_sample_by_id(test_id, id_refs[0]))



def simplified_list_factory(cursor, row):
    #l = []
    return row[0]

def get_samples_from_platform(gpl):
    con = sqlite3.connect(dbf)
    con.text_factory = lambda b: b.decode(errors = 'ignore')
    con.row_factory = simplified_list_factory
    cur = con.cursor()

    query = """
        SELECT gsm
        FROM gsm
        WHERE gpl == "%s"
    """ % gpl

    results = cur.execute(query).fetchall()

    return results



# samples = get_samples_from_platform("GPL4")
# print(samples)