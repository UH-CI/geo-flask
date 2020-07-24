import flask
from flask import request, jsonify, abort
import sqlite3
import sys
# import sample_retrieval
import db_connect
import atexit
import signal
import sqlalchemy
from sqlalchemy import text
from sqlalchemy import exc
import json
import logging
import ftp_handler

config_file = "config.json"

config = None
with open(config_file) as f:
    config = json.load(f)
    
dbf = config["meta_db_file"]

app = flask.Flask(__name__)
# app.config["DEBUG"] = config["debug"]

engine = db_connect.get_db_engine()

def prep_gene_gpl_ref_insert():
    return text("""
        INSERT INTO gene_gpl_ref (gene_symbol, gene_synonyms, gene_description, gpl, id_ref)
        VALUES (:gene_symbol, :gene_synonyms, :gene_description, :gpl, :id_ref);
    """)

gene_gpl_ref_insert = prep_gene_gpl_ref_insert()


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def filter_single_factory(cursor, row):
    return row[0]


@app.errorhandler(404)
def page_not_found(e):
    return flask.make_response("<h1>404</h1><p>The resource could not be found.</p>", 404)


@app.errorhandler(400)
def bad_req(e):
    return flask.make_response({"message": e.description}, 400)





@app.route("/api/v1/values/gene_info", methods=["GET"])
def api_get_gene_info():
    try:
        query_parameters = request.args
        #anything else you want to query the gene by or filter results by? (use this endpoint)
        gene_symbol = query_parameters.get("symbol")

        if gene_symbol is None:
            abort(400, "Need symbol parameter.")

        #what can be searched on?
        #gpl for reversing from disease, gene symbol (also gene synonyms)
        #note gene synonym query must have wildcard at start so no index, so check main symbol first (always consistent since from same record), then check like first item in list (no initial wildcard), then use initial wildcard as last resort
        #need to index symbol, synonyms, and gpl

        #connection to gene_type db and query on gene type (get gpls and ids)
        #should also check alternative gene names if nothing returned
        #allow gene description query? Probably would want "LIKE" query

        #remember to change table collation to case insensitive

        #queries tiered by speed
        gene_symbol_queries = [
            (
                text("SELECT * FROM gene_gpl_ref WHERE gene_symbol = :gene;"),
                {"gene": gene_symbol}
            ),
            (
                text("SELECT * FROM gene_gpl_ref WHERE gene_synonyms LIKE :gene;"),
                {"gene": "%s%%" % gene_symbol}
            ),
            #last resort, wildcard at start of pattern
            #can just use the find in set operator since can't use the index
            (
                text("SELECT * FROM gene_gpl_ref WHERE FIND_IN_SET(:gene, gene_synonyms);"),
                {"gene": gene_symbol}
            )
        ]

        res = None
        row = None
        with engine.begin() as con:  
            for query in gene_symbol_queries:
                #return value should be similar to cursor results
                res = con.execute(query[0], **query[1])
                #fetch first result to check if anything returned
                row = res.fetchone()
                #got a result, all entries using the same gene should be consistent so good to go?
                #use this for now but...
                #not sure about this because of the inconsistency of orthologs (some had values in the synonyms col where they were the main symbol in others...)
                #should eventually probably just enumerate gene synonyms to separate columns so can index (need index for any kind of speed over all > 2 billion rows that will eventually exist)
                #also remember to refactor table structure, and should add in taxonomic id and something to get species
                if row is not None:
                    break
        #found nothing
        if row is None:
            return jsonify({})

        #row order gene_symbol, gene_synonyms, gene_description, gpl, id_ref

        #gene info same for everything, would be better to have gene info table with gene_symbol as a foreign key

        #when refactor data going to add gene id and tax_id, anything else?
        ret = {
            "gene_symbol": row[0],
            "gene_synonyms": row[1],
            "gene_description": row[2],
            "platforms": {}
        }


        while row is not None:
            gpl = row[3]
            id_ref = row[4]

            id_list = ret["platforms"].get(gpl)
            if id_list is None:
                id_list = []
                ret["platforms"][gpl] = id_list
            id_list.append(id_ref)
            
            row = res.fetchone()

        response = jsonify(ret)
        #FOR DEBUGGING, PROBABLY NEED TO REMOVE THIS
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
    except Exception as e:
        app.logger.error(e)
        abort(500)



@app.route("/api/v1/values/gpl_gse", methods=["GET"])
def api_filter_gpl_gse():
    try:
        query_parameters = request.args
        #anything else you want to query the gene by? (use this endpoint)
        gpl = query_parameters.get("gpl")
        #accessions should be upper case
        gpl = gpl.upper()

        if gpl is None:
            abort(400, "Need gpl parameter.")
        #note sqlite supports == but generally sql has no ==
        query = "SELECT gse FROM gse_gpl WHERE gpl = ?"

        con = sqlite3.connect(dbf)
        con.text_factory = lambda b: b.decode(errors = 'ignore')
        con.row_factory = filter_single_factory
        cur = con.cursor()
        cur.execute(query, [gpl])
        gses = cur.fetchall()

        response = jsonify(gses)
        #FOR DEBUGGING, PROBABLY NEED TO REMOVE THIS
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
    except Exception as e:
        app.logger.error(e)
        abort(500)

#may be cases where need gpls from gses based on disease
@app.route("/api/v1/values/gse_gpl", methods=["GET"])
def api_filter_gse_gpl():
    try:
        query_parameters = request.args
        #anything else you want to query the gene by? (use this endpoint)
        gse = query_parameters.get("gse")
        #accessions should be upper case
        gse = gse.upper()

        if gse is None:
            abort(400, "Need gse parameter.")
        #note sqlite supports == but generally sql has no ==
        query = "SELECT gpl FROM gse_gpl WHERE gse = ?"

        con = sqlite3.connect(dbf)
        con.text_factory = lambda b: b.decode(errors = 'ignore')
        con.row_factory = filter_single_factory
        cur = con.cursor()
        cur.execute(query, gse)
        gpls = cur.fetchall()

        response = jsonify(gpls)
        #FOR DEBUGGING, PROBABLY NEED TO REMOVE THIS
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
    except Exception as e:
        app.logger.error(e)
        abort(500)

    


@app.route("/api/v1/values/gse_values", methods=["GET"])
def api_filter_gse_values():
    try:
        #need gse, gpl, and id_refs
        #if id refs none then just return all data
        query_parameters = request.args

        gse = query_parameters.get("gse")
        gpl = query_parameters.get("gpl")
        id_refs = query_parameters.get("id_refs")

        #trust that gpl and gse match, otherwise should catch issue when returning data
        #if no resource found when getting ftp data then note to check platform matches sample

        #must provide series and platforms
        if gse is None or gpl is None:
            abort(400, "Need gse and gpl.")

        #capitalize the accessions since they should be anyway
        gse = gse.upper()
        gpl = gpl.upper()

        #break into lists to allow multiples
        gse_list = gse.split(",")
        gpl_list = gpl.split(",")
        if id_refs is not None:
            id_refs = id_refs.split(",")
        #if only one platform provided expand that platform to all provided series
        if len(gpl_list) == 1:
            gpl_list = [gpl_list[0] for gse in gse_list]
        #must provide gpls for each series provided or one gpl for all of them
        if len(gse_list) != len(gpl_list):
            abort(400, "gse and gpl lists must match length.")

        #create pairs out of items
        gse_gpl_pairs = list(zip(gse_list, gpl_list))

        data = {}
        for pair in gse_gpl_pairs:
            try:
                data[pair[0]] = ftp_handler.get_gse_data(*pair, id_refs)
            except ValueError as e:
                abort(400, str(e))
            #runtime error would be 500 error so can just let it be thrown, will go to main exception

        response = jsonify(data)
        #FOR DEBUGGING, PROBABLY NEED TO REMOVE THIS
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response
    except Exception as e:
        app.logger.error(e)
        abort(500)
    
    


#type=gene_type
#return {
#   gene_name: gene_name
#   gene_name_alt: gene_name_alternatives
#   gene_description: gene_description
#   platforms: {
#       platform_id: {
#           sample_id: value_column
#           ...
#       }
#       ...
#   }
# }
# @app.route("/api/v1/values", methods=["GET"])
# def api_filter_values():
#     try:
#         query_parameters = request.args
#         gene_symbol = query_parameters.get("symbol")

#         #what can be searched on?
#         #gpl for reversing from disease, gene symbol (also gene synonyms)
#         #note gene synonym query must have wildcard at start so no index, so check main symbol first (always consistent since from same record), then check like first item in list (no initial wildcard), then use initial wildcard as last resort
#         #need to index symbol, synonyms, and gpl

#         #connection to gene_type db and query on gene type (get gpls and ids)
#         #should also check alternative gene names if nothing returned
#         #allow gene description query? Probably would want "LIKE" query

#         #remember to change table collation to case insensitive

#         #queries tiered by speed
#         gene_symbol_queries = [
#             (
#                 text("SELECT * FROM gene_gpl_ref WHERE gene_symbol = :gene;"),
#                 {"gene": gene_symbol}
#             ),
#             (
#                 text("SELECT * FROM gene_gpl_ref WHERE gene_synonyms LIKE :gene;"),
#                 {"gene": "%s%%" % gene_symbol}
#             ),
#             #last resort, wildcard at start of pattern
#             #can just use the find in set operator since can't use the index
#             (
#                 text("SELECT * FROM gene_gpl_ref WHERE FIND_IN_SET(:gene, gene_synonyms);"),
#                 {"gene": gene_symbol}
#             )
#         ]

#         res = None
#         row = None
#         with engine.begin() as con:  
#             for query in gene_symbol_queries:
#                 #return value should be similar to cursor results
#                 res = con.execute(query[0], **query[1])
#                 #fetch first result to check if anything returned
#                 row = res.fetchone()
#                 #got a result, all entries using the same gene should be consistent so good to go?
#                 #use this for now but...
#                 #not sure about this because of the inconsistency of orthologs (some had values in the synonyms col where they were the main symbol in others...)
#                 #should eventually probably just enumerate gene synonyms to separate columns so can index (need index for any kind of speed over all > 2 billion rows that will eventually exist)
#                 #also remember to refactor table structure, and should add in taxonomic id and something to get species
#                 if row is not None:
#                     break

#         if row is None:
#             return jsonify({})

#         #row order gene_symbol, gene_synonyms, gene_description, gpl, id_ref

#         #gene info same for everything, would be better to have gene info table with gene_symbol as a foreign key

#         ret = {
#             "gene_symbol": row[0],
#             "gene_synonyms": row[1],
#             "gene_description": row[2],
#             "platforms": {}
#         }


#         count = 0
#         while row is not None:
#             print(row)
#             count += 1
#             row = res.fetchone()
#             continue
#             gpl = row[3]
#             id_ref = row[4]

#             gsms = sample_retrieval.get_samples_from_platform(gpl)
#             ret["platforms"][gpl] = {}
#             for gsm in gsms:
#                 values = sample_retrieval.get_value_from_sample_by_id(gsm, id_ref)
#                 ret["platforms"][gpl][gsm] = values
            
#             row = res.fetchone()

#         print(count)

#         return jsonify(ret)
#     except Exception as e:
#         app.logger.error(e)
#         abort(500)





#proably going to bypass this
@app.route("/api/v1/gene_info", methods = ["POST"])
def api_create_gene_info():
    try:
        #reconstruct request to ensure required fields
        formatted_req = {
            "gene_symbol": request.get_json(force=True).get("gene_symbol"),
            "gene_synonyms": request.get_json(force=True).get("gene_synonyms"),
            "gene_description": request.get_json(force=True).get("gene_description"),
            "gpl": request.get_json(force=True).get("gpl"),
            "id_ref": request.get_json(force=True).get("id_ref")
        }
    except Exception as e:
        app.logger.error(e)
        abort(500)
    
    #make sure not null fields are provided otherwise abort with 400 (bad requset)
    if formatted_req["gene_symbol"] is None or formatted_req["gpl"] is None or formatted_req["id_ref"] is None:
        abort(400, "Must provide gene_symbol, gpl, and id_ref fields.")

    try:
        with engine.begin() as con:
            con.execute(gene_gpl_ref_insert, **formatted_req)
    except exc.IntegrityError as e:
        abort(400, "A conflict has occured with the provided values. Must have a unique gpl, id_ref combination.")
    except Exception as e:
        app.logger.error(e)
        abort(500)
    

    #abort(400)
    return flask.make_response(formatted_req, 201)

# gene_symbol varchar(50) NOT NULL,
# gene_synonyms varchar(255),
# gene_description varchar(21000),
# gpl varchar(10) NOT NULL,
# id_ref varchar(255) NOT NULL,
@app.route("/api/v1/gene_gpl_ref", methods = ["POST"])
def api_create_gene_gpl_ref():
    # global engine
    # global gene_gpl_ref_insert

    try:
        #reconstruct request to ensure required fields
        formatted_req = {
            "gene_symbol": request.get_json(force=True).get("gene_symbol"),
            "gene_synonyms": request.get_json(force=True).get("gene_synonyms"),
            "gene_description": request.get_json(force=True).get("gene_description"),
            "gpl": request.get_json(force=True).get("gpl"),
            "id_ref": request.get_json(force=True).get("id_ref")
        }
    except Exception as e:
        app.logger.error(e)
        abort(500)
    
    #make sure not null fields are provided otherwise abort with 400 (bad requset)
    if formatted_req["gene_symbol"] is None or formatted_req["gpl"] is None or formatted_req["id_ref"] is None:
        abort(400, "Must provide gene_symbol, gpl, and id_ref fields.")

    try:
        with engine.begin() as con:
            con.execute(gene_gpl_ref_insert, **formatted_req)
    except exc.IntegrityError as e:
        abort(400, "A conflict has occured with the provided values. Must have a unique gpl, id_ref combination.")
    except Exception as e:
        app.logger.error(e)
        abort(500)
    

    #abort(400)
    return flask.make_response(formatted_req, 201)


#ask sean best way to implement this
@app.route("/api/v1/gene_gpl_ref/delete", methods=["POST"])
def api_delete_values():
    abort(501)


# def prepare_sql_insert(table, fields, json):
#     sql = "INSERT INTO %s (" % table

#     params = []

#     formatted_req = {}

#     for field in fields:
#         value = json.get(field)
#         if value is not None:
#             sql += "%s, " % field
#             param = ":%s" % field
#             params.append(param)
#         formatted_req[field] = value

#     sql = sql[:-2]
#     sql += ") VALUES ("

#     for param in params:
#         sql += "%s, " % param

#     sql = sql[:-2]
#     sql += ");"

#     q = (text(sql), formatted_req)

#     return q







@app.route("/api/v1/metadata/gsm", methods=["GET"])
def api_filter_metadata_gsm():
    query_parameters = request.args
    valid_fields = ["gsm"]

    query = construct_query_from_params("gsm", valid_fields, query_parameters)

    con = sqlite3.connect(dbf)
    con.text_factory = lambda b: b.decode(errors = 'ignore')
    con.row_factory = dict_factory
    cur = con.cursor()

    # print(parameters, file = sys.stderr)
    # print(query, file = sys.stderr)
    results = cur.execute(query[0], query[1]).fetchall()

    return jsonify(results)


@app.route("/api/v1/metadata/gse", methods=["GET"])
def api_filter_metadata_gse():
    query_parameters = request.args
    valid_fields = ["gse"]

    query = construct_query_from_params("gse", valid_fields, query_parameters)

    con = sqlite3.connect(dbf)
    con.text_factory = lambda b: b.decode(errors = 'ignore')
    con.row_factory = dict_factory
    cur = con.cursor()

    # print(parameters, file = sys.stderr)
    # print(query, file = sys.stderr)
    results = cur.execute(query[0], query[1]).fetchall()

    return jsonify(results)




@app.route("/api/v1/metadata/gpl", methods=["GET"])
def api_filter_metadata_gpl():
    query_parameters = request.args
    valid_fields = ["gpl"]

    query = construct_query_from_params("gpl", valid_fields, query_parameters)

    con = sqlite3.connect(dbf)
    con.text_factory = lambda b: b.decode(errors = 'ignore')
    con.row_factory = dict_factory
    cur = con.cursor()

    #print(query, file = sys.stderr)
    # print(query, file = sys.stderr)
    results = cur.execute(query[0], query[1]).fetchall()

    return jsonify(results)



#format select=selection1,selection2,...&field=value1,value2,...
#don't worry about projections for now
def construct_query_from_params(table, valid_fields, query_parameters):
    #projection = query_parameters.get("select")

    query_filters = []
    #execute parameters should deal with sanitization
    parameters = []
    for field in valid_fields:
        values = query_parameters.get(field)
        if values is not None:
            #split on commas for "OR"
            values = values.split(",")
            query_filters.append((field, values))

    #assumes value list length at least 1, should always be true
    def construct_in_statement(query_filter, parameters):
        statement = ""
        field = query_filter[0]
        values = query_filter[1]

        #do first instance separate so no "or"
        value = values[0]
        statement += "%s COLLATE NOCASE IN (?" % field
        parameters.append(value)

        for i in range(1, len(values)):
            value = values[i]
            statement += ",?"
            parameters.append(value)

        statement += ")"

        # print(parameters, file = sys.stderr)
        return statement

    def construct_where_statement(query_filters, parameters):
        statement = ""
        if len(query_filters) > 0:
            query_filter = query_filters[0]
            statement += "WHERE %s" % construct_in_statement(query_filter, parameters)

            for i in range(1, len(query_filters)):
                query_filter = query_filters[i]
                statement += " AND %s" % construct_in_statement(query_filter, parameters)


        return statement

    query = """
        SELECT *
        FROM %s
        %s;
    """ % (table, construct_where_statement(query_filters, parameters))

    return (query, parameters)


def cleanup():
    db_connect.cleanup_db_engine()

def cleanup_sig(sig, frame):
    cleanup()
    exit(0)

atexit.register(cleanup)
signal.signal(signal.SIGINT, cleanup_sig)

#should only run if file run directly, not through gunicorn
if __name__ == "__main__":
    app.logger.setLevel(logging.DEBUG)
    #debug breaks with db connection stuff, reload doesn't work properly
    app.run(debug = False, threaded = True, processes = 1, host = "0.0.0.0")
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)