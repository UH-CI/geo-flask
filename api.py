import flask
from flask import request, jsonify, abort
import sqlite3
import sys
import sample_retrieval
import db_connect
import atexit
import signal
import sqlalchemy
from sqlalchemy import text
from sqlalchemy import exc
import json

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
        INSERT INTO gene_gpl_ref (gene_symbol, gene_synonyms, gene_description, gpl, ref_id)
        VALUES (:gene_symbol, :gene_synonyms, :gene_description, :gpl, :ref_id);
    """)

gene_gpl_ref_insert = prep_gene_gpl_ref_insert()


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


@app.errorhandler(404)
def page_not_found(e):
    return flask.make_response("<h1>404</h1><p>The resource could not be found.</p>", 404)


@app.errorhandler(400)
def bad_req(e):
    return flask.make_response({"message": e.description}, 400)





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
@app.route("/api/v1/values", methods=["GET"])
def api_filter_values():
    query_parameters = request.args
    gene_type = query_parameters.get("type")

    #connection to gene_type db and query on gene type (get gpls and ids)
    #should also check alternative gene names if nothing returned
    #allow gene description query? Probably would want "LIKE" query

    ret = {
        "gene_name": gene_type,
        "gene_name_alt": [],
        "gene_description": "temp",
        "platforms": {}
    }

    gpl_id = {

    }
    for gpl in gpl_id:
        ref_id = gpl_id[gpl]
        gsms = sample_retrieval.get_samples_from_platform(gpl)
        ret["platforms"][gpl] = {}
        for gsm in gsms:
            values = sample_retrieval.get_value_from_sample_by_id(gsm, ref_id)
            ret["platforms"][gpl][gsm] = values

    return ret


# gene_symbol varchar(50) NOT NULL,
# gene_synonyms varchar(255),
# gene_description varchar(21000),
# gpl varchar(10) NOT NULL,
# ref_id varchar(255) NOT NULL,
@app.route("/api/v1/gene_gpl_ref", methods=["POST"])
def api_create_values():
    # global engine
    # global gene_gpl_ref_insert

    #reconstruct request to ensure required fields
    formatted_req = {
        "gene_symbol": request.get_json(force=True).get("gene_symbol"),
        "gene_synonyms": request.get_json(force=True).get("gene_synonyms"),
        "gene_description": request.get_json(force=True).get("gene_description"),
        "gpl": request.get_json(force=True).get("gpl"),
        "ref_id": request.get_json(force=True).get("ref_id")
    }
    
    #make sure not null fields are provided otherwise abort with 400 (bad requset)
    if formatted_req["gene_symbol"] is None or formatted_req["gpl"] is None or formatted_req["ref_id"] is None:
        abort(400, "Must provide gene_symbol, gpl, and ref_id fields.")

    try:
        print(engine.table_names())
        with engine.begin() as con:
            con.execute(gene_gpl_ref_insert, **formatted_req)
    except exc.IntegrityError as e:
        abort(400, "A conflict has occured with the provided values. Must have a unique gpl, gene_symbol combination.")
    except Exception as e:
        print(e, file = sys.stderr)
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
    #debug breaks with db connection stuff, reload doesn't work properly
    app.run(debug = False, threaded = True, processes = 1, host = "0.0.0.0")
