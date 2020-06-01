import flask
from flask import request, jsonify, abort
import sqlite3
import sys

dbf = "E:/ncbigeo/GEOmetadb.sqlite"

app = flask.Flask(__name__)
app.config["DEBUG"] = True

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


@app.errorhandler(404)
def page_not_found(e):
    return flask.make_response("<h1>404</h1><p>The resource could not be found.</p>", 404)






@app.route('/api/v1/values', methods=['GET'])
def api_filter_values():
    for item in request.args:
        print(item)
    results = {}
    return jsonify(results)


@app.route('/api/v1/values', methods=['POST'])
def api_create_values():
    print(request.json)
    abort(400)
    return flask.make_response({"value": "test"}, 201)


def create_sql_insert(table, fields, json):
    sql = "INSERT INTO %s (" % table

    values = []

    for field in fields:
        value = json.get(field)
        if value is not None:
            sql += "%s, " % field
            values.append(value)
    
    sql = sql[:-2]
    sql += ") VALUES ("

    for value in values:
        sql += "%s, " % value

    sql = sql[:-2]
    sql += ");"

    return sql


@app.route('/api/v1/metadata', methods=['GET'])
def api_filter_metadata():
    query_parameters = request.args

    query_filters = [
        ("title", query_parameters.get("title"))
    ]

    gpl = query_parameters.get('gpl')
    published = query_parameters.get('published')
    author = query_parameters.get('author')

    query = "SELECT * FROM gpl WHERE"
    parameters = []

    query_filter = query_filters[0]
    query += " %s = ?" % query_filter[0]
    parameters.append(query_filter[1])

    for i in range(1, len(query_filters)):
        query_filter = query_filters[i]
        query += " AND %s = ?" % query_filter[0]
        parameters.append(query_filter[1])

    query += ";"

    con = sqlite3.conect(dbf)
    con.row_factory = dict_factory
    cur = con.cursor()

    print(parameters, file = sys.stderr)
    print(query, file = sys.stderr)
    results = cur.execute(query, parameters).fetchall()

    return jsonify(results)

#format select=selection1,selection2,...&field=value1,value2,...
def construct_query_from_params(valid_fields, query_parameters):
    projection = query_parameters.get("select")

    query_filters = []
    

#can change number of processes, but need to be aware of potential affects on r stuff
#also, gunicorn can handle this stuff
app.run(threaded = True, processes = 1)