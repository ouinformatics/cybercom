from pymongo import Connection
from datetime import datetime
from json_handler import handler
import ast
import iso8601
import simplejson as json
import geojson
from bson.code import Code



def find( db=None, col=None, query=None, callback=None, 
            showids=False, date=None):
    """Find data from a specific mongoDB db and collection
        :param db: Optional, mongodb database, if not specified a list of dbs is returned
        :param col: Optional, mongodb collection
        :param query: Optional, query provided as a python dictionary (see pymongo and mongodb docs for query syntax)
        :param callback: Optional, used for returning output as JSONP
        :param showids: Optional, return mongodb _id's
        :param date: Optional, helper for simpler syntax in date range queries (broken)
    
        At the moment this method assumes you want output as JSON, should probably refactor to default to dict and
        allow options for JSON/JSONP
    """
    con = Connection()
    # if db is set create db object, else show db names
    if db:
        db = con[db]
    else:
        return json.dumps(con.database_names())
    # If collection is set return records, else show collection names
    if col:
        col = db[col]
    else:
        return json.dumps(db.collection_names())

    dump_out = []

    if date:
        try:
            column, start, stop = date.split(',')
        except:
            pass
        start = iso8601.parse_date(start)
        stop = iso8601.parse_date(stop)
        date_spec = { column : { "$gte": start, "$lte": stop } }

    if date and query:
        query = ast.literal_eval(query)
        q = query["spec"]
        q = [ (k, v) for k, v in query['spec'].items() ]
        [ q.append(item) for item in date_spec.items() ]
        query['spec'] = dict(q) 
    elif date and not query:
        query={}
        query['spec'] = date_spec
    elif query and not date:
        query = ast.literal_eval(query)
    else:
        pass
        
    # If query set, run query options through pymongo find, else show all records
    if query:
        cur = col.find(**query)
    else:
        cur = col.find()

   
    for item in cur:
        if showids:
            dump_out.append(item)
        else:
            item.pop('_id')
            dump_out.append(item)
    serialized = json.dumps(dump_out, default = handler, sort_keys=True, indent=4)
    if callback is not None:
        return str(callback) + '(' + serialized + ')'
    else:
        return serialized

def distinct(db=None, col=None, distinct_key=None, query=None, callback=None):
    con = Connection()

    if db:
        db=con[db]
    else:
        return json.dumps(con.database_name())

    if col:
        col = db[col]
    else:
        return json.dumps(db.collection_names())

    dump_out = []
    if query and distinct:
        query = ast.literal_eval(query)
        cur = col.find(**query).distinct(distinct_key)
        for item in cur:
            dump_out.append(item)
        if callback:
            return str(callback) + '(' + json.dumps(dump_out, default = handler, sort_keys=True, indent=4) + ')'
        else:
            return json.dumps(dump_out, default = handler, sort_keys=True, indent=4)
    else:
        return json.dumps({ "error": "You must supply a distinct_key and query specification"})

def group(db=None, col=None, key=None,variable=None,query=None, callback=None):
    """Perform group by aggregations from a specific mongoDB db and collection
        :param db: Optional, mongodb database, if not specified a list of dbs is returned
        :param col: Optional, mongodb collection, if not specified a list of collections is returned
        :param key: Optional, List for keys to Group By(['key1','key2']), if not specified list available keys
        :param variable: varaible to sum,average, and count, if not specified list available variables
        :param query: Optional, query provided as a python dictionary (see pymongo and mongodb docs for query syntax)
        :param callback: Optional, used for returning output as JSONP
    
        At the moment this method assumes you want output as JSON, should probably refactor to default to dict and
        allow options for JSON/JSONP
    """
    con = Connection()
    # if db is set create db object, else show db names
    if db:
        db = con[db]
    else:
        return json.dumps(con.database_names())
    # If collection is set return records, else show collection names
    if col:
        col = db[col]
    else:
        return json.dumps(db.collection_names())

    dump_out = []
    # If query set, run query options through pymongo find, else show all records
    if query:
        query = ast.literal_eval(query)
        cur = col.find(**query).limit(1)[0]
    else:
        query={}
        cur = col.find().limit(1)[0]
    if not key:
        return json.dumps("Key is a list of key(s) you want to Group By:  " + str(cur.keys()))
    if variable:
        if not variable in cur.keys():
            return json.dumps("Variable is a string of the key you want to aggregate: " + str(cur.keys()))
    else:
        return json.dumps("Variable is a string of the key you want to aggregate: " + str(cur.keys()))
    reduce = Code(" function(obj,prev) {prev.Sum += obj.%s;prev.count+=1; prev.Avg = prev.Sum/prev.count;}" % (variable))
    results = col.group(ast.literal_eval(key),query,{'Sum':0,'Avg':0,'count':0,'Variable':variable},reduce)
    #dump_out = list(results)
    for item in results:
        dump_out.append(item)
   #serialize and return JSON or JSONP
    serialized = json.dumps(dump_out, default = handler, sort_keys=True, indent=4)
    if callback is not None:
        return str(callback) + '(' + serialized + ')'
    else:
        return serialized

def find_loc( db=None, col=None, x='lon', y='lat', idcol='_id', 
                properties=False, query=None, callback=None):
    """
    For a specific lat/lon column pair return GeoJSON representation of the 
        coordinates.

        :param db: Optional, mongodb database, if not specified a list of dbs is returned
        :param col: Optional, mongodb collection
        :praam x: x-coordinate (longitude)
        :param y: y-coordinate (lattitude)
        :param query: Optional, query provided as a python dictionary (see pymongo and mongodb docs for query syntax)
        :param callback: Optional, used for returning output as JSONP (not implemented yet)
        :param showids: Optional, return mongodb _id's
        :param date: Optional, helper for simpler syntax in date range queries (broken)

    
    Example:
    >>> get.find_loc('flora', 'data', x='midlon', y='midlat', idcol='REF_NO', properties= True)
    """
    # Make connection
    con = Connection()
    
    # Browse or return databases
    if db in con.database_names():
            db = con[db] 
    else:        
        serialized = json.dumps(con.database_names())
    
    # Browse or return collections
    if col in db.collection_names():
        col = db[col]
    else:
        serialzed = json.dumps(db.collection_names())
    
    # Two types of output, with and without properties
    if properties: # Return GeoJSON with all properties
        cur = col.find()
        serialized = geojson.dumps(geojson.FeatureCollection([ 
                        geojson.Feature(
                            geometry=geojson.Point((item[x], item[y])),
                            properties={'id': item[idcol], 'attributes': item }
                        )
                for item in cur if x in item.keys() and y in item.keys() ]
                    ), indent=2, default=handler)
    else: # Return GeoJSON with only lat/lon and id column.
        cur = col.find(fields=[x,y,idcol])
        serialized = geojson.dumps(geojson.FeatureCollection([ 
                        geojson.Feature(
                            geometry=geojson.Point((item[x], item[y])),
                            properties={'id': item[idcol] } 
                        )
                for item in cur if x in item.keys() and y in item.keys() ], 
                ), indent=2, default=handler)
    
    if callback:
        return str(callback) + '(' + serialized + ')'
    else:
        return serialized
