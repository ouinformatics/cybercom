from cybercom.data.catalog import datalayer
from cybercom.util.geojson_tools import mkGeoJSONPoint
import json
import cherrypy
from ast import literal_eval

def mimetype(type):
    def decorate(func):
        def wrapper(*args, **kwargs):
            cherrypy.response.headers['Content-Type'] = type
            return func(*args, **kwargs)
        return wrapper
    return decorate


class Root():
    @cherrypy.expose
    @mimetype('application/json')
    def search(self,tablename=None, columns=None, pkey=None):
        cat = datalayer.Metadata()
        tables = cat.gettables(as_method='dict')
        # respond to urls /search/
        if tablename is None:
            return json.dumps(tables, indent=1)
        elif tablename not in tables['metadata tables']:
            return "Specified table does not exist.  Valid tables are: \n %s" % (json.dumps(tables['metadata tables'], indent=2))
        # respond to urls /search/tablename/
        if columns is None and tablename:
            return cat.getcolumns(tablename)

        columns = columns.split(',')
        ref = cat.getprimarykeys(tablename, as_method='dict')        
        if pkey is None and tablename:
            return json.dumps(ref)
        elif pkey == '*':
            return json.dumps(cat.Search(tablename, columns), indent=2)
        elif pkey.find(',') > 0:
            pkey = pkey.split(',')
            items = zip(ref[tablename],pkey)
            qstring = [ "%s = '%s'" % (k,v) for k,v in items if v != 'None']
            whereclause = ' and '.join(map(str,qstring))
            return json.dumps(cat.Search(tablename, columns, where = str(whereclause)), indent=2)
        else:
            return "Invalid query"
    @cherrypy.expose
    @mimetype('application/json')
    def location(self, pkey=None, point=True, attributes=True, transform=False):
        tablename='dt_location'
        cat = datalayer.Metadata()
        columns = ['commons_id','loc_id', 'lat', 'lon']
        ref = cat.getprimarykeys(tablename, as_method='dict')
        if pkey:
            pkey = pkey.split(',')
            items = zip(ref[tablename],pkey)
            qstring = [ "%s = '%s'" % (k,v) for k,v in items if v != 'None']
            whereclause = ' and '.join(map(str,qstring))
            return mkGeoJSONPoint(cat.Search(tablename, columns, where=whereclause), 
                    latkey='lat', lonkey='lon', attributes=True, transform=transform)
        else:
            return "Error, invalid query string"
        
cherrypy.tree.mount(Root())
application = cherrypy.tree

if __name__ == '__main__':
    cherrypy.engine.start()
    cherrypy.engine.block()


