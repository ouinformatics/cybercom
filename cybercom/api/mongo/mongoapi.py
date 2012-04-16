import cherrypy
import simplejson as json 
from cybercom.data.mongo.get import find,group, find_loc, distinct
from cybercom.util.convert import *

def mimetype(type):
    def decorate(func):
        def wrapper(*args, **kwargs):
            cherrypy.response.headers['Content-Type'] = type
            return func(*args, **kwargs)
        return wrapper
    return decorate

def convert_output(entity,outtype=None):
    if outtype == 'json' or outtype == None:
        cherrypy.response.headers['Content-Type'] = 'application/json'
        return entity
    if outtype == 'csv':
        cherrypy.response.headers['Content-Type'] = 'text/csv'
        return csvfile_processor(entity)
    if outtype == 'shp':
        cherrypy.response.headers['Content-Type'] = 'application/zip'
        return shapefile_processor(entity)
    if outtype == 'geojson':
        cherrypy.response.headers['Content-Type'] = 'application/json'
        return geoJSON_processor(entity)
    if outtype == 'geojsonattrib':
        cherrypy.response.headers['Content-Type'] = 'text/csv'
        return geoJSONAttributes_processor(entity)

class Root(object):
    _cp_config = {'tools.gzip.on': True, 'tools.gzip.mime_types': ['text/*', 'application/json']}
    @cherrypy.expose
    def index(self):
        return None
    @cherrypy.expose
    @mimetype('application/json')
    @cherrypy.tools.gzip()
    def db_find(self, db=None, col=None, query=None, callback=None, showids=None, date=None, outtype=None, **kwargs):
        """ 
        Wrapper for underlying pymongo access
        """
        return convert_output(find(db, col, query, callback, showids, date),outtype)
    @cherrypy.expose
    @mimetype('application/json')
    @cherrypy.tools.gzip()
    def distinct(self, db=None, col=None, distinct_key=None, query=None, callback=None, **kwargs):
        return distinct(db,col,distinct_key,query,callback)
    @cherrypy.expose
    @mimetype('application/json')
    @cherrypy.tools.gzip()
    def group_by(self, db=None, col=None,key=None,variable=None, query=None,callback=None, **kwargs):
        """ 
        Wrapper for underlying pymongo access
        """
        return group(db, col, key,variable,query,callback)
    @cherrypy.expose
    @mimetype('application/json')
    def find_loc(self, db=None, col=None, x='lon', y='lat', idcol='_id', properties=False, query=None, callback=None, **kwargs):
        return find_loc( db, col, x, y, idcol, properties, query, callback)

cherrypy.tree.mount(Root())
application = cherrypy.tree

if __name__ == '__main__':
    cherrypy.engine.start()
    cherrypy.engine.block()

