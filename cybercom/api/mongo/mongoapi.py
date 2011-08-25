import cherrypy
import json 
from cybercom.data.mongo.get import find, find_loc

def mimetype(type):
    def decorate(func):
        def wrapper(*args, **kwargs):
            cherrypy.response.headers['Content-Type'] = type
            return func(*args, **kwargs)
        return wrapper
    return decorate

class Root(object):
    @cherrypy.expose
    def index(self):
        return None
    @cherrypy.expose
    @mimetype('application/json')
    @cherrypy.tools.gzip()
    def db_find(self, db=None, col=None, query=None, callback=None, showids=None, date=None, **kwargs):
        """ 
        Wrapper for underlying pymongo access
        """
        return find(db, col, query, callback, showids, date)
    @cherrypy.expose
    @mimetype('application/json')
    def find_loc(self, db=None, col=None, x='lon', y='lat', idcol='_id', properties=False, query=None, callback=None, **kwargs):
        return find_loc( db, col, x, y, idcol, properties, query, callback)

cherrypy.tree.mount(Root())
application = cherrypy.tree

if __name__ == '__main__':
    cherrypy.engine.start()
    cherrypy.engine.block()

