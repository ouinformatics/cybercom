from cybercom.data.catalog import datalayer
import json
import cherrypy

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
    def search(self,tablename=None, commons_id=None, columns=None):
        cat = datalayer.Metadata()
        if columns:
            columns = columns.split(',')
        if commons_id:
            return json.dumps(cat.Search(tablename, column=columns, 
                                where='commons_id = %s' % commons_id), indent=2)
        else:
            return json.dumps(cat.Search(tablename), column=columns, indent=2)
    def index(self):
        return "Yay"


cherrypy.tree.mount(Root())
application = cherrypy.tree

if __name__ == '__main__':
    cherrypy.engine.start()
    cherrypy.engine.block()


