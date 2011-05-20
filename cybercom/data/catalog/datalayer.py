'''
SQLalchemy Metadata Access
Mark Stacy -- markstacy@ou.edu
version: 1.0
Created 4/25/2011
Cybercommons metadata data layer. (CRUD, List Tables,Columns,Primary Keys)
'''
import simplejson, json, datetime, StringIO, csv, sys
from sqlalchemy import Table
from sqlalchemy.sql import select,update
import db_create as dl
conn = dl.engine.connect()

class Metadata():
    def Search(self,tablename,column=None,where=None,as_method='dict'):
        #****setup up mapped table

        tab = Table(tablename,dl.Base.metadata,autoload=True)

        #****Add Specific columns to query    
        cols=[]
        if column==None:
            saObj = select([tab],where)
        else:
            for col in tab.c:
                if col.name in column:
                    cols.append(col)
            saObj = select(cols,where)

        res = saObj

        if as_method == 'dict':
            return self._as_dict( res )
        if as_method == 'csv':
            return self._as_csv( res )    
        if as_method == 'json':
            return self._as_json(res)
        if as_method == 'yaml':
            return self._as_yaml(res)
        else:
            return conn.execute(res) 

    def Inserts(self,tablename,parameters):
        '''
           Insert records to requsted table
           tablename - string
           parameters - list of dicts with rows to insert
        ''' 
        tab = Table(tablename,dl.Base.metadata,autoload=True)
        for dct in parameters:
            try:
                inst = tab.insert(dct)
                conn.execute(inst)#inst.execute(parameters)
            except Exception as err:
                print err
                #print type(err)
    def Updates(self,tablename,where,paramUpdate):
        '''
           Update records to requsted table
           tablename - string
           where - string with where sql statement
           paramUpdate - Dict with updated values
           example:
            Metadata.Updates('rt_state',"status_flag='A'",{'state_name':'North Dakota'})
            Results: rows with rt_state.status_flag= A --> state_name updated to North Dakota 
        '''
        tab = Table(tablename,dl.Base.metadata,autoload=True)
        u = tab.update(where,paramUpdate)
        conn.execute(u)
    def Delete(self,tablename,where):
        '''
           Delete records to requsted table
           tablename - string
           where - string with where sql statement
           example:
            Metadata.Delete('rt_state',"status_flag='A'")
            Results: rows with rt_state.status_flag= A --> Deletes records
        '''
        tab = Table(tablename,dl.Base.metadata,autoload=True)
        d = tab.delete(where)
        conn.execute(d)

    #**************functions***********

    def _get_cols(self,res):
        """ Get the header of column names """ 
        cols = []
        for col in res.c:
            cols.append(col.name)
        return cols

    def _zip_rows(self,res):
        """ Zip the header and rows into a python dictionary """
        output = []
        header = self._get_cols( res )
        res1=conn.execute(res)
        for row in res1:
            output.append(dict(zip(header, row)))
        return output
        
    def _as_dict(self,res):
        """ Return res as a python dictionary """
        return self._zip_rows( res ) 

    def _as_csv(self,res):
        """ Return res as a csv complete with headers """
        outfile = StringIO.StringIO()
        cw = csv.writer(outfile, quotechar = '"', quoting=csv.QUOTE_MINIMAL,skipinitialspace=True)
        cw.writerow( self._get_cols( res ) )
        cw.writerows( conn.execute(res).fetchall())
        return outfile.getvalue()

    def _as_json(self,res):
        """ Return res as json """
        try:
            import json
        except:
            print >> sys.stderr, "Don't have json"
        return json.dumps( self._zip_rows( res ), indent=2, default= self._handler ) 

    def _as_yaml(self,res):
        """ return res as yaml """
        try:
            import yaml
        except:
            print >> sys.stderr, "You don't seem to have PyYAML installed"
        return yaml.dump( self._zip_rows( res ))

    def _handler(self,obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        else:
            raise TypeError, 'Object of type %s with value of %s is not JSON serializable' % (type(Obj), repr(Obj))
    def getprimarykeys(self,tablename=None,as_method='json'):
        keys = []
        dict = {}
        for t in dl.Base.metadata.sorted_tables:
            if tablename==None:
                for key in t.primary_key:
                    keys.append(key.name)
                dict[t.name]=keys
                keys=[]
            else:
                if t.name == tablename:
                    for key in t.primary_key:
                        keys.append(key.name)
                    dict[t.name]=keys 
        #return simplejson.dumps(dict, sort_keys=True, indent=2)
        if as_method == 'dict':
            return dict
        if as_method == 'json':
            return simplejson.dumps(dict, sort_keys=True, indent=2) 
        else:
            return dict
    def listcolumns(self,tablename):
        keys = []
        for t in dl.Base.metadata.sorted_tables:
            if t.name == tablename:
                for key in t.c:
                    keys.append(key.name)
        return keys
    def getcolumns(self,tablename=None):
        keys = []
        dict = {}
        for t in dl.Base.metadata.sorted_tables:
            if tablename==None:
                for key in t.c:
                    keys.append(key.name)
                dict[t.name]=keys
                keys=[]
            else:
                if t.name == tablename:
                    for key in t.c:
                        keys.append(key.name)
                    dict[t.name]=keys
        return simplejson.dumps(dict, sort_keys=True, indent=2)
    def gettables(self):
        keys = []
        dict = {}
        for t in dl.Base.metadata.sorted_tables:
            #print t.name
            keys.append(t.name)
        dict['metadata tables']=sorted(set(keys))
        return simplejson.dumps(dict, indent=2)

