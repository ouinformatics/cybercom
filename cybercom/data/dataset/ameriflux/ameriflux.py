"""
Functions for accessing ameriflux tower data stored in the cybercommons.

Examples:

Import module:
>>> from cybercom.data.dataset import ameriflux
>>> from datetime import datetime

"""

AMF_DB='amf_level4'

from pymongo import Connection
try:
    import numpy as np
except:
    print "numpy is not installed, getvar(as_method='numpy') will not work"
import datetime
from cybercom.data.catalog import datalayer

aggregation = 'weekly' # Set default aggregation to weekly
dbhost = 'fire.rccc.ou.edu'

con = Connection(dbhost)
db = con[AMF_DB]

def aggregations(collection=AMF_DB):
    """Show the levels of aggregation [hourly,dayly,weekly,monthly] available 
        in the dataset

    Show aggregations available:
    >>> ameriflux.aggregations()

    """
    return [ item for item in con[collection].collection_names() 
                if item != 'system.indexes'] 

def locations(aggregation=aggregation):
    """List locations available for a given aggregation level
    
    Show locations available at a given aggregation:
    >>> ameriflux.locations('monthly')
    """
    if aggregation in aggregations():
       return db[aggregation].distinct('location')
    else:
        print "Uknown aggregation, please select from " + str(aggregations())

def dates(location,aggregation=aggregation, limits=False):
    """Show date range of given location/aggregation pair

    Show dates available for given aggregation and location:
    >>> ameriflux.dates('US-FPE', 'monthly')
    """
    dates = db[aggregation].distinct('StartDate')
    if limits:
        return { 'start': min(dates), 'end': max(dates) }
    else:
        return dates

def variables(aggregation=aggregation):
    """List variables available for a particular aggregation product
    
    Show variables available for a given aggregation:
    >>> ameriflux.variables('weekly')

    """
    return db[aggregation].find_one().keys()

def locationinfo(location):
    cat = datalayer.Metadata()
    return cat.Search('dt_location', ['lat','lon','loc_id'], 
            where="loc_id = '%s'" %(location))

def getvar(location='US-FPE', 
           variable='NEE_or_fANN', 
           aggregation=aggregation, 
           date_from=datetime.datetime(1990,1,1), 
           date_to=datetime.datetime.now(),
           as_method='dict'):
    """Get an ameriflux observation timeseries from a given 
        aggregation.
    
    Get weekly data from US-FPE tower:
    >>> ameriflux.getvar('US-FPE', 'NEE_or_fANN', aggregation='weekly',
                        date_from = datetime(1990,1,1),
                        date_to = datetime(2011,2,28) )
    """
    if aggregation in aggregations() and variable in variables():
        if aggregation == 'monthly':
            month = db['monthly']
            cur = month.find({'location': location, 
                                'StartDate': { '$gte': date_from }, 
                                'EndDate': { '$lte': date_to } }, 
                                [variable, 'Month', 'StartDate'])
            if as_method == 'numpy':
                return np.array([ ( item['StartDate'], 
                                    item['Month'], item[variable]) 
                                for item in cur ], 
                                dtype = {'names': ['date', 
                                                    'month', 
                                                    variable], 
                                'formats': ['object','i4','f4']} ).view(np.recarray)
            if as_method == 'dict':
                return [ item for item in cur ]
        elif aggregation == 'weekly':
            week = db['weekly']
            cur = week.find({'location': location, 
                             'StartDate': { '$gte': date_from }, 
                             'EndDate': { '$lte': date_to} }, 
                             [variable, 'Period', 'StartDate'])
            if as_method == 'numpy':
                return np.array( [ (item['StartDate'], item['Period'], 
                                item[variable]) for item in cur ], 
                                dtype = {'names':['date','week',variable], 
                                'formats': ['object','i4', 'f4'] }
                                ).view(np.recarray)
            if as_method == 'dict':
                return [item for item in cur]
        elif aggregation == 'hourly':
            pass # not implemented
        elif aggregation == 'daily':
            pass # not implemented
    else:
        out_msg = """Unknown aggregation or variable, know aggregations are %s
           Variables are %s"""
        print  out_msg % (aggregations(), variables())


