"""
Functions for accessing ameriflux tower data stored in the cybercommons.

Examples:

Import module:
>>> from cybercom.data.dataset import ameriflux
>>> from datetime import datetime

"""
import datetime
import numpy as np

AMF_DB='amf_level4'
aggregation='weekly'

def connect():
    from pymongo import Connection

    aggregation = 'weekly' # Set default aggregation to weekly
    dbhost = 'fire.rccc.ou.edu'
    con = Connection(dbhost)
    return con[AMF_DB]

def aggregations(collection=AMF_DB):
    """Show the levels of aggregation [hourly,dayly,weekly,monthly] available 
        in the dataset

    Show aggregations available:
    >>> ameriflux.aggregations()

    """
    db = connect()
    return [ item for item in db.collection_names() 
                if item != 'system.indexes'] 

def locations(aggregation=aggregation):
    """List locations available for a given aggregation level
    
    Show locations available at a given aggregation:
    >>> ameriflux.locations('monthly')
    """
    db = connect()
    if aggregation in aggregations():
       return db[aggregation].distinct('location')
    else:
        print "Uknown aggregation, please select from " + str(aggregations())

def dates(location,aggregation=aggregation, limits=False):
    """Show date range of given location/aggregation pair

    Show dates available for given aggregation and location:
    >>> ameriflux.dates('US-FPE', 'monthly')
    """
    db = connect()
    dates = db[aggregation].distinct('StartDate')
    if limits:
        return { 'start': min(dates), 'end': max(dates) }
    else:
        return dates

def units(aggregation=aggregation):
    from cybercom.data.catalog import datalayer
    md=datalayer.Metadata()
    units = md.Search('rt_method_parameters',where="method_code='Ameriflux_%s' ORDER BY param_id" % (aggregation) ,as_method='dict')


def variables(aggregation=aggregation, metadata=None):
    """List variables available for a particular aggregation product
    
    Show variables available for a given aggregation:
    >>> ameriflux.variables('weekly')

    """
    from cybercom.data.catalog import datalayer
    md=datalayer.Metadata()
    output={}
    variables = md.Search('rt_method_parameters',where="method_code='Ameriflux_%s' ORDER BY param_id" % (aggregation.capitalize()) ,as_method='dict')
    for item in variables:
        output.update({item['param_name']: item})
    if metadata:
        return output
    else:
        return output.keys()


def locationinfo(location):
    from cybercom.data.catalog import datalayer
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
                        date_from = datetime(1990,1,1,
                        date_to = datetime(2011,2,28) )
    """
    db = connect()
    if aggregation in aggregations() and variable in variables():
        if aggregation == 'monthly':
            month = db['monthly']
            cur = month.find({'location': location, 
                                'StartDate': { '$gte': date_from }, 
                                'EndDate': { '$lte': date_to },
                                variable : { '$gt': -9999 } }, 
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
                             'EndDate': { '$lte': date_to},
                             variable: {'$gt': -9999} }, 
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
            hour = db['hourly']
            cur = hour.find({'location': location,
                             'StartDate': { '$gte': date_from },
                             'EndDate': { '$lte': date_to}, 
                             variable: {'$gt': -9999} },
                             [variable, 'Hour', 'StartDate'])
            if as_method == 'numpy':
                return np.array( [ (item['StartDate'], item['Hour'], 
                                item[variable]) for item in cur ], 
                                dtype = {'names':['date','Hour',variable], 
                                'formats': ['object','i4', 'f4'] }
                                ).view(np.recarray)
            if as_method == 'dict':
                return [item for item in cur]
        elif aggregation == 'daily':
            day = db['daily']
            cur = day.find({'location': location,
                             'StartDate': { '$gte': date_from },
                             'EndDate': { '$lte': date_to},
                             variable: {'$gt': -9999} },
                             [variable, 'Day', 'StartDate'])
            if as_method == 'numpy':
                return np.array( [ (item['StartDate'], item['Day'],
                                item[variable]) for item in cur ],
                                dtype = {'names':['date','Day',variable],
                                'formats': ['object','i4', 'f4'] }
                                ).view(np.recarray)
            if as_method == 'dict':
                return [item for item in cur]
    else:
        out_msg = """Unknown aggregation or variable, know aggregations are %s
           Variables are %s"""
        print  out_msg % (aggregations(), variables())


