from pymongo import Connection
import numpy as np
import datetime

aggregation = 'weekly'

con = Connection()
db = con['amf_level4']

def aggregations(collection='amf_level4'):
    """Show the levels of aggregation [hourly,dayly,weekly,monthly] available in the dataste
    """
    return [ item for item in con[collection].collection_names() if item != 'system.indexes'] 

def locations(aggregation=aggregation):
    """List locations available for a given aggregation level
    """
    if aggregation in aggregations():
       return db[aggregation].distinct('location')
    else:
        print "Uknown aggregation, please select from " + str(aggregations())

def dates(location,aggregation=aggregation, limits=False):
    """Show date range of given location/aggregation pair
    """
    dates = db[aggregation].distinct('StartDate')
    if limits:
        return { 'start': min(dates), 'end': max(dates) }
    else:
        return dates

def variables(aggregation=aggregation):
    """List variables available for a particular aggregation product
    """
    return db[aggregation].find_one().keys()
    

def getvar(location='US-FPE', variable='NEE_or_fANN', aggregation=aggregation, date_from=datetime.datetime(1990,1,1), date_to=datetime.datetime.now()):
    if aggregation in aggregations() and variable in variables():
        if aggregation == 'monthly':
            month = db['monthly']
            cur = month.find({'location': location, 'StartDate': { '$gte': date_from }, 'EndDate': { '$lte': date_to } }, [variable, 'Month', 'StartDate'])
            return np.array([ ( datetime.datetime.strftime(item['StartDate'], '%Y%j'), item['Month'], item[variable]) for item in cur ], dtype = {'names': ['StartDate', 'month', variable], 'formats': ['i4','i4','f4']} )
        elif aggregation == 'weekly':
            week = db['weekly']
            cur = week.find({'location': location, 'StartDate': { '$gte': date_from }, 'EndDate': { '$lte': date_to} }, [variable, 'Period', 'observed_year'])
            return np.array( [ (item['observed_year'], item['Period'], item[variable]) for item in cur ], dtype = {'names':['year','week',variable], 'formats': ['i4','i4', 'f4'] })
        elif aggregation == 'hourly':
            pass # not implemented
        elif aggregation == 'daily':
            pass # not implemented
    else:
        print "Unknown aggregation or variable, know aggregations are " + str(aggregations()) + ' variables are: ' + str(variables())


