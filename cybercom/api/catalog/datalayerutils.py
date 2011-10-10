from cybercom.data.catalog import datalayer
from datetime import datetime

def event_results_by_time(commons_id, cat_id, start_date, end_date=None, var_id=None):
    ''' 
    Based on a commons_id and cat_id, and start/end time return object
        containing all event <-> results for that time range.

    Example:
        # Standard time range
        event_results_by_time(807, 1448132, datetime(2011,5,1), datetime(2011,5,10))

        # Without a start date, the end date becomes datetime.now()
        event_results_by_time(807, 1448132, datetime(2011,08,1))

        # Just return a specific var_id
        event_results_by_time(807, 1448132, datetime(2011,08,1), var_id='URL')

    '''
    md = datalayer.Metadata()
    start = start_date.strftime('%Y-%m-%d')
    if end_date:
        end =  end_date.strftime('%Y-%m-%d')
    else:
        end = datetime.now().strftime('%Y-%m-%d')
    if var_id:
        whr = "var_id = '%s' and event_id in(select event_id from dt_event where commons_id=%s and cat_id=%s and event_date >= '%s' and event_date <= '%s')" % (var_id, commons_id, cat_id, start , end)
        return [ item.values()[0] for item in md.Search('dt_result', where = whr,column=['result_text']) ]
    else:
        whr = "event_id in(select event_id from dt_event where commons_id=%s and cat_id=%s and event_date >= '%s' and event_date <= '%s')" % (commons_id, cat_id, start , end)
        return md.Search('dt_result', where = whr,column=['var_id','result_text'])

def event_results_by_metadata():
    pass




