#from cybercom.data.catalog import datalayer
import pymongo, ast, shlex,os,sys
import datalayer

class Metadata_load():
    def __init__(self):
        self.metadata = datalayer.Metadata()
        self.session= self.metadata.getSession()
    def getSession(self):
        return self.session
    def catalog(self,param):
        '''
        param - dictionary having the following attributes

        * commons_id - reference to dt_data_commons table commons_id,
        cat_id - serial data type autogenerated return in resultproxy,
        * cat_name - name of cataloged item,
        data_provider -  name of who is providing data,
        * cat_type - type of cataloged item. reference to dt_type table ,
        * loc_id - reference to dt_location,
        source_id - integer datatype can be used for misc grouping of data,
        cat_desc - Description of cataloged data,
        * cat_method - reference rt_method table. catalog method,
        observed_date - cataloged item data observation date(General)
        remark - General remarks 
        observed_year - cataloged item data observered year
        custom_field_1 - custom data for cataloged item
        custom_field_2 - custom data for cataloged item
        status_flag - A is default for Active, I Inactive
        status_data - 'Data' - metadata for data 'Application' - metadata for applications
        userid - user who cataloged data
        timestamp_created - default to Now() - When item cataloged
        
        * required fields
        returns dictionary of primary keys inserted
        '''
        try:
            pkcol=self.metadata.getprimarykeys('dt_catalog',as_method='dict')['dt_catalog']
            result = self.metadata.Inserts('dt_catalog',[param,])
            val=result.inserted_primary_key
            return dict(zip(pkcol,val))
        except Exception as inst:
            raise inst
    
    def event_result(self,eventDict,resultListDict):
        pkcol=self.metadata.getprimarykeys('dt_event',as_method='dict')['dt_event']
        result = self.metadata.Inserts('dt_event',[eventDict,])
        val=result.inserted_primary_key
        eventpk = dict(zip(pkcol,val))
        for rstdict in resultListDict:
            rstdict['event_id']=eventpk['event_id']
            rstdict['commons_id']=eventpk['commons_id']
            result = self.metadata.Inserts('dt_result',[rstdict,])
    def repo_insertRow(self,tablename,param):
        try:
            pkcol=self.metadata.getprimarykeys(tablename,as_method='dict')[tablename]
            result = self.metadata.Inserts(tablename,[param,])
            val=result.inserted_primary_key
            return dict(zip(pkcol,val))
        except Exception as inst:
            raise inst
    def event_local_file(self,commons_id,cat_id, filepath,server,userid,eventUpdateDict={'event_name':'Local File Storage','event_desc':'Local File Storage'}):
        eparam = {'commons_id':commons_id,'cat_id':cat_id,'event_type':'Metadata','event_method':'Local_File'}
        eparam['userid'] = userid #os.getlogin()
        eparam.update(eventUpdateDict)
        rparam = {'commons_id':commons_id,'var_id':'Server','result_text':server,'result_order':1,'status_flag':'A','validated':'Y'}
        rparam1 = {'commons_id':commons_id,'var_id':'File_Path','result_text':filepath,'result_order':2,'status_flag':'A','validated':'Y'}
        self.event_result(eparam,[rparam,rparam1])
    def event_MongoDB_Access(self,commons_id,cat_id, database,collection,Query,server,userid,eventUpdateDict={'event_name':'MongoDB','event_desc':'MongoDB Access'}):
        eparam = {'commons_id':commons_id,'cat_id':cat_id,'event_type':'Metadata','event_method':'MongoDB_Access'}
        eparam['userid'] = userid #os.getlogin()
        eparam.update(eventUpdateDict)
        rparam = {'commons_id':commons_id,'var_id':'Server','result_text':server,'result_order':1,'status_flag':'A','validated':'Y'}
        rparam1 = {'commons_id':commons_id,'var_id':'Database','result_text':database,'result_order':2,'status_flag':'A','validated':'Y'}
        rparam2 = {'commons_id':commons_id,'var_id':'Collection','result_text':collection,'result_order':3,'status_flag':'A','validated':'Y'}
        rparam3 = {'commons_id':commons_id,'var_id':'Query','result_text':Query,'result_order':4,'status_flag':'A','validated':'Y'}
        self.event_result(eparam,[rparam,rparam1,rparam2,rparam3])
    def event_WebService(self,commons_id,cat_id,URL,event_method,userid,eventUpdateDict={'event_name':'Web Service','event_desc':'Web Service'}):
        eparam = {'commons_id':commons_id,'cat_id':cat_id,'event_type':'Metadata','event_method':event_method}
        eparam['userid'] = userid # os.getlogin()
        eparam.update(eventUpdateDict)
        rparam = {'commons_id':commons_id,'var_id':'URL','result_text':URL,'result_order':1,'status_flag':'A','validated':'Y'}
        self.event_result(eparam,[rparam,])
class Mongo_load():
    def __init__(self,database,host='localhost',port=27017):
        self.conn= pymongo.Connection(host,port)
        self.db= self.conn[database]
    def insert(self,collection,param):
        ''' collection is the mongo database collection
            param is a list of dictionaries with parameters to insert
        '''
        try:
            self.db[collection].insert(param)
            return True

        except Exception as inst:
            print type(inst)
            print inst
            return False
    def file2mongo(self,filename,collection,file_type='fixed_width',addDict=None,specificOperation=None,seperator=',',skiplines=0,skiplinesAfterHeader=0,groupInsert=1000):
        if file_type == 'fixed_width':
            self.file_fixed_width(filename,collection,addDict,specificOperation,skiplines,skiplinesAfterHeader,groupInsert)
        elif file_type == 'csv':
            self.file_csv(filename,collection,addDict,specificOperation,seperator,skiplines,skiplinesAfterHeader,groupInsert)
        else:
            raise NameError('file_type: ' + file_type + ' is not supported.(fixed_width or csv filetypes supported') 
    def file_csv(self,filename,collection,addDict=None,specificOperation=None,seperator=',', skiplines=0,skiplinesAfterHeader=0,groupInsert=1000):
        '''Takes csv file and inserts into Mongodb.
           filename: full path with file
           addDict: Optional Dictionary with key elements you want to add to each row
           calTimeFunc: Function passed to figure out time index. Method returns Dictionary to method
           skipline: Number of lines to skip prior to header line
        '''
        insertList=[]
        f2=open(filename,'r')
        for skip in range(skiplines):
            f2.readline()
        header = f2.readline()
        header = "".join(header.split()).split(seperator)
        for skip in range(skiplinesAfterHeader):
            f2.readline()
        count= 1
        for line in f2:
            arow=line#.split(seperator)
            arow="".join(arow.split()).split(seperator)
            if len(header)+1 ==len(arow):
                header.append('error_handle')
            if len(header)==len(arow):
                row=[]
                for data in arow:
                    try:
                        row.append(ast.literal_eval(data))
                    except:
                        row.append(data)
                temp = dict(zip(header,row))
                if not addDict == None:
                    temp.update(addDict)
                if not specificOperation == None:
                    specificOperation(temp)
                insertList.append(temp)
                if count % groupInsert == 0:
                    self.insert(collection,insertList)
                    insertList=[]
                count= count +1
        return self.insert(collection,insertList)
    def file_fixed_width(self,filename,collection,addDict=None,specificOperation=None, skiplines=0,skiplinesAfterHeader=0,groupInsert=1000):
        '''Takes fixed width file and inserts into Mongodb.
           filename: full path with file
           addDict: Optional Dictionary with key elements you want to add to each row
           calTimeFunc: Function passed to figure out time index. Method returns Dictionary to method
           skipline: Number of lines to skip prior to header line
        '''
        insertList=[]
        f2=open(filename,'r')
        for skip in range(skiplines):
            f2.readline()
        header = shlex.split(f2.readline())
        for skip in range(skiplinesAfterHeader):
            f2.readline()
        count=1
        for line in f2:
            arow=shlex.split(line)
            if len(header)==len(arow):
                row=[]
                for data in arow:
                    try:
                        row.append(ast.literal_eval(data))
                    except:
                        row.append(data)
                temp = dict(zip(header,row))
                if not addDict == None:
                    temp.update(addDict)
                if not specificOperation == None:
                    specificOperation(temp)
                insertList.append(temp)
                if count % groupInsert == 0:
                    self.insert(collection,insertList)
                    insertList=[]
                count= count +1
        return self.insert(collection,insertList)
