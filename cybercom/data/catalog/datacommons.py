import urllib, urllib2, cookielib,json
from json_handler import handler
from BeautifulSoup import BeautifulSoup

#Data Catalog base URL
base_url = 'http://production.cybercommons.org/catalog/'
loginurl='http://production.cybercommons.org/accounts/login/'

class toolkit():
    '''Toolkit class for Data Commons CRUD operations'''
    def __init__(self,username,password,login_url=loginurl):
        cj = cookielib.CookieJar()
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
        urllib2.install_opener(opener)
        url = urllib2.urlopen(login_url)
        html = url.read()
        doc = BeautifulSoup(html)
        csrf_input = doc.find(attrs = dict(name = 'csrfmiddlewaretoken'))
        csrf_token = csrf_input['value']
        params = urllib.urlencode(dict(username = username, password=password,
                 csrfmiddlewaretoken = csrf_token))
        url = urllib2.urlopen(login_url, params)

    def get_data(self, datacommons=None, query=None, showids=None, col='data', url=base_url + 'db_find', **kwargs):
        param={'db':datacommons,'col':col,'query':query,'showids':showids}
        return self.execWS(url,param) 

    def get_urllib2(self):
        '''Returns urllib2 with cookies from cybercommons login'''
        return urllib2
    def save(self,commons_name, document,date_keys=[],url=base_url + 'save',collection='data'):
        param = {"database":commons_name,"data":json.dumps(document,default = handler),"date_keys":json.dumps(date_keys,default = handler)}
        #param = {'database':commons_name,'data':document,'date_keys':date_keys}
        param['collection']=collection
        result= self.execWS(url,param,True)
        #return  result
        try:
            if result['status']:
                return result['_id']
            else:
                raise result['description']
        except:
            raise 
    def createCommons(self,commons_name,url=base_url + 'newCommons'):
        param={'commons_name':commons_name}
        return self.execWS(url,param)
    def dropCommons(self,commons_name,url=base_url + 'dropCommons'):
        param={'commons_name':commons_name}
        return self.execWS(url,param)
    def setPublic(self, commons_name,auth='r',url=base_url + 'setPublic'):
        #auth n, r , rw for  None,read, and read/write
        param={'commons_name':commons_name,'auth':auth}
        return self.execWS(url,param)
    def execWS(self, url, data,urlencode=True):
        try:
            if urlencode:
                param = urllib.urlencode(data)
                req = urllib2.Request(url, param)
                response = urllib2.urlopen(req)
                result= json.loads(response.read())
                return result
            else:
                headers = {'Content-Type':'application/json'}
                req = urllib2.Request(url, json.dumps(data,default = handler), headers)
                req = urllib2.Request(url,data, headers)
                response = urllib2.urlopen(req)
                result= json.loads(response.read())
                return result
        except:
            raise 
