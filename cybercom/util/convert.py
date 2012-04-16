'''
Tools to convert mongodb requests to other formats using Content-Type 
    manipulation
'''
import json, geojson, pandas, tempfile, sys, shutil, logging
from StringIO import StringIO
from subprocess import call


def geoJSON_processor(entity):
    """ Convert list of json Features to Feature Collection GeoJSON """
    try:
        jsonout = geojson.dumps(geojson.FeatureCollection(json.loads(entity)))
    except:
        logging.error('Had problem convertiong json to python, is it well formed?')
        logging.error(sys.exc_info)
    return jsonout
        
def geoJSONAttributes_processor(entity):
    """ Takes the properties/attributes of a GeoJSON document and returns a CSV representation of the data"""
    try:
        jsonout = json.loads(entity)
    except ValueError:
        logging.error('Had a problem converting json to python, is it well formed?')
        logging.error(sys.exec_info())
    try:
        df = pandas.DataFrame([ item['properties'] for item in jsonout ])
    except PandasError, e:
        logging.error('%s' % e )
        logging.error(sys.exc_info())
    try:
        outfile = StringIO()
        df.to_csv(outfile)
        outfile.seek(0)
        outdata = outfile.read()
        return outdata
    except:
        logging.error('Had trouble converting to CSV')
        logging.error(sys.exc_info())
        return None

def shapefile_processor(entity):
    """ Convert list of json Features to a zip archive containing a shapefile """
    try:
        tempdir = tempfile.mkdtemp()
        inloc = os.path.join(tempdir, 'temporary.json')
        infile = open(inloc, 'w')
        jsonout = json.loads(entity)
        infile.write(geojson.dumps(geojson.FeatureCollection(jsonout)))
        infile.close()
    except:
        logging.error('Had a problem processing JSON...is it a list of valid GeoJSON Features?')
        logging.error(sys.exc_info())
    try:
        command = ['ogr2ogr', '-f', 'ESRI Shapefile', output, inloc ]
        call(command)
    except:
        logging.error('''Couldn't run ogr2ogr''')
        logging.error(sys.exc_info())
    try:
        files = [ os.path.join(tempdir, item) for item in ['output.shp','output.prj','output.dbf','output.shx'] ]
        outfile = StringIO()
        zipfile = ZipFile(outfile, 'w')
        logging.info('Zipping...')
        for filename in files:
            zipfile.writestr(os.path.basename(filename), open(filename, 'r').read() )
        zipfile.close()
        outfile.seek()
    except:
        logging.error('''Problem zipping output''')
        logging.error(sys.exec_info())
    logging.info('Cleaning up...')
    shutil.rmtree(tempdir)
    return outfile

def csvfile_processor(entity):
    """ Use pandas to convert a list of json documents to a CSV file """
    try:
        jsonout = json.loads(entity)
        df = pandas.DataFrame(jsonout)
        outfile = StringIO()
        df.to_csv(outfile, index=False)
        outfile.seek(0)
        outdata = outfile.read()
        return outdata
    except:
        logging.error(sys.exc_info())
        return "We have a problem, is the input a list of JSON objects?"
            
