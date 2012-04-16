from .. import convert 
import urllib
import json_strings

def test_geoJSON_processor():
    assert json_strings.feature_collection == convert.geoJSON_processor(json_strings.polygons)

def test_geoJSONAttributes_processor():
    assert json_strings.attributes == convert.geoJSONAttributes_processor(json_strings.polygons)

def test_csvfile_processor():
    assert json_strings.csv_out == convert.csvfile_processor(json_strings.mesonet)
