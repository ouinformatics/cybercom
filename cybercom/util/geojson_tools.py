import geojson

def mkGeoJSONPoint(obj,latkey,lonkey,attributes=False):
    FC = []
    for item in obj:
        x = item.pop(lonkey)
        y = item.pop(latkey)
        if attributes:
            FC.append( geojson.Feature( geojson.Point((x,y)), properties = item ) )
        else:
            FC.append( geojson.Feature( geojson.Point((x,y)), properties = item ) )
    return geojson.dumps(geojson.FeatureCollection( FC ), indent=2)


