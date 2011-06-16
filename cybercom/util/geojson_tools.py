import geojson
import pyproj

# Set Default Projection
TARGET_PROJECTION="+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +a=6378137 +b=6378137 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
SOURCE_PROJECTION="+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"

def mkGeoJSONPoint(obj,latkey,lonkey,attributes=False, transform=False, t_srs=TARGET_PROJECTION, s_srs=SOURCE_PROJECTION):
    FC = []
    for item in obj:
        x = item.pop(lonkey)
        y = item.pop(latkey)
        if transform:
            p1 = pyproj.Proj(s_srs)
            p2 = pyproj.Proj(t_srs)
            x, y = pyproj.transform( p1, p2, x, y)
        if attributes:
            FC.append( geojson.Feature( geometry=geojson.Point((x,y)), properties = item ) )
        else:
            FC.append( geojson.Feature( geometry=geojson.Point((x,y)) ) )
    return geojson.dumps(geojson.FeatureCollection( FC ), indent=2)


