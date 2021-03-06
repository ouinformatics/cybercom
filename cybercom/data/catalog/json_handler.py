import calendar
import datetime
import re
try:
    import uuid
    _use_uuid = True
except ImportError:
    _use_uuid = False

from bson.dbref import DBRef
from bson.max_key import MaxKey
from bson.min_key import MinKey
from bson.objectid import ObjectId
from bson.timestamp import Timestamp
from bson.tz_util import utc
from geojson.mapping import Mapping
from geojson import Feature, FeatureCollection

_RE_TYPE = type(re.compile("foo"))

def handler(obj):
    if isinstance(obj, ObjectId):
        return str(obj) #{"$oid": str(obj)}
    if isinstance(obj, DBRef):
        return obj.as_doc()
    if isinstance(obj, datetime.datetime):
        # TODO share this code w/ bson.py?
        return obj.isoformat() + 'Z'
    if isinstance(obj, _RE_TYPE):
        flags = ""
        if obj.flags & re.IGNORECASE:
            flags += "i"
        if obj.flags & re.MULTILINE:
            flags += "m"
        return {"$regex": obj.pattern,
                "$options": flags}
    if isinstance(obj, MinKey):
        return {"$minKey": 1}
    if isinstance(obj, MaxKey):
        return {"$maxKey": 1}
    if isinstance(obj, Timestamp):
        return {"t": obj.time, "i": obj.inc}
    if _use_uuid and isinstance(obj, uuid.UUID):
        return {"$uuid": obj.hex}
    if isinstance(obj, Mapping):
        return dict(obj)
    if isinstance(obj, Feature):
        return dict(obj)
    if isinstance(obj, FeatureCollection):
        return dict(obj)
    raise TypeError("%r is not JSON serializable" % obj)
