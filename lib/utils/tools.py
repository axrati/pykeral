import datetime
import json
from xmlrpc.client import Boolean
import numpy as np

class np_encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(object, np.generic):
            return object.item()

def new_vs_update (arr1, arr2):
    creates = []
    updates = []
    for i in arr1:
        if i not in arr2:
            if i not in creates:
                creates.append(i)
        else:
            if i not in updates:
                updates.append(i)
    return creates,updates

def all_exist_in (arr1, arr2):
    check=0
    for i in arr1:
        if i not in arr2:
            check+=1
    return check==0





def q_dtype(key,obj):

    # Deserialize numpy types if available to do so
    try:
        val = obj[key].item()
    except:
        val = obj[key]

    #Always pads 2
    if type(val)==str:
        return "{}:'{}', ".format(key,obj[key])
    elif type(val)==datetime.datetime:
        stripped = str(obj[key]).replace(" ","T")
        return '{}:datetime("{}"), '.format(key,stripped)
    elif type(val)==datetime.date:
        return '{}:date("{}"), '.format(key,str(obj[key]))
    elif type(val)==list:
        return '{}:{}, '.format(key,str(obj[key]))
    elif type(val)==int or type(val)==float:
        return '{}:{}, '.format(key,obj[key])
    elif type(val)==dict:
        stripped = str(json.dumps(obj[key], cls=np_encoder))
        start = "{}:".format(key)
        guts = "'"+stripped+"', "
        return start+guts
    elif type(val)==bool:
        return '{}:{}, '.format(key,obj[key])
    else:
        print(key, obj)
        print(type(key), type(obj))
        print(type(obj[key]))
        raise Exception("Cypher query compiler recieved a key/value pair of node data that wasn't a str, number, list or dict")

