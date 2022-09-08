import datetime
import json
import numpy as np

def np_encoder(object):
    if isinstance(object, np.generic):
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
    #Always pads 2
    if type(obj[key])==str:
        return "{}:'{}', ".format(key,obj[key])
    elif type(obj[key])==datetime.datetime:
        stripped = str(obj[key]).replace(" ","T")
        return '{}:datetime("{}"), '.format(key,stripped)
    elif type(obj[key])==datetime.date:
        return '{}:date("{}"), '.format(key,str(obj[key]))
    elif type(obj[key])==list:
        return '{}:{}, '.format(key,str(obj[key]))
    elif type(obj[key])==int or type(obj[key])==float:
        return '{}:{}, '.format(key,obj[key])
    elif type(obj[key])==dict:
        stripped = str(json.dumps(obj[key], default=np_encoder))
        start = "{}:".format(key)
        guts = "'"+stripped+"', "
        return start+guts
    else:
        raise Exception("Cypher query compiler recieved a key/value pair of node data that wasn't a str, number, list or dict")
