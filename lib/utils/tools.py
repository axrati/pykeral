import datetime
import json
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


def list_cleaner(dlist):
    """
Remove None's and nan's from lists
    """
    new_list = []
    for i in dlist:
        print(i)
    return None



def q_dtype(key,obj):

    # Deserialize numpy types if available to do so
    try:
        val = obj[key].item()
    except:
        val = obj[key]

    # Remove spaces in keys, forces conformity to cypher
    cypher_key = key.replace(" ", "")
    
    #Always pads 2
    if type(val)==str:
        return "{}:'{}', ".format(cypher_key,obj[key])
    elif type(val)==datetime.datetime:
        stripped = str(obj[key]).replace(" ","T")
        return '{}:datetime("{}"), '.format(cypher_key,stripped)
    elif type(val)==datetime.date:
        return '{}:date("{}"), '.format(cypher_key,str(obj[key]))
    elif type(val)==list:
        return '{}:{}, '.format(cypher_key,str(obj[key]))
    elif type(val)==int or type(val)==float:
        return '{}:{}, '.format(cypher_key,obj[key])
    elif type(val)==dict:
        stripped = str(json.dumps(val, cls=np_encoder))
        # stripped = str(json.dumps(obj))
        stripped = stripped.replace("'","\\'")
        start = "{}:".format(cypher_key)
        guts = "'"+stripped+"', "
        return start+guts
    elif type(val)==bool:
        return '{}:{}, '.format(cypher_key,obj[key])
    elif obj[key] is None:
        # Need to understand how to best handle this. Nulls techincally shouldnt exist but its just an attr here
        return ""
        # return "{}:'{}', ".format(cypher_key,"NONE")
    else:
        print(key, obj)
        print(type(key), type(obj))
        print(type(obj[key]))
        raise Exception("Cypher query compiler recieved a key/value pair of node data that wasn't a str, number, list or dict")

