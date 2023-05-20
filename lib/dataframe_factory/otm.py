from lib.utils.tools import all_exist_in

def otm_trav(level,place,unq_cols):
    keys = list(level.keys())
    if all_exist_in(['sub_columns'], keys):
        cols = []
        for j in level['sub_columns']:
            cols.append(j['column_name'])
            unq_cols.append(j['column_name'])
        place.append({"first":level['column_name'], 'then':cols})
        for k in level['sub_columns']:
            otm_trav(k, place, unq_cols)
        

def otm_levels(level,place):
    keys = list(level.keys())
    if all_exist_in(['sub_columns'], keys):
        for j in level['sub_columns']:
            place.append(level['column_name'])
        for k in level['sub_columns']:
            otm_levels(k, place)

def otm_query(node):
    where = ''
    for key in list(node.keys()):
        if node[key] is None:
            where += "{}.isnull() & ".format(key)
        elif type(node[key])==str:
            where += "{}=='{}' & ".format(key,node[key])
        else:
            where += "{}=={} & ".format(key,node[key])
    where_q = where[0:len(where)-2]
    return where_q


