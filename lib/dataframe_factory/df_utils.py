import pandas as pd

def row_to_dict(dataframe,columns):
    decon_arr = []
    for index, row in dataframe.iterrows():
        data = {}
        for i in columns:
            if pd.isna(row[i]):
                data[str(i)]=None
            else:
                data[str(i)]=row[i]
        decon_arr.append(data)
    return decon_arr


def get_node(rel_name, nodes):
    check=0
    capture=[]
    for node in nodes:
        if node.self_name == rel_name:
            check+=1
            capture.append(node)
    if check == 0:
        raise Exception("You aligned a relationship to a node group name that doesnt exist... {}".format(rel_name))
    return capture


def rel_query_gen(node_o_keys, node_o_data, node_t_keys, node_t_data):
    where = ''
    for key in node_o_keys:
        if node_o_data[key] is None:
            where += "{}.isnull() & ".format(key)
        elif type(node_o_data[key])==str:
            where += "{}=='{}' & ".format(key,node_o_data[key])
        else:
            where += "{}=={} & ".format(key,node_o_data[key])

    for key in node_t_keys:
        if node_t_data[key] is None:
            where += "{}.isnull() & ".format(key)
        elif type(node_t_data[key])==str:
            where += "{}=='{}' & ".format(key,node_t_data[key])
        else:
            where += "{}=={} & ".format(key,node_t_data[key])
    where_q = where[0:len(where)-2]
    return where_q


def keys_of_group_name(node_group_name,node_schema):
    check = 0
    for node_frame in node_schema:
        if node_frame['node_group_name']==node_group_name:
            check+=1
            return node_frame['row_level_node_keys']
    if check == 0:
        raise Exception("You searched for a node group name that doesnt exist in the supplied node schema... {}".format(node_group_name))


def multi_cond_query(col_names:list,vals:list):
    if len(col_names)!=len(vals):
        raise Exception("You tried to create a df query with unequalivalent lengths of column array to value array... \n{}\n{}".format(str(col_names),str(vals)))
    where = ''
    for key in range(len(col_names)):
        if vals[key] is None:
            where += "{}.isnull() & ".format(col_names[key])
        elif type(vals[key])==str:
            where += "{}=='{}' & ".format(col_names[key],vals[key])
        else:
            where += "{}=={} & ".format(col_names[key],vals[key])
    where_q = where[0:len(where)-2]
    return where_q
