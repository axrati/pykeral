def row_to_dict(dataframe,columns):
    decon_arr = []
    for index, row in dataframe.iterrows():
        data = {}
        for i in columns:
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
        if type(node_o_data[key])==str:
            where += "{}=='{}' & ".format(key,node_o_data[key])
        else:
            where += "{}=={} & ".format(key,node_o_data[key])

    for key in node_t_keys:
        if type(node_t_data[key])==str:
            where += "{}=='{}' & ".format(key,node_t_data[key])
        else:
            where += "{}=={} & ".format(key,node_t_data[key])
    where_q = where[0:len(where)-2]
    return where_q

