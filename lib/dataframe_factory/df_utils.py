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
    for node in nodes:
        if node.self_name == rel_name:
            check+=1
            return node
    if check == 0:
        raise Exception("You aligned a relationship to a node group name that doesnt exist... check name: {}".format(rel_name))