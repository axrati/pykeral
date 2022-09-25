import pandas as pd
from lib.utils.tools import all_exist_in, q_dtype

def cypher_compiler(dfx):
    nodes = dfx.nodes
    relationships = dfx.relationships

    node_queries = []
    relationship_queries = []

    for node in nodes:
        label = node.label
        node_id = node.id
        node_attrs = list(node.data.keys())
        
        attr_string = ""
        for key in node_attrs:
            if key == "otm":
                for otm in node.data[key]:
                    for sub in otm:
                        otm_keys = list(sub.keys())
                        for otm_key in otm_keys:
                            attr_string+=q_dtype(otm_key, sub)
            elif key == "derived":
                for derv in node.data[key]:
                    derv_keys = list(derv.keys())
                    for derv_key in derv_keys:
                        attr_string+=q_dtype(derv_key,derv)
            else:
                attr_string+=q_dtype(key,node.data)
        attr_string+=q_dtype('pid',{"pid":node_id})


        attr_string = attr_string[0:len(attr_string)-2]
        cypher_query = "CREATE (n:{} {{ {} }})".format(label,attr_string)
        node_queries.append(cypher_query)

    for relationship in relationships:
        node_access = relationship.from_to_nodes
        from_node_id = node_access['from']
        to_node_id = node_access['to']
        label = relationship.label
        rel_attrs = list(relationship.data.keys())
        rel_id = relationship.id

        attr_string = ""
        for key in rel_attrs:
            if key in ["schema","rel_group_name", "name", "id"]:
                continue
            elif key == "otm":
                for otm in relationship.data[key]:
                    for sub in otm:
                        otm_keys = list(sub.keys())
                        for otm_key in otm_keys:
                            attr_string+=q_dtype(otm_key, sub)
            elif key == "derived":
                for derv in relationship.data[key]:
                    derv_keys = list(derv.keys())
                    for derv_key in derv_keys:
                        attr_string+=q_dtype(derv_key,derv)
            else:
                attr_string+=q_dtype(key,relationship.data)

        samp = {"label":label, "pid":rel_id}
        attr_string+= q_dtype("label",samp)
        attr_string+= q_dtype("pid",samp)

        attr_string = attr_string[0:len(attr_string)-2]

        rel_query = """
        MATCH (n) where n.pid='{}'
        MATCH (m) where m.pid='{}'
        WITH n,m
        CREATE (n)-[p:{} {{ {} }}]->(m)
        """.format(from_node_id, to_node_id, relationship.data['name'], attr_string)
        relationship_queries.append(rel_query)
    
    return node_queries, relationship_queries
