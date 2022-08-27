import pandas as pd
from lib.utils.tools import all_exist_in
from lib.dataframe_factory.node_linter import node_df_config_linter
from lib.dataframe_factory.df_utils import row_to_dict
from lib.object_factory.node_factory import Node
from lib.dataframe_factory.derived import derived_handler


def node_df_execution(dataframe, schema):

    nodes = []
    relationships = []

    # Lint
    if not all_exist_in(['nodes','relationships'], list(schema.keys())):
        raise Exception("You must provide a dict with the keys of 'nodes' and 'relationships', which should have type array. They can be set to empty arrays []")


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
            if type(node[key])==str:
                where += "{}=='{}' & ".format(key,node[key])
            else:
                where += "{}=={} & ".format(key,node[key])
        where_q = where[0:len(where)-2]
        return where_q


    for node in schema['nodes']:
        #node_df_config_linter(node)
        label = node['label']
        node_group_name=node['node_group_name']

        pd_cols = node['row_level_node_keys']
        unique_combinations = dataframe.groupby(pd_cols).size().reset_index().rename(columns={0:'count'})
        node_dicts = row_to_dict(unique_combinations,pd_cols)


        for node_row in node_dicts:
            # Get df data
            node_sub_data = dataframe.query(otm_query(node_row))
    
            ## Handle one_to_many first
            processed_otm=[]
            for otm in node['one_to_many']:
                attr_name = otm['attribute_name']
                level_cols_in_order = [otm['column_name']]
                col_sub_maps = []
                otm_trav(otm, col_sub_maps, level_cols_in_order)
                otm_sub_data = node_sub_data[level_cols_in_order]
                otm_sub_nodes = row_to_dict(otm_sub_data, level_cols_in_order)
                itemization = []
                if col_sub_maps == []:
                    generics = {}
                    d_vals = list(otm_sub_data[otm['column_name']].unique())
                    generics[attr_name]=d_vals
                    itemization.append(generics)
                    
                for mapping in col_sub_maps:
                    generics = {}
                    generics[attr_name]={}
                    keyname = mapping['first']
                    d_vals = list(otm_sub_data[keyname].unique())
                    generics[attr_name][keyname]=d_vals
                    
                    # generics[keyname]=d_vals
                    for val in mapping['then']:
                        generics[val]=list(otm_sub_data[val].unique())
                    itemization.append(generics)
                processed_otm.append(itemization)
                
            node_row['otm']=processed_otm


            processed_derv = []
            ## Then handle derive
            for derv in node['derived']:
                # Query for condition based on nodes
                derv_final = derived_handler(derv,node_sub_data)
                processed_derv.append(derv_final)
            node_row['derived']=processed_derv

        for indiv_node in node_dicts:
            nodes.append(Node(contents=indiv_node, label=label))

    return nodes, relationships
    