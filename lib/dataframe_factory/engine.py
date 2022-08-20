import pandas as pd
from lib.utils.tools import all_exist_in
from lib.dataframe_factory.node_linter import node_df_config_linter
from lib.dataframe_factory.df_utils import row_to_dict
from lib.object_factory.node_factory import Node



def node_df_execution(dataframe, schema):

    nodes = []
    relationships = []

    # Lint
    if not all_exist_in(['nodes','relationships'], list(schema.keys())):
        raise Exception("You must provide a dict with the keys of 'nodes' and 'relationships', which should have type array. They can be set to empty arrays []")


    for node in schema['nodes']:
        node_df_config_linter(node)
        label = node['label']
        node_group_name=node['node_group_name']

        pd_cols = node['row_level_node_keys']
        unique_combinations = dataframe.groupby(pd_cols).size().reset_index().rename(columns={0:'count'})
        node_dicts = row_to_dict(unique_combinations,pd_cols)

        ## Handle one_to_many first

        ## Then handle derive
        for derv in node['derived']:
            # Query for condition based on nodes
            a=0

        for indiv_node in node_dicts:
            nodes.append(Node(contents=indiv_node, label=label))

    return nodes, relationships
    

    
