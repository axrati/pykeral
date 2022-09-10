import pandas as pd
from lib.utils.tools import all_exist_in
from lib.dataframe_factory.node_linter import node_df_config_linter
from lib.dataframe_factory.relationship_linter import relationship_df_config_linter
from lib.dataframe_factory.df_utils import row_to_dict, get_node, rel_query_gen
from lib.object_factory.node_factory import Node
from lib.object_factory.relationship_factory import Relationship
from lib.dataframe_factory.derived import derived_handler
from lib.dataframe_factory.otm import otm_trav, otm_levels, otm_query

def node_df_execution(dataframe, schema):

    nodes = []
    relationships = []




    # Lint
    if not all_exist_in(['nodes','relationships'], list(schema.keys())):
        raise Exception("You must provide a dict with the keys of 'nodes' and 'relationships', which should have type array. They can be set to empty arrays []")




    # Node
    for node in schema['nodes']:
        # Lint for errors
        node_df_config_linter(node)

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
                derv_final = derived_handler(derv,node_sub_data)
                processed_derv.append(derv_final)
            node_row['derived']=processed_derv

        for indiv_node in node_dicts:
            nodes.append(Node(self_name=node_group_name, contents=indiv_node, label=label, keys=pd_cols))






    # Relationship
    for relationship in schema['relationships']:
        #Lint for errors
        relationship_df_config_linter(relationship)

        relationship_label = relationship['label']
        from_group = relationship['from']
        to_group = relationship['to']
        rel_group_name = relationship['rel_group_name']
        from_nodes = get_node(from_group,nodes)
        to_nodes = get_node(to_group, nodes)
        row_attrs = relationship['row_attributes']

        for frm in from_nodes:
            frm_keys = frm.keys
            frm_data = frm.data
            frm_id = frm.id
            for tom in to_nodes:
                tom_keys = tom.keys
                tom_data = tom.data
                tom_id = tom.id
                sub_query = rel_query_gen(frm_keys, frm_data, tom_keys, tom_data)
                potential_rel = dataframe.query(sub_query)

                if len(potential_rel)>0:
                    relationship_frame = {}
                    all_rel_columns = list(potential_rel.columns)
                    if len(row_attrs)>0:
                        for attr in row_attrs:
                            attrs_df_vals = list(potential_rel[attr].unique())
                            if len(attrs_df_vals)==1:
                                relationship_frame[attr]=attrs_df_vals[0]
                            else:
                                relationship_frame[attr]=attrs_df_vals
                    schema_data = row_to_dict(potential_rel,all_rel_columns)
                    relationship_frame['name']=relationship['name']
                    relationship_frame['rel_group_name']=rel_group_name
                    from_to_nodes={"from":frm_id, "to":tom_id}
                    # Get Derived
                    relationship_derived = []
                    for derv in relationship['derived']:
                        derv_final = derived_handler(derv,potential_rel)
                        relationship_derived.append(derv_final)
                    relationship_frame['derived']=relationship_derived
                    relationship_frame['schema']=schema_data
                    final_rel = Relationship(schema=relationship_frame, from_to_nodes=from_to_nodes, label=relationship_label)
                    relationships.append(final_rel)
                    # Manage Node Data
                    rel_id = final_rel.id
                    node_rel_data = {"from":frm_id, "to":tom_id, "relationship_id":rel_id}
                    frm.add_rel("from",node_rel_data)
                    tom.add_rel("to",node_rel_data)


                    
    return nodes, relationships
    