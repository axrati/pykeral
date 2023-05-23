import pandas as pd
from lib.utils.linter import lint_handler
from lib.dataframe_factory.df_utils import row_to_dict, keys_of_group_name, multi_cond_query
from lib.object_factory.node_factory import Node
from lib.object_factory.relationship_factory import Relationship
from lib.dataframe_factory.derived import derived_handler
from lib.dataframe_factory.otm import otm_trav, otm_query


def node_df_execution(dataframe, schema):
    """
    This is the main execution environment for passing the config object and turning them into Node and Relationship classes.
    Expects a dataframe and the config object to apply to it.
    """
    nodes = []
    relationships = []
    node_id_map = {}
    # Access map for df of key/vals and node_id under `pykeral_id`
    node_gn_map = {}

    # First Lint to avoid mid-engine execution errors
    lint_handler(schema, list(dataframe.columns))

    # Node
    for node in schema['nodes']:
        # Head print for updates
        print("\n")

        # Global frame for node-group-name dataframes
        gn_map_frames=[]

        # Basic node info
        label = node['label']
        node_group_name=node['node_group_name']
        pd_cols = node['row_level_node_keys']

        # Should count be here as a means of "checkout"?
        unique_combinations = dataframe.groupby(pd_cols).size().reset_index().rename(columns={0:'count'})
        # This matches index of unique_combos
        node_dicts = row_to_dict(unique_combinations,pd_cols)


        num_dicts = len(node_dicts)
        node_num_iterator = 0
        # Process node dictionaries inline
        for node_row in node_dicts:
            node_num_iterator+=1
            print(f"Creating {node_group_name} nodes : {node_num_iterator} / {num_dicts}", end="\r")
            # Get df data
            node_sub_data = dataframe.query(otm_query(node_row))
            # Should this get attached?^^^^
    
            ## Handle one_to_many first
            processed_otm=[]
            for otm in node['one_to_many']:
                attr_name = otm['attribute_name']
                distinct_head_values = list(node_sub_data[otm['column_name']].unique())
                col_sub_maps = []
                level_cols_in_order = [otm['column_name']]
                otm_trav(otm, col_sub_maps, level_cols_in_order)

                # If there are no sub_columns, then create the plain object
                if col_sub_maps == []:
                    generics = []
                    for dhv in distinct_head_values:
                        otm_sub_data = node_sub_data[node_sub_data[otm['column_name']]==dhv]
                        sub_obj = {otm['column_name']:dhv}
                        generics.append(sub_obj)
                    processed_otm.append({attr_name:generics})
                    
                # Else youre going to loop over it
                for mapping in col_sub_maps:
                    keyname = mapping['first']
                    generics = []
                    for dhv in distinct_head_values:
                        otm_sub_data = node_sub_data[node_sub_data[otm['column_name']]==dhv]
                        sub_obj = {keyname:dhv}
                        for val in mapping['then']:
                            sub_obj[val]=list(otm_sub_data[val].unique())
                        generics.append(sub_obj)
                    # End Mapping, add to otm's
                    processed_otm.append({attr_name:generics})
            # Append to node_row
            node_row['otm']=processed_otm




            ## Then handle derived
            processed_derv = []
            for derv in node['derived']:
                derv_final = derived_handler(derv,node_sub_data)
                if derv_final is not None:
                    processed_derv.append(derv_final)
            # Append to node_row
            node_row['derived']=processed_derv



            # Create Actual Node
            new_node = Node(self_name=node_group_name, contents=node_row, label=label, keys=pd_cols)
            # Recreate original node_dict for dataframe compliance
            raw_node = {}
            for key in pd_cols:
                raw_node[key]=node_row[key]
            raw_node["pykeral_id"]=new_node.id
            raw_node_df = pd.DataFrame([raw_node])
            gn_map_frames.append(raw_node_df)
            # Add to all node arr
            nodes.append(new_node)
            # Add to node by id dict
            node_id_map[new_node.id]=new_node


        # Create Group Name Map, attach to global function Obj
        total_gn_map = pd.concat(gn_map_frames)
        node_gn_map[node_group_name]=total_gn_map




    print("\n")
    # Relationship
    for relationship in schema['relationships']:

        relationship_label = relationship['label']
        from_group = relationship['from']
        to_group = relationship['to']
        rel_group_name = relationship['rel_group_name']
        row_attrs = relationship['row_attributes']
        from_keys = keys_of_group_name(from_group,schema['nodes'])
        to_keys = keys_of_group_name(to_group,schema['nodes'])
        from_ng_map = node_gn_map[from_group]
        to_ng_map = node_gn_map[to_group]

        # Create combined df between the keys:
        # f-a, f-b, fc   t-a, t-b
        # Join in the key data.
        # IMPORTANT - X will always be first join, Y will always be second.
        # In this code it'll be from->to
        mapped_nodes_df = dataframe.merge(from_ng_map,on=from_keys).merge(to_ng_map,on=to_keys)
        # ^^^ Use this data for all your queries now, as you only want sums and stuff from this
        pure_id_map = mapped_nodes_df[['pykeral_id_x','pykeral_id_y']]
        pure_id_map=pure_id_map.drop_duplicates()
        pure_id_map=pure_id_map.rename(columns={'pykeral_id_x':"from",'pykeral_id_y':"to"})
        potential_match_count = len(pure_id_map)
        # Loop through these combinations and apply relationship
        for idx,row in pure_id_map.iterrows():
            print(f"Creating ({from_group}) --> ({to_group}) relationship {idx+1} / {potential_match_count}", end="\r")
            df_from_id = row['from']
            df_to_id = row['to']
            sub_query = multi_cond_query(["pykeral_id_x","pykeral_id_y"],[df_from_id, df_to_id])
            potential_rel = mapped_nodes_df.query(sub_query)

            # Relationship skeleton
            relationship_frame = {}
            all_rel_columns = list(potential_rel.columns)
            # Add any row attributes
            if len(row_attrs)>0:
                for attr in row_attrs:
                    # This should always be 1 if its true row-level-attributes, but we array it just in case
                    attrs_df_vals = list(potential_rel[attr].unique())
                    if len(attrs_df_vals)==1:
                        relationship_frame[attr]=attrs_df_vals[0]
                    else:
                        relationship_frame[attr]=attrs_df_vals
            schema_data = row_to_dict(potential_rel,all_rel_columns)
            relationship_frame['name']=relationship['name']
            relationship_frame['rel_group_name']=rel_group_name
            from_to_nodes={"from":df_from_id, "to":df_to_id}
            # Get Derived
            relationship_derived = []
            for derv in relationship['derived']:
                derv_final = derived_handler(derv,potential_rel)
                if derv_final is not None:
                    relationship_derived.append(derv_final)
            relationship_frame['derived']=relationship_derived
            relationship_frame['schema']=schema_data
            final_rel = Relationship(schema=relationship_frame, from_to_nodes=from_to_nodes, label=relationship_label)
            relationships.append(final_rel)
            # Manage Node Data
            rel_id = final_rel.id
            node_rel_data = {"from":df_from_id, "to":df_to_id, "relationship_id":rel_id}
            node_id_map[df_from_id].add_rel("from",node_rel_data)
            node_id_map[df_to_id].add_rel("to",node_rel_data)


    print("\n")
    return nodes, relationships