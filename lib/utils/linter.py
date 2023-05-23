from lib.utils.tools import all_exist_in

supported_operations = ['AVG','SUM','MAX','MIN','COUNT','COUNTD', 'DISTINCT']


def lint_handler(schema, dataframe_columns):
    """
    Lint the config object to ensure compliance and avoid mid-engine hiccups
    """
    # Top level check
    if not all_exist_in(['nodes','relationships'], list(schema.keys())):
        raise Exception("\nYou must provide a dict with the keys of 'nodes' and 'relationships', which should have type list. They can be set to empty lists []")

    # Key level check in schema
    for i in schema['nodes']:
        node_df_config_linter(i)
    for i in schema['relationships']:
        relationship_df_config_linter(i)
    
    # Content level check against schema/dataframe columns/supported operations
    node_column_level_check(schema['nodes'], dataframe_columns)
    relationship_node_group_name_linter(schema['nodes'], schema['relationships'], dataframe_columns)

    return 0


def otm_row_check(arr, mandatory_otm_sub_keys, available_otm_sub_keys):
    for i in arr:
        c_keys = list(i.keys())
        mandatory_check = all_exist_in(mandatory_otm_sub_keys, c_keys)
        available_check = all_exist_in(c_keys, available_otm_sub_keys)
        if mandatory_check==False:
            raise Exception("\nA mandatory key in the one-to-many input is not there. Mandatory keys are: \n{}".format(str(mandatory_otm_sub_keys)))
        if available_check==False:
            raise Exception("\nA key in the one-to-many input is not a valid key. Availabled keys are: \n{}".format(str(available_otm_sub_keys)))

        for j in c_keys:
            if j == 'sub_columns':
                otm_row_check(i[j], mandatory_otm_sub_keys, available_otm_sub_keys)

def node_df_config_linter(config):
    supplied_keys = list(config.keys())
    default_type = type(config)==dict
    mandatory_root_keys = ['node_group_name','row_level_node_keys','one_to_many','derived','label']
    available_root_keys = ['node_group_name','label','row_level_node_keys','one_to_many','derived']
    mandatory_root_key_check = all_exist_in(mandatory_root_keys, supplied_keys)
    available_root_key_check = all_exist_in(supplied_keys, available_root_keys)
    if mandatory_root_key_check==False:
        raise Exception("\nA mandatory key in the node config input is not there. Mandatory keys are: \n{}".format(str(mandatory_root_keys)))
    if available_root_key_check==False:
        raise Exception("\nA key in the node input is not a valid key. Availabled keys are: \n{}".format(str(available_root_keys)))


    one_to_many_check = all_exist_in(['one_to_many'], supplied_keys)

    if one_to_many_check:
        for item in config['one_to_many']:
            root_item_keys = list(item.keys())
            mandatory_otm_head_keys = ['attribute_name','column_name']
            available_otm_head_keys = ['attribute_name','column_name', 'sub_columns']
            mandatory_check = all_exist_in(mandatory_otm_head_keys,root_item_keys)
            available_check = all_exist_in(root_item_keys, available_otm_head_keys)
            if mandatory_check==False:
                raise Exception("\nA mandatory key in the one-to-many input is not there. Mandatory keys are: \n{}".format(str(mandatory_otm_sub_keys)))
            if available_check==False:
                raise Exception("\nA key in the one-to-many input is not a valid key. Availabled keys are: \n{}".format(str(available_otm_sub_keys)))

            mandatory_otm_sub_keys = ['column_name']
            available_otm_sub_keys = ['column_name', 'sub_columns']
            sub_check = all_exist_in(['sub_columns'], root_item_keys)
            if sub_check:
                otm_row_check(item['sub_columns'], mandatory_otm_sub_keys, available_otm_sub_keys)

    derived_check = all_exist_in(['derived'], supplied_keys)
    if derived_check:
        for derv in config['derived']:
            root_item_keys = list(derv.keys())
            mandatory_root_keys = ['attribute_name','operation','columns']
            mandatory_root_key_check = all_exist_in(mandatory_root_keys, root_item_keys)
            if mandatory_root_key_check == False:
                raise Exception("\nA key was provided to the node derived input that shouldnt be there. Available keys are:\n{}".format(str(mandatory_root_keys)))
            columns_dict_check = type(derv['columns'])==list
            if columns_dict_check == False:
                raise Exception("\nThe columns argument was not provided a list for a node derived value, please supply an array of column strings or indexes:\n")


def relationship_df_config_linter(config):
    supplied_keys = list(config.keys())
    default_type = type(config)==dict
    mandatory_root_keys = ['rel_group_name','name', 'from','to','label']
    available_root_keys = ['rel_group_name','name', 'from','to','label', 'derived', "row_attributes"]
    mandatory_root_key_check = all_exist_in(mandatory_root_keys, supplied_keys)
    available_root_key_check = all_exist_in(supplied_keys, available_root_keys)
    if mandatory_root_key_check==False:
        raise Exception("\nA mandatory key in the relationship config input is not there. Mandatory keys are: \n{}".format(str(mandatory_root_keys)))
    if available_root_key_check==False:
        raise Exception("\nA key in the relationship input is not a valid key. Availabled keys are: \n{}".format(str(available_root_keys)))


    derived_check = all_exist_in(['derived'], supplied_keys)
    if derived_check:
        for derv in config['derived']:
            root_item_keys = list(derv.keys())
            mandatory_root_keys = ['attribute_name','operation','columns']
            mandatory_root_key_check = all_exist_in(mandatory_root_keys, root_item_keys)
            if mandatory_root_key_check == False:
                raise Exception("\nA key was provided to the derived_check input for a relationship that shouldnt be there. Available keys are:\n{}".format(str(mandatory_root_keys)))
            columns_dict_check = type(derv['columns'])==list
            if columns_dict_check == False:
                raise Exception("\nThe columns argument for a derived relationship value was not provided a list, please supply an array of column strings or indexes:\n")

def relationship_node_group_name_linter(node_config, rel_config, dataframe_columns):
    node_group_names = []
    for n in node_config:
        node_group_names.append(n['node_group_name'])
    for r in rel_config:
        from_group = r['from']
        to_group = r['to']
        if from_group not in node_group_names:
            raise Exception(f"\nThe 'from' node_group in relationship group {r['rel_group_name']} does not exist in your node config ({from_group}). \nPlease supply a node_group_name in your node config:\n")
        if to_group not in node_group_names:
            raise Exception(f"\nThe 'to' node_group in relationship group {r['rel_group_name']} does not exist in your node config ({to_group}). \nPlease supply a node_group_name in your node config:\n")
        for ra in r['row_attributes']:
            if ra not in dataframe_columns:
                raise Exception(f"\nThe column '{ra}' provided in relationship group '{r['rel_group_name']}' doesn't exist in your dataframe. \nPlease supply a column_name that exists in your dataframe:\n{str(dataframe_columns)}")
        for der in r['derived']:
            if der['operation'] not in supported_operations:
                raise Exception("An invalid value was sent to the derived handler. Valid values are: \n{}".format(str(supported_operations)))
            for derc in der['columns']:
                if derc not in dataframe_columns:
                    raise Exception(f"\nThe column '{derc}' provided in relationship group '{r['rel_group_name']}' doesn't exist in your dataframe. \nPlease supply a column_name that exists in your dataframe:\n{str(dataframe_columns)}")
            
        
def node_column_level_check(node_config, dataframe_columns):
    for n in node_config:
        ng_name = n['node_group_name']
        for rlnk in n['row_level_node_keys']:
            if rlnk not in dataframe_columns:
                raise Exception(f"\nThe column '{rlnk}' provided in node group '{ng_name}' doesn't exist in your dataframe. \nPlease supply a column_name that exists in your dataframe:\n{str(dataframe_columns)}")
        for otm in n['one_to_many']:
            if otm['column_name'] not in dataframe_columns:
                raise Exception(f"\nThe column '{otm['column_name']}' provided in node group '{ng_name}' doesn't exist in your dataframe. \nPlease supply a column_name that exists in your dataframe:\n{str(dataframe_columns)}")
            otm_keys = list(otm.keys())
            if "sub_columns" in otm_keys:
                for sc in otm['sub_columns']:
                    if sc['column_name'] not in dataframe_columns:
                        raise Exception(f"\nThe column '{sc['column_name']}' provided in node group '{ng_name}' doesn't exist in your dataframe. \nPlease supply a column_name that exists in your dataframe:\n{str(dataframe_columns)}")
        for der in n['derived']:
            if der['operation'] not in supported_operations:
                raise Exception("An invalid value was sent to the derived handler. Valid values are: \n{}".format(str(supported_operations)))
            for der_c in der['columns']:
                if der_c not in dataframe_columns:
                    raise Exception(f"\nThe column '{der_c}' provided in node group '{ng_name}' doesn't exist in your dataframe. \nPlease supply a column_name that exists in your dataframe:\n{str(dataframe_columns)}")