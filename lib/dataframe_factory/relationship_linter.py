from lib.utils.tools import all_exist_in


def relationship_df_config_linter(config):
    supplied_keys = list(config.keys())
    default_type = type(config)==dict
    mandatory_root_keys = ['rel_group_name','name', 'from','to','label']
    available_root_keys = ['rel_group_name','name', 'from','to','label', 'derived', "row_attributes"]
    mandatory_root_key_check = all_exist_in(mandatory_root_keys, supplied_keys)
    available_root_key_check = all_exist_in(supplied_keys, available_root_keys)
    if mandatory_root_key_check==False:
        raise Exception("A mandatory key in the relationship config input is not there. Mandatory keys are: \n{}".format(str(mandatory_root_keys)))
    if available_root_key_check==False:
        raise Exception("A key in the relationship input is not a valid key. Availabled keys are: \n{}".format(str(available_root_keys)))


    derived_check = all_exist_in(['derived'], supplied_keys)
    if derived_check:
        for derv in config['derived']:
            root_item_keys = list(derv.keys())
            mandatory_root_keys = ['attribute_name','operation','columns']
            mandatory_root_key_check = all_exist_in(mandatory_root_keys, root_item_keys)
            if mandatory_root_key_check == False:
                raise Exception("A key was provided to the derived_check input for a relationship that shouldnt be there. Available keys are:\n{}".format(str(mandatory_root_keys)))
            columns_dict_check = type(derv['columns'])==list
            if columns_dict_check == False:
                raise Exception("The columns argument for a derived relationship value was not provided a list, please supply an array of column strings or indexes:\n")
