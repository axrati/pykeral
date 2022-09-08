from lib.utils.tools import all_exist_in

# {
#     "node_group_name": "person_group_1"
#     "label":"Person",
#     "row_level_node_keys":['id','name','age','address'],
#     "one_to_many":[
#                 { 
#                     "attribute_name":"employment"
#                     "column_name":"industry", 
#                     "sub_columns":[
#                         { "column_name":"occupation_role_name", "sub_columns":[
#                                                                   {"column_name":"occupation_cities"}
#                                                                               ] }
#                     ]
#                 }
#             ],
#     "derived":[]
# },
#                 {
#     "node_group_name": "sport_group_1"
#     "label":"Sport",
#     "row_level_node_keys":['sport_name','sport_description'],
#     "one_to_many":[],
#     "derived":[
#         {"attribute_name":"number_of_players", "operation":"COUNTD", "columns":['user_id']}
#     ]
# },

def otm_row_check(arr, mandatory_otm_sub_keys, available_otm_sub_keys):
    for i in arr:
        c_keys = list(i.keys())
        mandatory_check = all_exist_in(mandatory_otm_sub_keys, c_keys)
        available_check = all_exist_in(c_keys, available_otm_sub_keys)
        if mandatory_check==False:
            raise Exception("A mandatory key in the one-to-many input is not there. Mandatory keys are: \n{}".format(str(mandatory_otm_sub_keys)))
        if available_check==False:
            raise Exception("A key in the one-to-many input is not a valid key. Availabled keys are: \n{}".format(str(available_otm_sub_keys)))

        for j in c_keys:
            if j == 'sub_columns':

                otm_row_check(i[j], mandatory_otm_sub_keys, available_otm_sub_keys)

def node_df_config_linter(config):
    supplied_keys = list(config.keys())
    default_type = type(config)==dict
    mandatory_root_keys = ['node_group_name','row_level_node_keys']
    available_root_keys = ['node_group_name','label','row_level_node_keys','one_to_many','derived']
    mandatory_root_key_check = all_exist_in(mandatory_root_keys, supplied_keys)
    available_root_key_check = all_exist_in(supplied_keys, available_root_keys)
    if mandatory_root_key_check==False:
        raise Exception("A mandatory key in the config input is not there. Mandatory keys are: \n{}".format(str(mandatory_root_keys)))
    if available_root_key_check==False:
        raise Exception("A key in the one-to-many input is not a valid key. Availabled keys are: \n{}".format(str(available_root_keys)))


    one_to_many_check = all_exist_in(['one_to_many'], supplied_keys)

    if one_to_many_check:
        for item in config['one_to_many']:
            root_item_keys = list(item.keys())
            mandatory_otm_head_keys = ['attribute_name','column_name']
            available_otm_head_keys = ['attribute_name','column_name', 'sub_columns']
            mandatory_check = all_exist_in(mandatory_otm_head_keys,root_item_keys)
            available_check = all_exist_in(root_item_keys, available_otm_head_keys)
            if mandatory_check==False:
                raise Exception("A mandatory key in the one-to-many input is not there. Mandatory keys are: \n{}".format(str(mandatory_otm_sub_keys)))
            if available_check==False:
                raise Exception("A key in the one-to-many input is not a valid key. Availabled keys are: \n{}".format(str(available_otm_sub_keys)))

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
            mandatory_root_key_check = all_exist_in(mandatory_root_keys, supplied_keys)
            if mandatory_root_key_check == False:
                raise Exception("A key was provided to the derived_check input that shouldnt be there. Available keys are:\n{}".format(str(mandatory_root_key_check)))
            columns_dict_check = type(derv['columns'])==list
            if columns_dict_check == False:
                raise Exception("The columns argument was not provided a list, please supply an array of column strings or indexes:\n")
