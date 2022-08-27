import pandas as pd
from datetime import datetime
from lib.object_factory.node_factory import Node
from lib.object_factory.relationship_factory import Relationship
from lib.dataframe_factory.engine import node_df_execution

def dfxc(df):
    return dfx(df)
    

class dfx:
    def __init__(self,data):
        ## Dataframe data
        self.data = data
        self.column_names = list(data.columns)
        column_types = []
        datatype_map = dict(data.dtypes.astype(str))
        for key in datatype_map.keys():
            if datatype_map[key]=="object":
                datatype_map.update({key:"string"})
                column_types.append(datatype_map[key])
            else:
                column_types.append(datatype_map[key])
        self.column_types = column_types
        self.datatype_map = datatype_map 

        rows = []
        idx = []
        for index, row in data.iterrows():
            rows.append(row)
            idx.append(index)
        self.rows = rows
        self.idx = idx

        ## Graph data
        self.nodes = []
        self.relationships = []
        # self.models = []


    def fish(self,config):
        nodes,relationships = node_df_execution(self.data, config)
        self.nodes = nodes
        self.relationships = relationships
        return nodes, relationships

    def template(self):
        data = {
        "nodes": [ { "node_group_name": "a1", "label":"Person", "row_level_node_keys":['id','name','age'], "one_to_many":[   {    "attribute_name":"work_data",   "column_name":"industry",    "sub_columns":[       { "column_name":"occupation_role_name" }    ]    }  ], "derived":[  {"attribute_name":"number_of_players", "operation":"COUNTD", "columns":['user_id']}  ]  }  ],
        "relationships": [{"rel_group_name":"rel_type_1","name":"HAS_INTEREST_IN","from":"a1","to":"a2","derived":[    {"attribute_name":"money_spent", "operation":"SUM", "columns":['transaction_amt']}] } ]
        }
        return data



    def help(self):
            print("""
Accessible properities:

  self.data === Original pandas dataframe
  self.column_names === Array of dataframe's columns
  self.column_types === Array of dataframe's datatypes
  self.datatype_map === Dictionary of column name to datatype mapping
  self.rows === Array of pandas Series objects
  self.idx === Array of row level indexes
  self.nodes === Array of Nodes class objects
  self.relationships === Array Relationship class objects


-- FUNCTION: .template() --

Returns a sample template to provide. Can be simply changed/modified and applied to .fish()



-- FUNCTION: .fish() --

Expects a schema of df to node/rel maps. Requires at least row_level_keys.

``````
one_to_many stores items as nested objects/arrays & works in the following way:
You must define what you want the root key name to be. 
    IF root_node has 1 level sub_teir:
        make root_node an array of those values
    ELIF root_node has >1 level sub_teir:
        make root_node dict w/ keys of distinct value, recursive until array of vals
``````
derived calculates data to store as an attribute & works in the following way:

    (attribute_name)   (operation)     (columns)                
      average_time        AVG       ['play_minutes']                 
      total_pay           SUM       ['paycheck_amt', 'gift_amt']     
      last_visit          MAX       ['patient_visit_date', 'employee_visit_date']
      unique_teams       COUNTD     ['sport_city','sport_name']
      num_of_visits       COUNT     ['person_id']
                    
    AVG, SUM, MAX, MIN will only work on numbers and dates. They calculate
    additively. MAX above will get the max date from the union of both columns.

    AVG/MIN/MAX to not have comparitive support yet. 
    ie: Average across the unique values of both columns above for total_pay

    COUNT, COUNTD will work on any datatype. It reads across, so in the example 
    above, team names shared by differentcities would be considered unique. 
    COUNT will find the number of times the values are != None

    Planned configuration for custom calculations

``````

Example Payload
{

    "nodes":[
                {
                    "node_group_name": "person_group_1"
                    "label":"Person",
                    "row_level_node_keys":['id','name','age','address'],
                    "one_to_many":[
                                { 
                                    "attribute_name":"employment"
                                    "column_name":"industry", 
                                    "sub_columns":[
                                        { "column_name":"occupation_role_name" }
                                    ]
                                }
                            ],
                    "derived":[]
                },
                                {
                    "node_group_name": "sport_group_1"
                    "label":"Sport",
                    "row_level_node_keys":['sport_name','sport_description'],
                    "one_to_many":[],
                    "derived":[
                        {"attribute_name":"number_of_players", "operation":"COUNTD", "columns":['user_id']}
                    ]
                },
                ...
            ]
    "relationships":[
                {
                    "rel_group_name":"rel_type_1",
                    "name":"HAS_INTEREST_IN",
                    "from":"person_group_1",
                    "to":"sport_group_1",
                    "derived":[
                        {"attribute_name":"money_spent", "operation":"SUM", "columns":['transaction_amt']}
                    ]
                }
        ]
}

              """)


