from pykeral.main import dfxc
import pandas as pd

#1) Read data
df = pd.read_csv("sample_vacc_data.csv")

#2) Create dfx
dfx = dfxc(df)

#3) Generate Schema
config = {
    'nodes': 
          [
              {'node_group_name': 'a1', 'label': 'Place', 'row_level_node_keys': ['State'], 
               'one_to_many': [], 
               'derived': [
                   {'attribute_name': 'employees_vaccinated', 'operation': 'SUM', 'columns': ['Emp_Number_Vaccinated']},
                   {'attribute_name': 'employees_working', 'operation': 'SUM', 'columns': ['Emp_Number_Working']}
                   ]
               },
              {'node_group_name': 'a2', 'label': 'Place', 'row_level_node_keys': ['County'], 
               'one_to_many': [], 
               'derived': [
                   {'attribute_name': 'employees_vaccinated', 'operation': 'SUM', 'columns': ['Emp_Number_Vaccinated']},
                   {'attribute_name': 'employees_working', 'operation': 'SUM', 'columns': ['Emp_Number_Working']}
                   ]
               }
          ], 
     'relationships': 
              [
                  {'rel_group_name': 'rel_type_1',  'name': 'HAS_SUBREGION',  'row_attributes': ['Mask Required'], 
                  'label': 'geographic', 'from': 'a1', 'to': 'a2', 
                   'derived': [
                       {'attribute_name': 'hospital_count', 'operation': 'SUM', 'columns': ['Number of Hospitals']}
                       ]
                   }
                      ]
      }

#4) Fish for nodes/rels
dfx.fish(config)

#5) Generate queries
dfx.query("cypher")

#6) Wrangle into DB (nodes first)
print(len(dfx.queries['nodes']))
print(len(dfx.queries['relationships']))
print("Have fun, use dfx.help()")
# dfx.help()