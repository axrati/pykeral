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
              {'node_group_name': 'state-level-nodes', 'label': 'Place', 'row_level_node_keys': ['State'], 
               'one_to_many': [], 
               'derived': [
                   {'attribute_name': 'employees_vaccinated', 'operation': 'SUM', 'columns': ['Emp_Number_Vaccinated']},
                   {'attribute_name': 'employees_working', 'operation': 'SUM', 'columns': ['Emp_Number_Working']},
                   {'attribute_name': 'unique_counties', 'operation': 'DISTINCT', 'columns': ['County']}
                   ]
               },
              {'node_group_name': 'county-level-nodes', 'label': 'Place', 'row_level_node_keys': ['County'], 
               'one_to_many': [], 
               'derived': [
                   {'attribute_name': 'employees_vaccinated', 'operation': 'SUM', 'columns': ['Emp_Number_Vaccinated']},
                   {'attribute_name': 'employees_working', 'operation': 'SUM', 'columns': ['Emp_Number_Working']}
                   ]
               }
          ], 
     'relationships': 
              [
                  {
                    'rel_group_name': 'rel_type_1',  'name': 'HAS_SUBREGION',  'row_attributes': ['Mask Required'], 
                    'label': 'geographic', 'from': 'state-level-nodes', 'to': 'county-level-nodes', 
                    'derived': [
                        {'attribute_name': 'hospital_count', 'operation': 'SUM', 'columns': ['Number of Hospitals']}
                        ]
                    }
                      ]
      }

#4) Fish for nodes/rels
dfx.fish(config)

#5) Generate queries
dfx.query_generator("cypher")

# #6) Connect to database
# dfx.connect("Neo4j", { "host":"localhost", "port":7687, "database":"neo4j", "username":"neo4j", "password":"password" } )

# #7) Query/Create your data
for query in dfx.queries['nodes']:
    # dfx.dbconn.query(query,False)
    print(query)
for query in dfx.queries['relationships']:
    print(query)
    # dfx.dbconn.query(query,False)

# #8) Validate Data
# new_data = dfx.dbconn.query("match (n)-[p]-(m) return n,p,m limit 10")
# print(new_data)











