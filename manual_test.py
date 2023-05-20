from pykeral.main import dfxc
import pandas as pd

#1) Create Data
raw_data = [
    {"name":"Alex", "gender":"M", "legal":True, "employer":None, "visit_cost":111, "child":"Joanna", "child_age":18, "hospital":"A"},
    {"name":"Alex", "gender":"M", "legal":True, "employer":None, "visit_cost":111, "child":"Brick", "child_age":12, "hospital":"A"},
    {"name":"George", "gender":"M", "legal":False, "employer":"Aetna", "visit_cost":500, "child":"Karen", "child_age":12, "hospital":"A"},
    # {"name":"Jill", "gender":"F", "legal":True, "employer":"Aetna", "visit_cost":500, "child":None, "child_age":None, "hospital":"A"},
    {"name":"Margret", "gender":"F", "legal":True, "employer":"Walmart", "visit_cost":2222, "child":"Brick", "child_age":24, "hospital":"B"},
    {"name":"Carolina", "gender":"F", "legal":False, "employer":"Target", "visit_cost":999, "child":"Brick", "child_age":23, "hospital":"B"},
    {"name":"Rodrigo", "gender":"M", "legal":False, "employer":None, "visit_cost":1500, "child":"Betty", "child_age":45, "hospital":"B"},
]
df = pd.DataFrame(raw_data)

#2) Create dfx
dfx = dfxc(df)

#3) Generate Schema
config = {
    'nodes': 
          [
              {'node_group_name': 'a1', 'label': 'Person', 'row_level_node_keys': ['name','gender','legal'], 
               'one_to_many': [
                              {"attribute_name":"children", "column_name":"child", "sub_columns":[ {"column_name":"child_age"}] },
                            # {"attribute_name":"child_age", "column_name":"child_age" }
               ],  
               'derived': [
                   {'attribute_name': 'total_claims', 'operation': 'SUM', 'columns': ['visit_cost']},
                   {'attribute_name': 'total_visits', 'operation': 'COUNT', 'columns': ['name']}
                   ]
               },
              {'node_group_name': 'a2', 'label': 'Employer', 'row_level_node_keys': ['employer'], 
               'one_to_many': [], 
               'derived': [
                   {'attribute_name': 'total_employees', 'operation': 'COUNTD', 'columns': ['name']},
                   {'attribute_name': 'total_costs', 'operation': 'SUM', 'columns': ['visit_cost']}
                   ]
               },
            {'node_group_name': 'a3', 'label': 'Hospital', 'row_level_node_keys': ['hospital'], 
               'one_to_many': [], 
               'derived': [
                   {'attribute_name': 'total_visits', 'operation': 'COUNT', 'columns': ['name']},
                   {'attribute_name': 'total_costs', 'operation': 'SUM', 'columns': ['visit_cost']}
                   ]
               }
          ], 
     'relationships': 
              [
                  {'rel_group_name': 'rel_type_1',  'name': 'HAS_EMPLOYEE',  'row_attributes': [], 
                  'label': 'employoment', 'from': 'a2', 'to': 'a1', 
                   'derived': [
                       {'attribute_name': 'total_cost', 'operation': 'SUM', 'columns': ['visit_cost']}
                       ]
                   },
                {'rel_group_name': 'rel_type_2',  'name': 'VISITED_HOSPITAL',  'row_attributes': [], 
                  'label': 'employoment', 'from': 'a1', 'to': 'a3', 
                   'derived': [
                       {'attribute_name': 'total_cost', 'operation': 'SUM', 'columns': ['visit_cost']},
                        {'attribute_name': 'total_visits', 'operation': 'COUNT', 'columns': ['name']}
                       ]
                   },
                    ]
      }

#4) Fish for nodes/rels
dfx.fish(config)

#5) Generate queries
dfx.query_generator("cypher")

# #6) Connect to database
# dfx.connect("Neo4j", { "host":"localhost", "port":7687, "database":"neo4j", "username":"neo4j", "password":"password" } )

#7) Query/Create your data
for query in dfx.queries['nodes']:
    # dfx.dbconn.query(query,False)
    print(query)
for query in dfx.queries['relationships']:
    print(query)
    # dfx.dbconn.query(query,False)

# #8) Validate Data
# new_data = dfx.dbconn.query("match (n)-[p]-(m) return n,p,m limit 10")
# print(new_data)

# print(dfx.queries)









