from pykeral.main import dfxc
import pandas as pd

#1) Create Data
raw_data =  [
     {"product_flavor":"Cherry", "flavor_version":"v1.0",  "product":"Coca-Cola", "vendor":"CC Corp", "cost":123},
     {"product_flavor":"Cherry", "flavor_version":"v2.0", "product":"Coca-Cola", "vendor":"CC Corp", "cost":123},
     {"product_flavor":"Cherry", "flavor_version":"v3.0", "product":"Coca-Cola", "vendor":"CC Corp", "cost":123},
     {"product_flavor":"Lime", "flavor_version":"v1.0","product":"Coca-Cola", "vendor":"CC Corp", "cost":None},
     {"product_flavor":"Lime", "flavor_version":"v2.0", "product":"Coca-Cola", "vendor":"CC Corp", "cost":None},
     {"product_flavor":"Raspberry", "flavor_version":"v1.0","product":"Coca-Cola", "vendor":"CC Corp", "cost":123}
     ]
df = pd.DataFrame(raw_data)

#2) Create dfx
dfx = dfxc(df)

#3) Generate Schema
config = {
    'nodes': 
          [
              {"node_group_name": "a1", 
               "label": "Vendor", 
               "row_level_node_keys": ["vendor"], 
               "one_to_many": [], 
               "derived": []
               },
                {"node_group_name": "a2", 
                "label": "Product", 
                "row_level_node_keys": ["product","product_flavor"], 
                "one_to_many": [], 
                "derived": []
               }
          ], 
     'relationships': 
              [
                {
                    "rel_group_name": "manuf_to_soda",  "label": "genesis_type_relationship",  "name": "CREATED",  
                    "derived": [],
                    "from": "a1", "to": "a2",
                    "row_attributes": ["flavor_version"], 
                   }
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
