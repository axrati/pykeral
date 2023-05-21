# Pykeral

## A pandas to graph database library



### How to get started




### FOR CURRENT DEVELOPMENT ONLY:

*Pull this repository down, use the `test.py` and `manual_test.py` files to pass data through. One is built for reading csv's and the other is a manually created ```list[dict]```*  
```shell
git clone https://github.com/axrati/pykeral.git
```


### FUTURE STATE


*Install the library in the dist/ folder*  
```shell
pip install pykeral.whl
```

*Import dfxc and pandas from the main libraries*  
```python
from pykeral.main import dfxc
import pandas as pd
```

*Apply to a dataframe*  
```python
df = pd.Dataframe() # Your data goes here
dfx = dfxc(df)
```
<br>

*You're good to go. Use  **dfx.help()**  for a printout of helpful hints*

<br>

*To generate nodes/relationships on your dfx object, fish with a config dict (see below example)*  
```python
dfx.fish(config)
```

*Access the data*  
```python
dfx.nodes
dfx.relationships

dfx.nodes[0].help()
dfx.relationships[0].help()
```

*Generate queries (only Cypher today)*  
```python
dfx.query("cypher")
```

*Access Queries*  
```python
dfx.queries
```

<br>

**Take a look at the test.py file as an example of how to use this library.**

<br>

### Config

*This is the basic syntax for identifying nodes/relationships. Beneath the codeblock is a description of each key*  
```json
{
    "nodes": 
          [
          
              {"node_group_name": "a1", "label": "Place", "row_level_node_keys": ["State"], 
               "one_to_many": [
                               {  "attribute_name":"officials", "column_name":"leader_name" }
               ], 
               "derived": [
                   {"attribute_name": "employees_vaccinated", "operation": "SUM", "columns": ["Emp_Number_Vaccinated"]},
                   {"attribute_name": "employees_working", "operation": "SUM", "columns": ["Emp_Number_Working"]}
                   ]
               },
               
              {"node_group_name": "a2", "label": "Place", "row_level_node_keys": ["County"], 
               "one_to_many": [
                              {"attribute_name":"officials", "column_name":"leader_name", "sub_columns":[ {"column_name":"leader_child_name"}] }
               ], 
               "derived": [
                   {"attribute_name": "employees_vaccinated", "operation": "SUM", "columns": ["Emp_Number_Vaccinated"]},
                   {"attribute_name": "employees_working", "operation": "SUM", "columns": ["Emp_Number_Working"]}
                   ]
               }
               
          ], 
     "relationships": 
              [
                  {"rel_group_name": "rel_type_1",  "name": "HAS_SUBREGION",  "row_attributes": ["Mask Required"], 
                  "label": "geographic", "from": "a1", "to": "a2", 
                   "derived": [
                       {"attribute_name": "hospital_count", "operation": "SUM", "columns": ["Number of Hospitals"]}
                       ]
                   }
                      ]
      }
```


### Nodes

* **Node Group Name** - *The user-defined name of the node group to identify in the relationship block*
* **Label** - *A class based descriptor for this type of node... ie: Person, Place*
* **Row Level Node Keys** - *The rows that uniquely identify this node in the dataframe. Much like a database key, these must never be null. It is recommended that if you have a field that may be null, you can use the `"DISTINCT"` type in the `Derived` section*
* **One To Many** - *How to bring in data that has multiple values per Row Level Node Keys. More on this below*
* **Derived** - *Summary/Derived information at the node level, support operations listed below*

### Relationships

* **Relationship Group Name** - *The user-defined name of the relationship group for later use in dfx's*
* **Label** - *A class based descriptor for this type of relationship... ie: Geographic, Communication*
* **Row Level Attributes** - *Row level detail that you want to list as attributes between shared node groups*
* **Derived** - *Summary/Derived information at the relationship level, support operations listed below*


<br>

### One To Many
One to many relationships mean there are multiple values per row-level-keys on a given node. If you are looking for a simple array of a row value, its recommended you use the derived's `DISTINCT` configuration<br><br>
This functionality is more or less `JSON`'ifying your data and storing it as a string.<br><br> This was created because multi-level type dictionaries arent available in Neo4J - so this alternative is storing objects as strings. The first level requires the keys "attribute_name" and "column_name"... "sub_columns" is optional and only requires "column_name" after that.*

*Consider taking the below data:*
```python
raw_data =  [
     {"product_flavor":"Cherry", "flavor_version":"v1.0",  "product":"Coca-Cola", "vendor":"CC Corp"},
     {"product_flavor":"Cherry", "flavor_version":"v2.0", "product":"Coca-Cola", "vendor":"CC Corp"},
     {"product_flavor":"Cherry", "flavor_version":"v3.0", "product":"Coca-Cola", "vendor":"CC Corp"},
     {"product_flavor":"Lime", "flavor_version":"v1.0","product":"Coca-Cola", "vendor":"CC Corp"},
     {"product_flavor":"Lime", "flavor_version":"v2.0", "product":"Coca-Cola", "vendor":"CC Corp"},
     {"product_flavor":"Raspberry", "flavor_version":"v1.0","product":"Coca-Cola", "vendor":"CC Corp"}
     ]
df = pd.DataFrame(raw_data)
dfx = dfxc(df)
```

*And providing a node config of this:*
```
config = {
    'nodes': 
          [
              {"node_group_name": "a1", "label": "Drink", "row_level_node_keys": ["product"], 
               "one_to_many": [
                               {  "attribute_name":"flavors", "column_name":"product_flavor", "sub_columns":[{"column_name":"flavor_version"}] }
               ], 
               "derived": []
               }
          ], 
     'relationships': 
              []
      }
```

*You can expect this as a query outcome:*
```python
dfx.fish(config)
dfx.query_generator("cypher")

print(dfx.queries['nodes'][0])
"""
CREATE (n:Drink { 
        product:'Coca-Cola', 
        flavors:'{"flavors": [
                            {"product_flavor": "Cherry", "flavor_version": ["v1.0", "v2.0", "v3.0"]}, 
                            {"product_flavor": "Lime", "flavor_version": ["v1.0", "v2.0"]}, 
                            {"product_flavor": "Raspberry", "flavor_version": ["v1.0"]}
                            ]}', 
        pid:'134df4f9-3ca8-4c6a-aeba-8d69da173263' 
})
"""
```




<br>

### Derived

Derived calculates data to store as an attribute & works in the following way:

```
    (attribute_name)   (operation)     (columns)                
      average_time        AVG       ['play_minutes']                 
      total_pay           SUM       ['paycheck_amt', 'gift_amt']     
      last_visit          MAX       ['patient_visit_date', 'employee_visit_date']
      lowest_score        MIN       ['grade']
      unique_people      COUNTD     ['customer_id','salesman_id']
      num_of_visits      COUNT      ['person_id']
      unique_states     DISTINCT    ['state_abbreviation']      
                    
```

You can think of these as hamburger stacking multiple columns and deriving information.
    

--- *AVG, SUM, MAX, MIN will only work on numbers and dates. They calculate additively.<br>-.-.-.-.-.-.- For Example: MAX above will get the max date from the union of both columns.*

--- *AVG/MIN/MAX do not have comparitive support yet. <br>-.-.-.-.-.-.- For Example: AVG would give average across the values of both columns above for total_pay in the example above*

--- *COUNT, COUNTD will work on any datatype. COUNT will find the number of times the values are != na in pandas.*

--- *DISTINCT returns an array of the distinct values. It behaves similar to a one level one-to-many.*

--- *Planned configuration for custom calculations, TBD on priorities*


<br>

<br>


