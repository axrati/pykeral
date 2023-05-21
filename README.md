# Pykeral

<style>h1,h2,h3,h4 { border-bottom: 0; } </style>

### Turn your relational data into ready-to-query graph databases. 
____
###### Put your data into a dataframe and provide a simple configuration....

```python 
Thats all it takes.
```
###### This library generates all the queries you need to instantiate your dataframes in a graph database.

______

<br>



# Get Started - Link to "Hello, World!"

#### Pull this repository down, click below for the "Hello, World!". You can run/edit the `test.py` & `manual_test.py` files directly or create your own.
<br>

[`CLICK HERE TO VIEW OUR "HELLO WORLD"`](#hello-world-tutorial) 

<br>



```shell
git clone https://github.com/axrati/pykeral.git
```
```shell
python test.py
```


###### *Coming soon it will be `pip install pykeral`*

<br>

______



<br>



# Pykeral Workflow/Process Explanation

This section will show you how you should be using pykeral to seed your graph database.


# `STEP ONE` 
### Generate Nodes/Relationships

*The first thing you do is create a `dfx` object. To create one, all you need is a dataframe. Import dfxc and pandas from the main libraries.*

```python
from pykeral.main import dfxc
import pandas as pd
```

*Apply to a dataframe*  
```rust
df = pd.Dataframe() // Add your dataframe here - this can be empty for now
dfx = dfxc(df)
```

*To generate nodes/relationships on your dfx object, you will provide a `config` object and pass it to the `fish()` function.* [Click here to see how to create the `config` object.](#hello-world-tutorial)


```rust
dfx.fish(config)
```

*When you run `fish()`, it creates all of your nodes/relationships and makes it available to the `dfx` object... You'll be able to access them like this*

```haskell
dfx.nodes
dfx.relationships
```

*These are arrays containing your nodes and relationships. Each node/array has a `.help()` function that provides data accessing information*
```python
first_node = dfx.nodes[0]
first_node.help()

first_relationship = dfx.relationships[0]
first_relationship.help()
```


_____

<br>


# `STEP TWO` 
### Generate Queries for Graph Database

<br>

 *To generate your graph database migration queries, call the `.query_generator()` on your `dfx` object.*

###### *This feature only supports Cypher today* 
```rust
dfx.query_generator("cypher")
``` 


<br>


*This will make your queries accessible here. You'll need to execute your `Node` queries before your `Relationship` queries.*  
```golang
for query in dfx.queries['nodes']:
    print(query)
for query in dfx.queries['relationships']:
    print(query)
```

_____

<br>

# `STEP THREE` 
### Connect to database and populate data


<br>

*You can connect to Neo4j directly using `.connect()`. This is the basic localhost setup, but you can configure any connection you need.*  
```rust
dfx.connect("Neo4j", { "host":"localhost", "port":7687, "database":"neo4j", "username":"neo4j", "password":"password" } )
```

<br>

*From here, you can loop through your queries to seed your database. We provide `False` in the `.query()` because we are not expecting any data back.*  
```rust
for n_query in dfx.queries['nodes']:
    dfx.dbconn.query(n_query,False)

for r_query in dfx.queries['relationships']:
    dfx.dbconn.query(r_query,False)
```

<br>

*Your database is now seeded! You can use `dfx.query()` to pull data directly from your databse.*
```javascript
query = "MATCH (n)-[p]-(m) RETURN n,p,m"
results = dfx.dbconn.query(query)
```



<br>
<br>
<br>
<br>

_____

<br>

# Hello World Tutorial

*Let's start with a simple dataframe*  
```python
raw_data = [
     {"product_flavor":"Cherry", "flavor_version":"v1.0",  "product":"Coca-Cola", "vendor":"CC Corp"},
     {"product_flavor":"Cherry", "flavor_version":"v2.0", "product":"Coca-Cola", "vendor":"CC Corp"},
     {"product_flavor":"Lime", "flavor_version":"v1.0","product":"Coca-Cola", "vendor":"CC Corp"},
     {"product_flavor":"Lime", "flavor_version":"v2.0", "product":"Coca-Cola", "vendor":"CC Corp"},
     {"product_flavor":"Raspberry", "flavor_version":"v1.0","product":"Coca-Cola", "vendor":"CC Corp"}
     ]

data = pd.DataFrame(raw_data)
```


*Here is the syntax for identifying nodes/relationships. The high level object is a dictionary with a key of `nodes` and `relationship`*

```python
config = {
    "nodes":[],
    "relationships":[]
}
```

<br>

*This would be a simple `node_configuration` that we can add to the nodes array*

```json
    {
        "node_group_name": "manufacturers", 
        "label": "Manufacturer", 
        "row_level_node_keys": ["vendor"], 
        "one_to_many": [], 
        "derived": []
    }
```

*The `row_level_node_keys` basically identify the unique columns in your Dataframe that represent an object.*<br>
*Using the beginner dataset, this would only identify one node: `CC Corp`*<br>
*It's basically asking for the primary keys of a Node. No column provided should ever have a `NULL`.*

<br>
*Here is a sample node configuration:


```json
{
    "node_group_name": "a1", 
    "label": "Drink", 
    "row_level_node_keys": ["product"], 
    "one_to_many": [
                       {  
                            "attribute_name":"flavors", 
                            "column_name":"product_flavor", 
                            "sub_columns":[{"column_name":"flavor_version"}]
                        }
                   ], 
   "derived": []
               }
```


```json
{
    "nodes": 
          [
          
              {"node_group_name": "drinks", "label": "Drinks", "row_level_node_keys": ["soda_name","soda_manufacturer"], 
               "one_to_many": [
                               {  "attribute_name":"flavors", "column_name":"soda_flavor" }
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
<br>
You can think of these as hamburger stacking multiple columns and deriving information.
    


- AVG, SUM, MAX, MIN will only work on numbers and dates. They calculate additively. `For Example: MAX above will get the max date from the union of both columns`

- AVG/MIN/MAX do not have comparitive support yet.`For Example: AVG would give average across the values of both columns above for total_pay in the example above`

- COUNT, COUNTD will work on any datatype. COUNT will find the number of times the values are `!= na` in pandas.

- DISTINCT returns an array of the distinct values. It behaves similar to a one level one-to-many.

- Planned configuration for custom calculations, TBD on priorities


<br>

<br>


