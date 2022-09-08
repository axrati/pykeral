from pykeral.main import dfxc
import pandas as pd


df = pd.DataFrame([
    {"id":1234, "name":"alex", "age":142, "gender":"male", "has_kids":True},
    {"id":1234, "name":"alex", "age":142, "gender":"female", "has_kids":True},
    {"id":23453, "name":"joe", "age":122, "gender":"male", "has_kids":False},
    {"id":234523, "name":"ham", "age":12, "gender":"female", "has_kids":True},
    {"id":1234523434, "name":"waw", "age":14, "gender":"female", "has_kids":False},
    {"id":12345234, "name":"evv", "age":16, "gender":"male", "has_kids":True},
    {"id":1453453234, "name":"tee", "age":50, "gender":"male", "has_kids":False},

], columns=['id','name','age', 'gender','has_kids'])

#2) Create Nodes / Rels
dfx = dfxc(df)

