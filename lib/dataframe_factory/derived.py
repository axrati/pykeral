import pandas as pd

def stack_cols(column_names, df):
    dfs = []
    for col in column_names:
        sub_data = pd.DataFrame(df[col])
        sub_data.rename(columns={col:"summarizer"}, inplace=True)
        dfs.append(sub_data)
    return pd.concat(dfs)



def derived_handler(derived_obj, dataframe):
    generic = {}
    supported_operations = ['AVG','SUM','MAX','MIN','COUNT','COUNTD']

    sent_option = derived_obj['operation']
    sent_columns = derived_obj['columns']
    return_key = derived_obj['attribute_name']
    
    if sent_option == "AVG":
        data = stack_cols(sent_columns,dataframe)
        return {return_key:data['summarizer'].mean()}
    
    elif sent_option == "SUM":
        data = stack_cols(sent_columns,dataframe)
        return {return_key:data['summarizer'].sum()}
    
    elif sent_option == "MIN":
        data = stack_cols(sent_columns,dataframe)
        return {return_key:data['summarizer'].min()}

    elif sent_option == "MAX":
        data = stack_cols(sent_columns,dataframe)
        return {return_key:data['summarizer'].max()}

    elif sent_option == "COUNT":
        data = stack_cols(sent_columns,dataframe)
        data = data.dropna()
        return {return_key:len(data['summarizer'])}

    elif sent_option == "COUNTD":
        data = stack_cols(sent_columns,dataframe)
        return {return_key:len(data['summarizer'].unique())}


