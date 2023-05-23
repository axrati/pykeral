import pandas as pd

def stack_cols(column_names, df):
    dfs = []
    for col in column_names:
        sub_data = pd.DataFrame(df[col])
        sub_data.rename(columns={col:"summarizer"}, inplace=True)
        dfs.append(sub_data)
    return pd.concat(dfs)



def derived_handler(derived_obj, dataframe):
    """
    This function takes in columns and a dataframe of node/rel sub data to derive further data.
    Due to the nature of how pandas treats nulls (ie: sum() on a column of NA's is 0), there is discretionary handling per operation.
    """

    # supported_operations = ['AVG','SUM','MAX','MIN','COUNT','COUNTD', 'DISTINCT'] 
    
    sent_option = derived_obj['operation']
    sent_columns = derived_obj['columns']
    return_key = derived_obj['attribute_name']
    
    
    if sent_option == "AVG":
        data = stack_cols(sent_columns,dataframe)
        result = data['summarizer'].mean()
        if pd.isna(result):
            return None
        else:
            return {return_key:result}
    
    elif sent_option == "SUM":
        data = stack_cols(sent_columns,dataframe)
        data = data.dropna()
        if len(data)==0:
            return None
        else:
            return {return_key:data['summarizer'].sum()}
    
    elif sent_option == "MIN":
        data = stack_cols(sent_columns,dataframe)
        result = data['summarizer'].min()
        if pd.isna(result):
            return None
        else:
            return {return_key:result}

    elif sent_option == "MAX":
        data = stack_cols(sent_columns,dataframe)
        result = data['summarizer'].max()
        if pd.isna(result):
            return None
        else:
            return {return_key:result}

    elif sent_option == "COUNT":
        data = stack_cols(sent_columns,dataframe)
        data = data.dropna()
        return {return_key:len(data['summarizer'])}

    elif sent_option == "COUNTD":
        data = stack_cols(sent_columns,dataframe)
        data = data.dropna()
        return {return_key:len(data['summarizer'].unique())}

    elif sent_option == "DISTINCT":
        data = stack_cols(sent_columns,dataframe)
        data = data.dropna()
        return {return_key:list(data['summarizer'].unique())}
    
    else:
        raise Exception("Unexpected error in the derived_handler function. Traceback to lib/dataframe_factory/derived.py. \n")


