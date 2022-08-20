def row_to_dict(dataframe,columns):
    decon_arr = []
    for index, row in dataframe.iterrows():
        data = {}
        for i in columns:
            data[str(i)]=row[i]
        decon_arr.append(data)
    return decon_arr
