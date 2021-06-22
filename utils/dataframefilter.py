
def df_filter(dataframe):
    return dataframe.filter(dataframe['value'].isNull())
