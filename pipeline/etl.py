# -*- coding: utf-8 -*-
from libs import *


'''Set a seed for reproducibility'''
seed = 1

'''Set path where all data files are located'''

'''Create load data function to easy import all necessary'''
def LoadJSONData(path,file):
    """Load Data from defined directory.
    Output is initial data dataframe."""
    data = [json.loads(line) for line in open(path + file, 'r')]
    data = pd.json_normalize(data)
    print ("Data is loaded!")
    print ("Data: ",data.shape[0],"observations, and ",data.shape[1],"features")
    #
    return data

'''Create function for providing summary statistics in a table'''
def resumetable(df):
    print(f"Dataset Shape: {df.shape}")
    summary = pd.DataFrame(df.dtypes,columns=['dtypes'])
    summary = summary.reset_index()
    summary['Name'] = summary['index']
    summary = summary[['Name','dtypes']]
    summary['Missing'] = df.isnull().sum().values
    summary['Missing Percentage'] = df.isnull().sum().values/len(df)
    summary['Uniques'] = df.nunique().values
    return summary

'''Create function for easy aggregations at any level'''
def AggCol(df,group_by,col,agg):
       "input is df, groupbycol(s), desired aggregation. Output should be a column"
       col = df.groupby(group_by)[col].transform(agg)
       return col

'''Fix all columns datatypes to correct ones'''
def FixDataTypes(df):
    #convert date to datetime format
    df['date'] = pd.to_datetime(df['date'],infer_datetime_format=True)
    # convert metric_offset and events variables to int
    int_cols = ['metric_offset','events']
    for col in int_cols: 
        df[col] = df[col].astype(int)
    return df

'''Create new column which will be binary: Original/Variation'''
def VariationBinary(df):
    #binary event original or variation
    df['variation_binary'] = np.where((df['variation_name'] != 'Original'), 'Variation' , 'Original') 
    return df

'''Create date features'''
def DateFeatures(df):
    df = df.copy()
     # date features
    df['date']= pd.to_datetime(df['date'])
    df['date:day'] = df['date'].dt.day
    df['date:day_of_week'] = df['date'].dt.dayofweek
    df['date:is_weekend'] = np.where(df['date:day_of_week'].isin([5,6]), 1,0)
    df['date:month'] = df['date'].dt.month
    df['date:day_of_year'] = df['date'].dt.dayofyear
    return df

'''Convert categorical variables to less categories'''
def CatLandingPage(df):
    df = df.copy()
    #binary event original or variation
    df['landingPage'] = np.where(( (df['landingPage'] == 'home') | (df['landingPage'] == 'shop_details') | (df['landingPage'] == 'shop_list') ), df['landingPage'], 'other' ) 
    return df

'''Convert categorical variables to less categories'''
def CatappBrowser(df):
    df = df.copy()
    #binary event original or variation
    df['appBrowser'] = np.where(( (df['appBrowser'] == 'Chrome') | (df['appBrowser'] == 'Firefox') ), df['appBrowser'], 'other' ) 
    return df

'''Convert categorical variables to less categories'''
def Catmedium(df):
    df = df.copy()
    #binary event original or variation
    df['medium'] = np.where(( (df['medium'] == 'organic') | (df['medium'] == 'cpc') | (df['medium'] == "(none)")), df['medium'], 'other' ) 
    return df

'''Convert categorical variables to less categories'''
def Catsource(df):
    df = df.copy()
    #binary event original or variation
    df['source'] = np.where(( (df['source'] == 'google') | (df['source'] == "(direct)")), df['source'], 'other' ) 
    return df

'''Put data pipeline tasks in order for automation purposes'''
def DataPipeline(df):
    df = df.copy()
     # Pipeline
    df = FixDataTypes(df)
    df =  VariationBinary(df)
    df =  DateFeatures(df)
    df = CatLandingPage(df)
    df = CatappBrowser(df)
    df = Catmedium(df)
    df = Catsource(df)
    return df
