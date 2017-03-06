"""
Because parameters have been transformed into functions in distrib_data.py,
this script imports and runs all functions in distrib_data to extract their data, 
then outputs results in separate CSV files of parameters.
"""
#%%
import os
import pandas as pd 
import distrib_data as dist
from distrib_data import *

distrib_list = dir(dist)[8:]
distrib_list_data = []
#run all defined functions and extract the data
for func in distrib_list:
    temp = globals()[func]()
    temp['name'] = func
    distrib_list_data.append(temp)

#find the most complete list of copyright holders
temp_list = []
for i in distrib_list_data:
    if 'CopyrightHolder' in i:
        temp_list.append(i['CopyrightHolder'])
max_copyright_holder = []    
for item in temp_list:
    for i in item:
        if i not in max_copyright_holder and i != 'NULL':
            max_copyright_holder.append(i)

# finds the most complete list of product types
temp_list = []
for i in distrib_list_data:
    if 'ProductTypeCodeIN' in i:
        temp_list.append(i['ProductTypeCodeIN'])
    elif 'product_type_code' in i:
        temp_list.append(i['product_type_code'])
max_product_type = []
for item in temp_list:
    for i in item:
        if i not in max_product_type:
            max_product_type.append(i)
max_copyright_holder.sort(); max_product_type.sort()

#creates a dataframe for copyright holders
cpy_df = pd.DataFrame(columns=max_copyright_holder, index=distrib_list); cpy_df = cpy_df.fillna('')
for series in distrib_list_data:
    if 'CopyrightHolder' in series:
       for i in series['CopyrightHolder']:
           if i in cpy_df.columns:
               cpy_df.loc[series['name']][i] = 'X'
               
# creates a data frame for product types
pt_df = pd.DataFrame(columns=max_product_type, index=distrib_list); pt_df = pt_df.fillna('')
for series in distrib_list_data:
    if 'ProductTypeCodeIN' in series:
       for i in series['ProductTypeCodeIN']:
           if i in pt_df.columns:
               pt_df.loc[series['name']][i] = 'X'
    elif 'product_type_code' in series:
       for i in series['product_type_code']:
           if i in pt_df.columns:
               pt_df.loc[series['name']][i] = 'X'
               
# creates a data frame for tns and protected product types
tn_df = pd.DataFrame(columns=['get TN','get unsecured files'], index=distrib_list); tn_df = tn_df.fillna('')
for series in distrib_list_data:
    if 'DocumentType' in series:
        if '*t2.pdf' in series['DocumentType'][0]:
           tn_df.loc[series['name']]['get TN'] = 'X'
        elif '*p2.pdf' or '*f2.pdf' in series['DocumentType'][0]:
          tn_df.loc[series['name']]['get unsecured files'] = 'X'

# creates a data frame for languages
lang_df = pd.DataFrame(columns=['FRE', 'SPA', 'POR', 'JPN' ], index=distrib_list); lang_df = lang_df.fillna('')
for series in distrib_list_data:
    if 'Language' in series:
       for i in series['Language']:
           if i in lang_df.columns:
               lang_df.loc[series['name']][i] = 'X'

# creates a data frame for file types
file_df = pd.DataFrame(columns=['C2','F2','P2',	'PPT','PPTX','S2','T2',	'W2','XLS','XLSX'], index=distrib_list); file_df = file_df.fillna('')
for series in distrib_list_data:
    if 'DocumentType' in series:
        if 'XLS' or 'XLSX' in series['DocumentType']:
            file_df.loc[series['name']]['C2'] = 'X'
file_dict = dict(zip(['F2','PPT','PPTX','S2','T2','P2','W2','XLS','XLSX'],['*f2.pdf', 'PPT', 'PPTX', '*s2.pdf', '*t2.pdf', '*p2.pdf', '*w2.pdf', 'XLS', 'XLSX']))
def file_fillout(file_dict, df, distrib_list_data):
    """
    Used to create a data frame of results for filetypes
    """
    for series in distrib_list_data:
        if 'DocumentType' in series:
            for name, string in file_dict.items():
                if string in series['DocumentType'][0]:
                    df.loc[series['name']][name] = 'X'
    return df
file_df = file_fillout(file_dict, file_df, distrib_list_data)

#%% file output
# creates results directory if it doesn't already exist
if 'results' not in os.listdir(os.getcwd()):
    os.system('mkdir results')

#outputs all results CSVs
cpy_df.to_csv(path_or_buf=str('results/' + 'copyright_holders.csv'))
file_df.to_csv(path_or_buf=str('results/' + 'file_types.csv'))
lang_df.to_csv(path_or_buf=str('results/' + 'languages.csv'))
tn_df.to_csv(path_or_buf=str('results/' + 'teaching_notes.csv'))
pt_df.to_csv(path_or_buf=str('results/' + 'product_types.csv'))
#%%





