# -*- coding: utf-8 -*-
import pandas as pd
# import os

#%% 
def pim_raw_load(list_of_dfs,path,fill_na_flag=True):
    '''
    A simple function to read PIM data from raw CSVs. Returns a dictionary of Pandas DataFrames.
    
    @param list_of_dfs: a list of DataFrames to add as attributes
    @param path: the absolute filepath to use when reading CSVs
    @param fill_na_flag: boolean operator to define whether to add Pandas' df.fillna('') to DataFrame creation (default == True)
    '''
    files_dict = {'relationships':'alfresco_extract-association_.csv',
        'categories':'alfresco_extract-category_.csv',
        'contributors':'alfresco_extract-contributor_.csv',
        'products':'alfresco_extract-product_.csv',
        'taxonomy':'alfresco_extract-taxonomy_.csv'}
    ret_dict = {}
    if path[-1] != '/':
        path += '/'
    if fill_na_flag == True:
        for df in list_of_dfs:
            ret_dict[df] = pd.read_csv(str(path+files_dict[df]),low_memory=False, header=None, sep='^').fillna('')
    else:
        for df in list_of_dfs:
            ret_dict[df] = pd.read_csv(str(path+files_dict[df]),low_memory=False, header=None, sep='^')
    return ret_dict

#%%
def return_already_exported(list_of_dfs,fill_na_flag):
    '''
    A simple function to read PIM data from CSVs that have already been processed and exported. Returns a dictionary of Pandas DataFrames.
    
    @param list_of_dfs: a list of DataFrames to add as attributes
    @param path: the absolute filepath to use when reading CSVs
    @param fill_na_flag: boolean operator to define whether to add Pandas' df.fillna('') to DataFrame creation (default == True)
    '''
    files_dict = {'categories':'Categories dataset.csv',
        'products':'Product dataset.csv',
        'relationships':'Relationship dataset.csv',
        'taxonomy':'Taxonomy dataset.csv',
        'contributors':'Contributors dataset.csv'}
    read_path = '/Users/nathaniel.hunt/Box Sync/HBP Metadata Group/PIM Data/'
    ret_dict = {}
    if fill_na_flag == True:
        for df in list_of_dfs:
            ret_dict[df] = pd.read_csv(str(read_path+files_dict[df]),low_memory=False).fillna('')
    else:
        for df in list_of_dfs:
            ret_dict[df] = pd.read_csv(str(read_path+files_dict[df]),low_memory=False)
    return ret_dict
            
#%% 
def transform_and_export(df_dict,export=True):
    '''
    The main data-cleanup and processing function of the PIM module. Returns a dictionary of Pandas DataFrames.
    
    @param df_dict: a dictionary of DataFrames from pim_raw_load() or PimData.manual_load()
    @param export: a boolean operator to specify whether to export the cleansed and processed DataFrames (default == True)
    '''
    products = df_dict['products']
    taxonomy = df_dict['taxonomy']
    categories = df_dict['categories']
    relationships = df_dict['relationships']
    contributors = df_dict['contributors']
    # guess at naming columns
    taxonomy.columns = ['Term ID','Term ID 2','Category','Term','Create Date','Last Mod Date']
    categories.columns = ['Category Type ID','Category Type','L1 ID','L1','L2 ID','L2','Last Mod Date']
    relationships.columns = ['Base','Association Type','Target','Sort','Bundle Percentage','NA','Role','Last Mod Date']
    contributors.columns = ['Contributor ID','Contributor ID 2','Entity Type','First Name','Middle Name',
        'Last Name','Prefix','Suffix','Author Comments','HBS Faculty Indicator','Alias Indicator','Authornet ID',
        'HBS Faculty Year Left','Organization Name','Organization Comments','Create Date','Last Mod Date']
    prod_col_extent = len(products.columns)
    prod_cols = ['CoreProd Part Number','Product ID','Availability Part Number','Availability ID','Format',
        'Language','Core Product State','Availability Product State','Title','Abbreviated Title','Product Class',
        'Business Group','Marketing Category','Pre-Abstract','Abstract','Post-Abstract','Marketing Description',
        'Learning Objective Description','Event Begin Year','Event End Year','Company About Description 1',
        'Company About Description 2','Company About Description 3','Company About Description 4',
        'Company About Description 5','Company Employee Count','Person About Description 1','Person About Description 2',
        'Person About Description 3','Person About Description 4','Person About Description 5','Revenue','Course',
        'Course Type','Program Type','Copyright Holder','Right Type 1','Right Type 2','Right Type 3','Right Type 4',
        'Right Value 1','Right Value 2','Right Value 3','Right Value 4','Series 1','Series 2','Series 3',
        'Major Discipline','Major Subject','Demo URL','Enroll Now URL','Fifty Lessons Video Number','Parent Number',
        'Product Type','Source','Authornet Flag','List Price','Publication Date','Distribution Availablity Date',
        'Release Date', 'Status', 'Status Comments', 'Restriction Code', 'Restriction Message',
        'Requirements Description', 'Alternate ID Type 1', 'Alternate ID Type 2', 'Alternate ID Type 3',
        'Alternate ID Type 4', 'Alternate ID Value 1', 'Alternate ID Value 2', 'Alternate ID Value 3',
        'Alternate ID Value 4', 'Operator Message', 'Unit Value 1', 'Unit Value 2', 'Unit Type 1', 'Unit Type 2',
        'Product Change Type', 'Status Date', 'Actual Revision Date', 'Version list', 'Estimated Revision Date',
        'Revision Out Date', 'Revision Requestor', 'Revision Comments', 'Revision Type', 'Withdrawn Obsolete Indicator',
        'Title Override', 'Translator', 'Translation Submit', 'Content Last Modified Date', 'Production Message',
        'External Product URL', 'External Admin URL', 'Add Timestamp', 'Last Updated Time', 'Insert Update Flag',
        'Core Add Timestamp', 'Core Last Updated Timestamp', 'To Be Retired Indicator', 'Suppress Sample Indicator',
        'Content Modified Date Time Aspect', 'CL Eligible', 'HBRG Eligible', 'HE Eligible', 'Single Click Eligible',
        'EBS Product Class', 'Accounting Rule ID', 'Marketing Materials Indicator', 'Conference Indicator',
        'Protagonist First Name 1', 'Protagonist Middle Name 1', 'Protagonist Last Name 1', 'Protagonist Job Title 1',
        'Protagonist Gender 1', 'Protagonist Location 1', 'Protagonist Race 1', 'Protagonist First Name 2',
        'Protagonist Middle Name 2', 'Protagonist Last Name 2', 'Protagonist Job Title 2', 'Protagonist Gender 2',
        'Protagonist Location 2', 'Protagonist Race 2', 'Protagonist First Name 3', 'Protagonist Middle Name 3',
        'Protagonist Last Name 3', 'Protagonist Job Title 3', 'Protagonist Gender 3', 'Protagonist Location 3',
        'Protagonist Race 3', 'Owner Code', 'Copyright Holder Display Name', 'Product Type Description',
        'Format Description', 'Parent Number Description', 'Product Status Description',
        'HE List Price Degree Granting','HE List Price Non Degree Granting', 'Management Screens', 'Status Data',
        'Flash Asset', 'Multi Scenario','Multi User', 'Platform', 'External Resource Path', 'Instructional Videos',
        'Brochure', 'HE Marketing Best Seller', 'Student Preview', 'Instructor Preview', 'Parent Title Override',
        'Subtitle', 'Chapter Number','Free Trial', 'Has Test Bank','Executive Summary','Unit Value 3', 'Unit Value 4','Unit Value 5', 'Unit Type 3', 'Unit Type 4', 'Unit Type 5', 'Student Purchasable']
    try:
        products.columns = prod_cols[:prod_col_extent]
    except:
        print('Renaming Products DF columns failed--there were probably new columns added. Check the CSVs.')
    # drop redundant columns
    contributors = contributors.drop(['Contributor ID 2','Entity Type'],axis=1)
    taxonomy = taxonomy.drop(['Term ID 2'],axis=1)
    relationships = relationships.drop(['NA'],axis=1)
    products = products.drop(['CoreProd Part Number'],axis=1)
        
    # pulling in information from other datasets into products datasets

    # construct series objects
    #%% category Series
    catg_series = pd.Series()
    l2s = categories[categories['L2 ID'] != '']
    l2s.index = l2s['L2 ID'].astype(int).astype(str)
    catg_series = catg_series.append(l2s.apply(lambda x: '^'.join( [x['Category Type'], x['L1'], x['L2']] ), axis=1))

    l1s = categories[(categories['L1 ID'] != '') & (categories['L2 ID'] == '')]
    l1s.index = l1s['L1 ID'].astype(int).astype(str)
    catg_series = catg_series.append(l1s.apply(lambda x: '^'.join( [x['Category Type'], x['L1']] ) + '^', axis=1)).sort_index()

    types = categories[(categories['Category Type ID'].astype(str) != '') & (categories['L1 ID'] == '') & (categories['L2 ID'] == '')]
    types.index = types['Category Type ID'].astype(int).astype(str)
    catg_series = catg_series.append(types.apply(lambda x: x['Category Type'] + '^' + '^', axis=1)).sort_index()
    catg_series.name = 'Category'

    #%% taxo Series
    taxo_tmp = taxonomy.copy()
    taxo_tmp.index = taxo_tmp['Term ID'].astype(str)
    taxo_series = taxo_tmp.apply(lambda x: x['Category'] + '/' + x['Term'],axis=1).sort_index()
    taxo_series = taxo_series[taxo_series.index != '0']
    taxo_series.name = 'Taxonomy Terms'

    #%% contributor Series
    non_empty = contributors[contributors['Last Name'] != ''].copy()
    non_empty.index = non_empty['Contributor ID'].astype(str)
    contr_series = non_empty.apply(lambda x: ' '.join( [str( x['Contributor ID'] ), x['First Name'], x['Middle Name'], x['Last Name']]) if x['Middle Name'] != '' else ' '.join( [str( x['Contributor ID'] ), x['First Name'], x['Last Name'] ]), axis=1 )
    contr_series = contr_series.apply(lambda x: x.replace('  ', ' ')).sort_index()
    contr_series.name = 'Contributors'

    #%%    
    include_list = [
        'Allows Purchase Of',
        'Also Recommended (Cross-Sell)',
        'Also Recommended (Up-Sell)',
        'Also of Interest',
        'Contained By',
        'Contains',
        'Instructed With',
        'Is Sibling Of',
        'Must Be Purchased With',
        'Must be Used With',
        'Recommended for Use With',
        'Related Concept To',
        'Replaces',
        'Repurposed To',
        'Requires',
        'Supplemented By',
        'Supported By',
        'Supports',
        'Teaching Instruction For'
     ]

    #%% relationship Series
    valid_rels = relationships[relationships['Association Type'].isin(include_list)]
    rel_series = valid_rels.groupby('Base').apply(
            lambda x: '^'.join(
                [i + ' ' + j for i,j in zip(x['Target'], x['Association Type'])]
                )  
            )
    rel_series.name = 'Relationships'

    #%%
    gr1 = valid_rels.join(rel_series,on = 'Target',how='left').fillna('')
    gr2 = relationships[(~relationships['Association Type'].isin(include_list)) & (relationships['Association Type'].isin([catg_series.name,taxo_series.name,contr_series.name]))]
    gr3 = relationships[(~relationships.index.isin(gr1.index)) & (~relationships.index.isin(gr2.index))]


    #%%
    for s in [catg_series,taxo_series,contr_series]:
        t = prepare_join(s,s.name)
        gr2 = gr2.merge(t,on=['Target','Association Type'],how='left')
    gr2 = gr2.fillna('')

    #%% joining all series into products dataframe
    products = products.join(rel_series, on = 'Product ID', how='left')
    for s in ['Category','Taxonomy Terms','Contributors']:
        products = products.join(prepare_join_products(gr2,s), on = 'Product ID', how='left')
    
    #%%
    products = products.fillna('')
    products = products.rename(columns={'Taxonomy Terms':'Taxonomy','Category':'Categories'})
    relationships = pd.concat([gr1,gr2,gr3]).fillna('').drop_duplicates()
    #%%
    ret_dict = {}
    ret_dict['products'] = products
    ret_dict['taxonomy'] = taxonomy
    ret_dict['categories'] = categories
    ret_dict['contributors'] = contributors
    ret_dict['relationships'] = relationships
    if export == True:
        export(ret_dict)
    return ret_dict

def export(ret_dict):
    dest_path = '/Users/nathaniel.hunt/Box Sync/HBP Metadata Group/PIM Data/'
    ret_dict['products'].to_csv(dest_path + 'Product dataset.csv',index=False)
    ret_dict['taxonomy'].to_csv(dest_path + 'Taxonomy dataset.csv',index=False)
    ret_dict['categories'].to_csv(dest_path + 'Categories dataset.csv',index=False)
    ret_dict['contributors'].to_csv(dest_path + 'Contributors dataset.csv',index=False)
    ret_dict['relationships'].to_csv(dest_path + 'Relationship dataset.csv',index=False)
#%%
def prepare_join(srs,desc):    
    tmp = pd.DataFrame(data=srs)
    tmp = tmp.reset_index()
    tmp = tmp.rename(columns={tmp.columns[0]: 'Target'})
    tmp['Association Type'] = desc
    return tmp

def prepare_join_products(gr2,col):
    srs = gr2[gr2['Association Type'] == col].groupby('Base').apply(
            lambda x: '^'.join(x[col] )
            )
    srs.name = col
    return srs.sort_index()