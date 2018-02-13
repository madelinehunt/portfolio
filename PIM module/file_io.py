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
    
    It works, but slowly, and eventually I want to rewrite it to be faster and more effecient. 
    
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
        print('Renaming Products DF failed--there were probably new columns added. Check the CSVs.')
    # drop redundant columns
    contributors = contributors.drop(['Contributor ID 2','Entity Type'],axis=1)
    taxonomy = taxonomy.drop(['Term ID 2'],axis=1)
    relationships = relationships.drop(['NA'],axis=1)
    products = products.drop(['CoreProd Part Number'],axis=1)
    
    # he and he_current
    he_products = products[products['Business Group'] == 'Higher Education']
    he_current = he_products[he_products['Status'] == 'C']
    he_current = he_current[he_current['Core Product State'] == 'Approved (All)']
    he_current = he_current[he_current['Availability Product State'] == 'Approved (All)']
        
    # pulling in information from other datasets into products datasets

    # construct dicts
    catg_dict = {}
    for i in [x for x in list(categories['L2 ID']) if x != '']:
        select = categories[categories['L2 ID'] == i]
        catg_dict[str(i).split('.')[0]] = '^'.join([select['Category Type'].iloc[0]] + [select['L1'].iloc[0]] + [select['L2'].iloc[0]])
    for i in [x for x in list(categories['L1 ID']) if x != '' and str(x).split('.')[0] not in catg_dict.keys()]:
        select = categories[categories['L1 ID'] == i]
        catg_dict[str(i).split('.')[0]] = '^'.join([select['Category Type'].iloc[0]] + [select['L1'].iloc[0]]) + '^'
    for i in [x for x in list(categories['Category Type ID']) if x != '' and str(x).split('.')[0] not in catg_dict.keys()]:
        select = categories[categories['Category Type ID'] == i]
        catg_dict[str(i).split('.')[0]] = '^'.join([select['Category Type'].iloc[0]]) + '^' + '^'

    taxo_dict = {}
    for i in list(taxonomy['Term ID']):
        select = taxonomy[taxonomy['Term ID'] == i]
        taxo_dict[str(i)] = select['Category'].iloc[0] + '/' + select['Term'].iloc[0]

    contr_dict = {}
    non_empty = contributors[contributors['Last Name'] != '']
    for i in list(non_empty['Contributor ID']):
        select = non_empty[non_empty['Contributor ID'] == i]
        name_list = []
        name_list.append(str(select['Contributor ID'].iloc[0]))
        for n in [select['First Name'].iloc[0], select['Middle Name'].iloc[0],select['Last Name'].iloc[0]]:
            if n != '':
                name_list.append(n)
        contr_dict[str(i)] = ' '.join(name_list)
            
    #
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

    relationships['Contributors'] = '' 
    relationships['Taxonomy'] = ''
    relationships['Categories'] = ''
    relationships['Relationships'] = ''
    relationships.Contributors = relationships.apply(get_contr, args=(contr_dict,),axis=1)
    relationships.Categories = relationships.Target.apply(get_catg, args=(catg_dict,))
    relationships.Taxonomy = relationships.Target.apply(get_taxo, args=(taxo_dict,))
    relationships.Relationships = relationships.apply(get_rela, args=(include_list,), axis=1)

    #
    rel_dict = {}
    test = relationships.groupby('Base')
    for key, item in test:
        rel_dict[key] = {}
        rel_dict[key]['Taxonomy'] = '^'.join([x for x in list(item['Taxonomy']) if x != ''])
        rel_dict[key]['Contributors'] = '^'.join([x for x in list(item['Contributors']) if x != ''])
        catg_lookups = '^'.join([x for x in list(item['Categories']) if x != ''])
        if len(catg_lookups) > 0:
            if list(catg_lookups)[-2] == '^':
                catg_lookups = '^'.join(catg_lookups.split('^')[:-2])
            elif list(catg_lookups)[-1] == '^':
                catg_lookups = '^'.join(catg_lookups.split('^')[:-1])
        rel_dict[key]['Categories'] = catg_lookups
        rel_dict[key]['Relationships'] = '^'.join([x for x in list(item['Relationships']) if x != ''])

    #
    products['Contributors'] = ''
    products['Taxonomy'] = ''
    products['Categories'] = ''
    products['Relationships'] = ''
    products.Contributors = products['Product ID'].apply(get_rela_contr, args=(rel_dict,))
    products.Categories = products['Product ID'].apply(get_rela_catg, args=(rel_dict,))
    products.Taxonomy = products['Product ID'].apply(get_rela_taxo, args=(rel_dict,))
    products.Relationships = products['Product ID'].apply(get_rela_rela,args=(rel_dict,))
    
    ret_dict = {}
    ret_dict['products'] = products
    ret_dict['taxonomy'] = taxonomy
    ret_dict['categories'] = categories
    ret_dict['contributors'] = contributors
    ret_dict['he_products'] = he_products
    ret_dict['relationships'] = relationships
    if export == True:
        export(ret_dict)
    return ret_dict
    
def get_contr(row,contr_dict):
    if row['Target'] in contr_dict.keys() and row['Association Type'] == 'Contributors':
        if row['Role'] != '':
            return contr_dict[row['Target']] + ' (' + row['Role'] + ')'
        else:
            return contr_dict[row['Target']]
    else:
        return ''

def get_catg(x,catg_dict):
    if x in catg_dict.keys():
        return catg_dict[x]
    else:
        return ''
        
def get_taxo(x,taxo_dict):
    if x in taxo_dict.keys():
        return taxo_dict[x]
    else:
        return ''

def get_rela(row,include_list):
    if row['Association Type'] in include_list:
        return row['Target'] + ' ' + row['Association Type']
    else:
        return ''
def get_rela_contr(x,rel_dict):
    if x in rel_dict.keys():
        return rel_dict[x]['Contributors']
    else:
        return ''
def get_rela_taxo(x,rel_dict):
    if x in rel_dict.keys():
        return rel_dict[x]['Taxonomy']
    else:
        return ''
def get_rela_catg(x,rel_dict):
    if x in rel_dict.keys():
        return rel_dict[x]['Categories']
    else:
        return ''
def get_rela_rela(x,rel_dict):
    if x in rel_dict.keys():
        return rel_dict[x]['Relationships']
    else:
        return ''

def export(ret_dict):
    products = ret_dict['products']
    taxonomy = ret_dict['taxonomy']
    categories = ret_dict['categories']
    relationships = ret_dict['relationships']
    contributors = ret_dict['contributors']
    relationships = ret_dict['relationships']
    dest_path = '/Users/nathaniel.hunt/Box Sync/HBP Metadata Group/PIM Data/'
    products.to_csv(dest_path + 'Product dataset.csv',index=False)
    relationships.to_csv(dest_path + 'Relationship dataset.csv',index=False)
    categories.to_csv(dest_path + 'Categories dataset.csv',index=False)
    taxonomy.to_csv(dest_path + 'Taxonomy dataset.csv',index=False)
    contributors.to_csv(dest_path + 'Contributors dataset.csv',index=False)
    