# -*- coding: utf-8 -*-
'''
'''

#%% add required libraries
import pandas as pd
from pim.file_io import pim_raw_load, return_already_exported, transform_and_export
from pim.file_check import old_or_missing
#%%

class PimData(object):
    '''
    A test of an OOP approach to this script
    '''
    
    def __init__(self,list_of_dfs=['products','taxonomy','relationships','contributors','categories'],fill_na_flag=True,bypass_check=False):
        
        # performs validation on parameters passed to this function
        if type(list_of_dfs) == str:
            list_of_dfs = [list_of_dfs]
        assert type(list_of_dfs) == list, 'TypeError: etl was not passed a list (or a string to be cast to single-element list) as an argument for list_of_dfs'
        list_of_dfs = list(set(list_of_dfs)) # removes duplicate values from list_of_dfs, in case there are duplicates
        approved_dfs = ['products','taxonomy','relationships','contributors','categories']
        check_list = [x for x in list_of_dfs if x not in approved_dfs]
        assert len(check_list) == 0, 'Error: List of DFs includes items that are not valid DFs: {}'.format(check_list)
        assert type(fill_na_flag) == bool, 'TypeError: etl was not passed a boolean as an argument for fill_na_flag'
        assert type(bypass_check) == bool, 'TypeError: etl was not passed a boolean as an argument for bypass_check'
        
        # check age of files against the files in the 'Current extract' directory, then either loads or performs ETL
        origin_path = '/Users/nathaniel.hunt/Box Sync/In progreſs/PIM Extracts/Current extract/'
        if old_or_missing() == True and bypass_check != False:
            df_dict = pim_raw_load(approved_dfs,origin_path,fill_na_flag=fill_na_flag)
            ret_dict = transform_and_export(df_dict)
            ret_dict = {i:j for i,j in ret_dict.items() if i in list_of_dfs}
        else:
            ret_dict = return_already_exported(list_of_dfs,fill_na_flag)
        
        if 'products' in list_of_dfs:
            self.products = ret_dict['products']
            self.he_standard = self.products[
                (self.products['Business Group'] == 'Higher Education') &
                (self.products['Status'] == 'C') &
                (self.products['Format'] == 'HCB') &
                (self.products['Product Type'] == 130) & 
                (self.products['Restriction Code'] == '99A')
            ]
            self.hbp_current = self.products[
                (self.products['Status'] == 'C') &
                (self.products['Restriction Code'].isin(['99A','75A']))
            ]
        for k in ret_dict.keys():
            if k == 'products':
                self.products = ret_dict[k]
            elif k == 'taxonomy':
                self.taxonomy = ret_dict[k]
            elif k == 'relationships':
                self.relationships = ret_dict[k]
            elif k == 'categories':
                self.categories = ret_dict[k]
            elif k == 'contributors':
                self.contributors = ret_dict[k]
    
    def add(self,list_of_dfs,fill_na_flag=True):
        if type(list_of_dfs) == str:
            list_of_dfs = [list_of_dfs]
        else:
            list_of_dfs = list_of_dfs
        assert type(list_of_dfs) == list, 'TypeError: etl was not passed a list (or a string to be cast to single-element list) as an argument for list_of_dfs'
        list_of_dfs = list(set(list_of_dfs)) # removes duplicate values from list_of_dfs, in case there are duplicates
        approved_dfs = ['products','taxonomy','relationships','contributors','categories']
        check_list = [x for x in list_of_dfs if x not in approved_dfs]
        assert len(check_list) == 0, 'Error: List of DFs includes items that are not valid DFs: {}'.format(check_list)
        assert type(fill_na_flag) == bool, 'TypeError: etl was not passed a boolean as an argument for fill_na_flag'
        
        ret_dict = return_already_exported(list_of_dfs,fill_na_flag)
        
        if 'products' in list_of_dfs:
            products = ret_dict['products']
            self.he_standard = self.products[
                (self.products['Business Group'] == 'Higher Education') &
                (self.products['Status'] == 'C') &
                (self.products['Format'] == 'HCB') &
                (self.products['Product Type'] == 130) & 
                (self.products['Restriction Code'] == '99A')
            ]
            self.hbp_current = self.products[
                (self.products['Status'] == 'C') &
                (self.products['Restriction Code'].isin(['99A','75A']))
            ]
        
        for k in ret_dict.keys():
            if k == 'products':
                self.products = ret_dict[k]
            elif k == 'taxonomy':
                self.taxonomy = ret_dict[k]
            elif k == 'relationships':
                self.relationships = ret_dict[k]
            elif k == 'categories':
                self.categories = ret_dict[k]
            elif k == 'contributors':
                self.contributors = ret_dict[k]
    
    def manual_load(self,directory,list_of_dfs=['products','taxonomy','relationships','contributors','categories'],fill_na_flag=True,append=False,skip_processing=False):
        if type(list_of_dfs) == str:
            list_of_dfs = [list_of_dfs]
        else:
            list_of_dfs = list_of_dfs
        assert type(list_of_dfs) == list, 'TypeError: etl was not passed a list (or a string to be cast to single-element list) as an argument for list_of_dfs'
        list_of_dfs = list(set(list_of_dfs)) # removes duplicate values from list_of_dfs, in case there are duplicates
        approved_dfs = ['products','taxonomy','relationships','contributors','categories']
        check_list = [x for x in list_of_dfs if x not in approved_dfs]
        assert len(check_list) == 0, 'Error: List of DFs includes items that are not valid DFs: {}'.format(check_list)
        assert type(fill_na_flag) == bool, 'TypeError: etl was not passed a boolean as an argument for fill_na_flag'
        assert type(append) == bool, 'TypeError: etl was not passed a boolean as an argument for append'
        assert type(append) == bool, 'TypeError: etl was not passed a boolean as an argument for skip_processing'
        
        ret_dict = pim_raw_load(list_of_dfs,directory,fill_na_flag)
        if skip_processing != True:
            ret_dict = transform_and_export(ret_dict)
        
        if append == False:
            for k in ret_dict.keys():
                if k == 'products':
                    self.products = ret_dict[k]
                elif k == 'taxonomy':
                    self.taxonomy = ret_dict[k]
                elif k == 'relationships':
                    self.relationships = ret_dict[k]
                elif k == 'categories':
                    self.categories = ret_dict[k]
                elif k == 'contributors':
                    self.contributors = ret_dict[k]
        else:
            self.manually_loaded = ret_dict


#%%
class superdf(pd.DataFrame):
        
    def sql_dump(self,db_name):
        remove_spaces = [re.sub(' ','',x) for x in self.columns]
        df = self.copy()
        df.columns = remove_spaces
        import sqlite3
        conn = sqlite3.connect('/Users/nathaniel.hunt/Box Sync/Data/pimData.db')
        df.to_sql(db_name,conn,index=False,if_exists='replace')
    
    def essential_columns(self,add_extras=[]):
        
        if type(add_extras) == str:
            add_extras = [add_extras]
        assert type(add_extras) == list, 'TypeError: essential_columns was not passed a list (or a string to be cast to single-element list) as an argument for add_extras'
        add_extras = list(set(add_extras))
        check_list = [x for x in add_extras if x not in list(self.columns)]
        assert len(check_list) == 0, 'Error: List of columns includes items that are not valid columns: {}'.format(check_list)
        
        df = self.copy()
        essentials = ['Product ID','Availability ID','Restriction Code','Status','Product Type','Contributors','Taxonomy','Categories','Relationships']
        essentials += add_extras
        df = [essentials]
        return df



#class PimData(object):
#    def __init__(self):
#        self.products = pd.DataFrame()
#        self.taxonomy = pd.DataFrame()
#        self.relationships = pd.DataFrame()
#        self.categories = pd.DataFrame()
#        self.contributors = pd.DataFrame()
#        self.he_standard = pd.DataFrame()
#        self.hbp_current = pd.DataFrame()
#    
#    def etl(self,dstring):
#        '''
#        A function to perform ETL on the data extracts from PIM
#        @param list_of_dfs: a list of DFs to perform ETL on and return (or a string to be cast to single-element list)
#        @param na_flag: boolean operator to specify whether to use pandas' fillna() function on DFs to return
#        '''
#        if dstring == 'products':
#            self.products = pd.read_csv('/Users/nathaniel.hunt/Box Sync/HBP Metadata Group/PIM Data/Product dataset.csv',low_memory=False)
#        self.taxonomy = pd.DataFrame()
#        self.relationships = pd.DataFrame()
#        self.categories = pd.DataFrame()
#        self.contributors = pd.DataFrame()
#        self.he_standard = pd.DataFrame()
#        self.hbp_current = pd.DataFrame()
#        return self