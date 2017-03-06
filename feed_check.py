#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analyzes a directory on an FTP server to determine if there are missing files,
as compared to a metadata (.tab) file which lists all the files that should be
in the directory.
"""
import pandas as pd

#%% analysis file
analysis_file = input('Enter the name of the tab file to analyze: ')
#supplies absolute filepath for the file entered as analysis_file
analysis_file = '/Users/nathaniel.hunt/Desktop/ftp/' + analysis_file
tabble = pd.read_table(analysis_file, header=None)
tabble = tabble.fillna('n/a')
for index, value in tabble.iterrows():
    tabble.loc[index][0] = tabble.loc[index][0][15:]
tabble.sort_values(by=0)
#%% ls file
ls_file = input('Enter the name of the LS file to analyze: ')
#supplies absolute filepath for the file entered as ls_file (which is the output of an ls command on an FTP server)
ls_file = '/Users/nathaniel.hunt/Desktop/ftp/' + ls_file
file_list = []
ls = open(ls_file, 'r+')
for line in ls.readlines():
    # extracts product file names from ls output
    if len(line) == 92:
        file_list.append(line[-13:])
    elif len(line) == 91:
        file_list.append(line[-12:])
    elif len(line) == 90:
        file_list.append(line[-11:])
ls.close()
new_list = []
for i in file_list:
    # extracts product ID numbers from their file names
    i = i.rstrip('\n')
    i = i.replace('t2.pdf', '')
    i = i.replace('p2.pdf', '')
    new_list.append(i)

#%% compare .tab file with the contents of the directory
missing_list = []
for index, value in tabble.iterrows():
    if value[0] not in new_list:
        missing_list.append(value[0])
        
missing_df = pd.DataFrame(columns=tabble.columns)
for i in missing_list:
    missing_df = missing_df.append(tabble[tabble[0]==i]) 