# -*- coding: utf-8 -*-
import os
import datetime

def old_or_missing():
    dest_path = '/Users/nathaniel.hunt/Box Sync/HBP Metadata Group/PIM Data'
    origin_path = '/Users/nathaniel.hunt/Box Sync/In progreſs/PIM Extracts/Current extract'
    etl_file_check = ['Categories dataset.csv',
         'Product dataset.csv',
         'Relationship dataset.csv',
         'Taxonomy dataset.csv',
         'Contributors dataset.csv']
    extract_file_check = ['alfresco_extract-association_.csv',
     'alfresco_extract-category_.csv',
     'alfresco_extract-contributor_.csv',
     'alfresco_extract-product_.csv',
     'alfresco_extract-taxonomy_.csv']

    are_files_all_there = [x in os.listdir(dest_path) for x in etl_file_check]

    if False in are_files_all_there:
        return True
    else:
        from platform import system
        if system() == 'Darwin':
            etl_files = [x for x in os.listdir(dest_path) if '.DS' not in x and x in etl_file_check]
            etl_files = [dest_path + '/' + x for x in etl_files]
            etl_times = [os.stat(x).st_birthtime for x in etl_files]
            etl_times = [datetime.datetime.utcfromtimestamp(x) for x in etl_times]
            oldest_etl_export = min(etl_times)
            
            extract_files = [x for x in os.listdir(origin_path) if '.DS' not in x and x in extract_file_check]
            extract_files = [origin_path + '/' + x for x in extract_files]
            extract_times = [os.stat(x).st_birthtime for x in extract_files]
            extract_times = [datetime.datetime.utcfromtimestamp(x) for x in extract_times]
            oldest_extract_export = min(etl_times)
            
        else:
            etl_files = [dest_path + '/' + x for x in os.listdir(dest_path) if '.DS' not in x]
            etl_times = [os.path.getctime(x) for x in etl_files]
            etl_times = [datetime.datetime.utcfromtimestamp(x) for x in etl_times]
            oldest_etl_export = min(etl_times)
            
            extract_files = [origin_path + '/' + x for x in os.listdir(origin_path) if '.DS' not in x]
            extract_times = [os.path.getctime(x) for x in extract_files]
            extract_times = [datetime.datetime.utcfromtimestamp(x) for x in extract_times]
            oldest_extract_export = min(etl_times)
        are_etl_files_older_than_pim_extract_files = oldest_etl_export < oldest_extract_export
        if are_etl_files_older_than_pim_extract_files == True:
            return True
        else:
            return False
            