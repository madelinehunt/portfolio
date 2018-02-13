# -*- coding: utf-8 -*-
'''
This is a short script written to take in data from HBP's products data set, 
and to output a CSV containing only the most recent Actual Revision Dates
for ingestion by PIM's Bulk Edit feature
'''

from datetime import datetime
import pandas as pd #this script requires the Pandas data analysis library.

# Reads from PIM product data extract. 
# This path will need to be changed depending on who is running this script.
products = pd.read_csv('/Users/nathaniel.hunt/Box Sync/HBP Metadata Group/PIM Data/Product dataset.csv', low_memory=False) 

# Selects for all products that are not Core Curriculums (Product Type 164)
non_ccs = products[
        (products['Product Type'] != 164) &
        (products['Actual Revision Date'].str.contains(','))
        ] 

# Begins to format our data frame based on the columns that Bulk Edit will need to ingest 
# (including renaming the columns to match Bulk Edit requirements).
pruned_non_ccs = non_ccs[['Availability ID','Actual Revision Date']].rename(columns={'Availability ID': 'product/availability', 'Actual Revision Date': 'ActualRevisionDate'})

# Defines a function to return only the latest Actual Revision Date, in the format that Bulk Edit needs.
def acrev_stripper(row):
    acrev_list = str(row['ActualRevisionDate']).split(',')
    if acrev_list[-1].strip(']').strip('[').strip() == '':
        ret = acrev_list[-2].strip(']').strip('[').strip()
    else:
        ret = acrev_list[-1].strip(']').strip('[').strip()
    ret = str(datetime.strptime(str(ret),"%m-%d-%Y").isoformat())+'.000-05:00'
    return ret
pruned_non_ccs['ActualRevisionDate'] = pruned_non_ccs.apply(acrev_stripper,axis=1)

# Exports Bulk Edit CSV. 
pruned_non_ccs.to_csv('/Users/nathaniel.hunt/Desktop/Restore single actual revision dates.csv',index=False)

# Removes products that don't exist in PIM QA and exports a Bulk Edit CSV specifically for testing in QA.
del_qa_list = ['308047-EPB-ENG','308047-XML-ENG', 
               '618007-HCC-ENG','711053-EPB-ENG','711053-XML-ENG','712500-HCC-ENG',
               '718034-EPB-ENG','718034-HCC-ENG','718034-PDF-ENG','810141-EPB-ENG',
               '810141-XML-ENG','818031-EPB-ENG','818031-HCC-ENG','818031-PDF-ENG',
               '818031-XML-ENG','818064-EPB-ENG','818064-HCB-ENG','818064-PDF-ENG',
               '818064-XML-ENG']
pruned_for_qa = pruned_non_ccs[pruned_non_ccs['product/availability'].isin(del_qa_list) == False]
pruned_for_qa.to_csv('/Users/nathaniel.hunt/Desktop/Restore single actual revision dates--QA test--.csv',index=False)


