"""
Script to analyze HBP product metadata. Data is supplied by exports from Product Information Management software,
which end up as CSVs in the '/input' directory. From there, this script outputs CSVs which can be read by Tableau
for a data pipeline to reporting.
"""
#%% libraries and initialization
import pandas as pd
import numpy as np
#import math
#import matplotlib.pyplot as ply
import os
import sys
import datetime
import calendar
now = datetime.datetime.now()

#%% file input
# set current working directory
cwd = os.getcwd()
sys_path = os.path.join(sys.path[0], sys.argv[0])
path=os.path.split(sys_path)[0]

# imports all CSVs found in the input folder
files_input_dir = path + '/input/'
allfiles=[]
for file in os.listdir(files_input_dir):
    allfiles.append(os.path.join(files_input_dir,file))
temp_list = []
for file in allfiles[1:]:
    temp_df = pd.read_csv(file, header=0, low_memory=False)
    temp_list.append(temp_df)
main_df = pd.concat(temp_list, ignore_index=True); main_df = main_df.fillna('n/a')
del temp_list, temp_df, allfiles

# imports all CSVs found in the avail folder
files_input_dir = path + '/avail/'
allfiles=[]
for file in os.listdir(files_input_dir):
    allfiles.append(os.path.join(files_input_dir,file))
temp_list = []
for file in allfiles[1:]:
    temp_df = pd.read_csv(file, header=0, low_memory=False)
    temp_list.append(temp_df)
avail_df = pd.concat(temp_list, ignore_index=True)
del temp_list, temp_df

# create HE data frame
he_frame = main_df[main_df['BusinessGroup'] == 'Higher Education']; he_frame = he_frame.fillna('n/a')

# defines a partners copyright holder list
partners_list = list(he_frame.CopyrightHolder.unique())
del partners_list[55]
harvard_list = ['HBS', 'Harvard Advanced Leadership Initiative', 'Non-HBS',
    'Harvard Business School Publishing - Higher Ed', 'Harvard University',
    'HBS and Stanford U', 'Marketing - HBSP', 'Harvard Business School Publishing',
    'Harvard Medical School', 'Harvard Kennedy School', 'HBS and Harvard Kennedy School',
    'Harvard T. H. Chan School of Public Health', 'Program on Negotiation at Harvard Law School (PON)',
    'The Crimson Group', 'HBS Press', 'Fifty Lessons Ltd.', 'HBS Streaming', 'Harvard Business School Publishing - HBR',
    'Harvard Business School Publishing - Corporate Learning']
for i in harvard_list:
    if str(i) in partners_list:
        partners_list.remove(i)
partners_for_reporting_list = ['WDI Publishing at the University of Michigan', 'Kellogg School of Management, Northwestern Univ.',
'UC Berkeley - Haas School of Business', 'University of Virginia Darden School Foundation', 'Business Enterprise Trust', 'Harvard Kennedy School',
 'Babson College', 'North American Case Research Association (NACRA)', 'MIT Sloan Management Review',  'Thunderbird School of Global Management',
  'HEC Montreal Centre for Case Studies',  'Indian Institute of Management-Bangalore', 'University of Hong Kong', 'Stanford University',
   'Business Horizons/Indiana Univ.', 'INSEAD']
   
# defines the canon of Major Discipline and its counterpart Major Subject
major_disc_list = ['Accounting', 'Business & Government Relations','Business Ethics','Economics','Entrepreneurship','Finance',
'General Management','Human Resource Management','Information Technology','International Business','Marketing','Negotiation',
'Operations Management','Organizational Behavior','Sales','Service Management','Social Enterprise','Strategy','Teaching & the Case Method']
major_sub_list = ['Finance & Accounting','Global Business','Leadership & Managing People','Finance & Accounting',
'Innovation & Entrepreneurship','Finance & Accounting','Leadership & Managing People','Organizational Development',
'Technology & Operations','Global Business','Sales & Marketing','Sales & Marketing','Technology & Operations','Organizational Development',
'Sales & Marketing','Strategy & Execution','Innovation & Entrepreneurship','Strategy & Execution','Communication']
def create_dict(disc, sub):
    return {'Major Discipline': disc, 'Counterpart Major Subject': sub}
disc_dict = [create_dict(i, major_sub_list[major_disc_list.index(i)]) for i in major_disc_list]
major_canon = pd.DataFrame(columns=['Major Discipline', 'Counterpart Major Subject'], data=disc_dict)

#%% functions
# define two functions to separate HBS reporting data by month and by fiscal year
def hbs_created_month(month, year, dframe):
    """
    Returns a list of the amount of HBS cases created per month
    """
    month_list = list(range(1,13))
    if month <= 0:
        month = month_list[month-1]
        year -= 1
    formatted_month = datetime.date(year, month, 1).strftime('%m')
    current_month_created = dframe[dframe['created'].str.contains('{}....{}'.format(formatted_month, year))==True]
    current_month_hbs = current_month_created[current_month_created['CopyrightHolder']=='HBS']
    hbs_case_count = len(current_month_hbs[current_month_hbs['ProductTypeCode']==130].index)
    hbs_supp_count = len(current_month_hbs[current_month_hbs['ProductTypeCode']==132].index)
    hbs_tn_count = len(current_month_hbs[current_month_hbs['ProductTypeCode']==131].index)
    hbs_indnote_count = len(current_month_hbs[current_month_hbs['ProductTypeCode']==134].index)
    return [calendar.month_name[month], hbs_case_count, hbs_supp_count, hbs_tn_count, hbs_indnote_count]

def hbs_created_year(year, dframe):
    """
    Returns a list of the amount of HBS cases created per *fiscal* year
    """
    fiscal_year_first_half = year-1
    fy_1st_months = list(range(7, 13))
    fiscal_year_second_half = year
    fy_2nd_months = list(range(1, 7))
    df_list = []
    for month in fy_1st_months:
        fy_month = datetime.date(fiscal_year_first_half, month, 1).strftime('%m')
        fy_df = dframe[dframe['created'].str.contains('{}....{}'.format(fy_month, fiscal_year_first_half))==True]
        df_list.append(fy_df)
    for month in fy_2nd_months:
        fy_month = datetime.date(fiscal_year_second_half, month, 1).strftime('%m')
        fy_df = dframe[dframe['created'].str.contains('{}....{}'.format(fy_month, fiscal_year_second_half))==True]
        df_list.append(fy_df)
    fy = pd.concat(df_list)
    fy_year_hbs = fy[fy['CopyrightHolder']=='HBS']
    hbs_case_count = len(fy_year_hbs[fy_year_hbs['ProductTypeCode']==130].index)
    hbs_supp_count = len(fy_year_hbs[fy_year_hbs['ProductTypeCode']==132].index)
    hbs_tn_count = len(fy_year_hbs[fy_year_hbs['ProductTypeCode']==131].index)
    hbs_indnote_count = len(fy_year_hbs[fy_year_hbs['ProductTypeCode']==134].index)
    return [year, hbs_case_count, hbs_supp_count, hbs_tn_count, hbs_indnote_count]

#counterparts to the above functions, but for non-HBS
def partner_case_count(month, year, dframe):
    """
    Returns a list of the amount of non-HBS cases created per month
    """
    month_list = list(range(1,13))
    if month <= 0:
        month = month_list[month-1]
        year -= 1
    formatted_month = datetime.date(year, month, 1).strftime('%m')
    current_month_created = dframe[dframe['created'].str.contains('{}....{}'.format(formatted_month, year))==True]
    current_month_non_hbs = current_month_created[current_month_created['CopyrightHolder']!='HBS']
    current_month_cases = current_month_non_hbs[current_month_non_hbs['ProductTypeCode']==130]
    num_of_cases = len(current_month_cases.name)
    return_dict = {}
    return_dict[calendar.month_name[month]] = num_of_cases
    return return_dict

def partner_all_products_count(month, year, dframe):
    """
    Returns a list of the amount of non-HBS products created per month
    """
    month_list = list(range(1,13))
    if month <= 0:
        month = month_list[month-1]
        year -= 1
    formatted_month = datetime.date(year, month, 1).strftime('%m')
    current_month_created = dframe[dframe['created'].str.contains('{}....{}'.format(formatted_month, year))==True]
    partner_count_df = pd.DataFrame(columns=['Month','Partner','Count for all Products'])
    for partner in partners_for_reporting_list:
        partner_content = current_month_created[current_month_created['CopyrightHolder']==str(partner)]
        add_to_df = pd.DataFrame([[calendar.month_name[month], partner, len(partner_content.name)]],columns=['Month','Partner','Count for all Products'])
        partner_count_df = partner_count_df.append(add_to_df)
    # add coding to account for GHD (Global Health Delivery) from HMS, identifiable by product number starting with "GHD"
    hms = current_month_created[current_month_created.CopyrightHolder=='Harvard Medical School']
    hms = hms[hms['name'].str.contains('GHD...')==True]
    add_to_df = pd.DataFrame([[calendar.month_name[month], 'Harvard Medical School GHD', len(hms.name)]],columns=['Month','Partner','Count for all Products'])
    partner_count_df = partner_count_df.append(add_to_df)
    # add product type CFFs to this reporting
    cffs = current_month_created[current_month_created.ProductTypeCode==169]
    add_to_df = pd.DataFrame([[calendar.month_name[month], 'Case Flash Forwards', len(cffs.name)]],columns=['Month','Partner','Count for all Products'])
    partner_count_df = partner_count_df.append(add_to_df)
    return partner_count_df.sort_values(by='Partner')

def major_compare(df, canon):
    """
    Checks Major Disciplines and Major Subjects and return a matrix of whether they match or not. 
    (rewritten from R metadata health script)
    """
    return_df = pd.DataFrame(columns=['Major Discipline', 'Major Subject', 'Number of Matches', 'Number of non-matches'])
    for row, disc in canon.iterrows():
        comp_yes = df[(df.MajorDiscipline == disc[0]) & (df.MajorSubject == disc[1])]
        comp_no = df[(df.MajorDiscipline != disc[0]) & (df.MajorSubject != disc[1])]        
        return_df = return_df.append(pd.DataFrame([[disc[0],disc[1],int(len(comp_yes.name)),int(len(comp_no.name))]],columns=['Major Discipline', 'Major Subject', 'Number of Matches', 'Number of non-matches']))
        return_df['Number of Matches'] = return_df['Number of Matches'].astype(int); return_df['Number of non-matches'] = return_df['Number of non-matches'].astype(int)
    return return_df
    
def taxo_count(df):
    """
    Breaks down the string values found in pimCategories (taxonomy terms) and
    returns a data frame listing how many terms are associated with which product
    (rewritten from R metadata health script)
    """
    return_df = pd.DataFrame(columns=['Number of taxo terms','Number of Products'], index=list(range(0,67)))
    counts_list = []
    for index, value in df.iterrows():
        if value.pimCategories == 'n/a':
            counts_list.append(0)
        else:
            counts_list.append(len(value.pimCategories.split(',')))
    for num in list(range(0,67)):
            return_df.loc[num]['Number of Products'] = counts_list.count(num)
    return_df['Number of taxo terms'] = list(range(0,67))
    return return_df

def breakup(row):
    return row.PersonAbouts.split(',')
def protagonist_splitup(df):
    """
    Splits product protagonist data into a data frame listing all protagonists for each product
    """
    df_copy = df[df['PersonAbouts'] != 'n/a']; df_copy = df_copy[df_copy['PersonAbouts'] != ' ']
    return_df = pd.DataFrame(columns=['Product ID','Protagonists'], index=df_copy.index); return_df = return_df.fillna('n/a')
    return_df['Protagonists'] = df_copy.apply(breakup, axis=1)
    return_df['Product ID'] = df_copy['name']
    return return_df
                    
#%% data transformations
# create a data frame of HBS cases created in the current month
hbs_main_case_stats = pd.DataFrame([hbs_created_month(now.month, now.year, he_frame),
	hbs_created_month(now.month-1, now.year, he_frame),
	hbs_created_month(now.month-2, now.year, he_frame),
	hbs_created_month(now.month-3, now.year, he_frame),
	hbs_created_month(now.month-4, now.year, he_frame)],
    columns=['Month','Cases','Supplements','Teaching Notes','Industry Notes'])
#create a data frame of HBS cases created in the current year
hbs_year_stats = pd.DataFrame([hbs_created_year(now.year, he_frame),
  hbs_created_year(now.year-1, he_frame),
  hbs_created_year(now.year-2, he_frame),
  hbs_created_year(now.year-3, he_frame),
  hbs_created_year(now.year-4, he_frame)],
  columns=['Fiscal year','Cases','Supplements','Teaching Notes','Industry Notes'])

# create a data frame of non-HBS cases created in the current month
partner_cases = [partner_case_count(now.month, now.year, he_frame),
    partner_case_count(now.month-1, now.year, he_frame),
    partner_case_count(now.month-2, now.year, he_frame),
    partner_case_count(now.month-3, now.year, he_frame)]
# create a data frame of all non-HBS products created in the current month
new_partner_setup = pd.concat([partner_all_products_count(now.month-3, now.year, he_frame),
    partner_all_products_count(now.month-2, now.year, he_frame),
    partner_all_products_count(now.month-1, now.year, he_frame),
    partner_all_products_count(now.month, now.year, he_frame)])
    
# creates a data frame analyzing how often a Major Discipline matches a Major Subject
comparison_major = major_compare(he_frame, major_canon)
# creates a count (distribution) of taxonomy terms 
count_of_taxo = taxo_count(he_frame)

#creates a data frame version of each product's protagonists
protags = protagonist_splitup(he_frame)
protag_df = pd.DataFrame(columns=['Product ID','1st protagonist', '2nd protagonist', '3rd protagonist', 
    '4th protagonist', '5th protagonist', '6th protagonist', '7th protagonist'], index=protags.index)
protag_df['Product ID'] = protags['Product ID']; protag_df = protag_df.fillna('n/a')
for index, value in protags.iterrows():
    for i in value['Protagonists']:
        for columnx in protag_df.columns:
            list_loc = list(protag_df.columns).index(columnx)
            if i in list(protag_df.loc[index]):
                break
            else:
                if protag_df.loc[index][list_loc] == 'n/a':
                    protag_df.loc[index][list_loc] = i

#%% duplicates (inactive for now)
'''
# create a data frame of duplicated products
dupe_df = main_df[pd.DataFrame.duplicated(main_df, subset='title', keep=False)==True]
dupe_df = dupe_df[dupe_df['title'].notnull() == True]

core_dupe_df = pd.DataFrame(columns=dupe_df.columns)
increment = 1
dupe_df_sorted = dupe_df.sort_values(by='title')
for product, row in dupe_df_sorted.iterrows():
    while increment != len(dupe_df_sorted.name):
        prod = dupe_df_sorted.iloc[increment]
        matching_prods = dupe_df_sorted[dupe_df_sorted.title == prod.title]
        if prod.title not in list(core_dupe_df.title):
            if matching_prods.ProductTypeCode.iloc[0] != matching_prods.ProductTypeCode.iloc[1]:
                core_dupe_df = core_dupe_df.append(matching_prods)
                increment += 1
                # print('match!', increment)
            else:
                increment += 1
                # print('non-match', increment)
        else:
            increment += 1
            # print('skipping line!', increment)

# create a list of duplicate product IDs
increment = 0
dupe_list = []
for product, row in dupe_df.iterrows():
    prod_num = dupe_df.name.iloc[increment]
    dupe_list.append(prod_num)
    increment += 1

# create a data frame of duplicate products, with availabilities
avail_dupe_df = pd.DataFrame(columns=avail_df.columns)
increment_avail = 0
for product, row in avail_df.iterrows():
    prod_num_avail = avail_df.name.iloc[increment_avail]
    prod_num_avail = prod_num_avail[0:6]
    if str(prod_num_avail) in dupe_list:
        avail_dupe_df = avail_dupe_df.append(avail_df.iloc[increment_avail])
        increment_avail += 1
    else:
        increment_avail += 1

# take data frame of duplicates with availabilities, and copy the title
# from the core record to each availability--necessary to sort by title
increment_avail = 0
avail_df_w_titles = avail_df.copy(deep=True)
avail_df_w_titles = avail_df_w_titles.fillna('n/a')
for product, row in avail_df_w_titles.iterrows():
    var = avail_df_w_titles.title.iloc[increment_avail]
    if var != 'n/a':
        prod_title = avail_df_w_titles.title.iloc[increment_avail]
        increment_avail += 1
    else:
        avail_df_w_titles.title.iloc[increment_avail] = prod_title
        increment_avail += 1


#write out CSVs
# core_dupe_df.to_csv(path_or_buf=path + '/output/duplicate_products.csv')


#this doesn't work so well, because my availabilities data frame is filled with incomplete data
def add_avails(dframe):
    increment = 0
    return_df = dframe.copy(deep=True)
    for product, row in dframe.iterrows():
        while increment != len(dframe.name):
            avails = dframe.iloc[increment].availabilities
            avails_list = avails.split(',')
            for avail in avails_list:
                if avail not in list(return_df.name):
                    return_df = return_df.append(avail_df_w_titles[avail_df_w_titles.name==str(avail)])
                    increment += 1
                else:
                    increment += 1
    return return_df.sort_values(by=['title','name'])
core_dupe_w_avails = add_avails(core_dupe_df)


# create a data frame for HKS duplicates of HBS products
hks = main_df[main_df.CopyrightHolder == 'Harvard Kennedy School']
hks_dupes = pd.DataFrame(columns=hks.columns)
for index, value in hks.iterrows():
    compare_hbs = main_df[main_df.title == value.title]
    if 'HBS' in list(compare_hbs.CopyrightHolder) or 'HBS Press' in list(compare_hbs.CopyrightHolder): 
        hks_dupes = hks_dupes.append(compare_hbs)
hks = hks.sort_values(by='title')
hks_dupes = hks_dupes.append(main_df[main_df.CopyrightHolder == 'HBS and Harvard Kennedy School'])
'''

#%% file writeout
# export a CSV of stats of HBS main case collection setup for reporting
hbs_main_case_stats.to_csv(path_or_buf=path + '/output/HBS Main Case Stats.csv')
hbs_year_stats.to_csv(path_or_buf=path + '/output/HBS Main Case Fiscal Year Stats.csv')
comparison_major.to_csv(path_or_buf=path + '/output/Comparison of Major Discpline and Major Subject.csv')