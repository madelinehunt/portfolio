# -*- coding: utf-8 -*-
'''
A collection of data analysis cells (mostly independent of each other) which
are useful for working with HBP's product data. 
'''
#%% initialization, ETL of data
import pandas as pd
import re
import os

from pim_etl import pim_etl
qa_flag = False
if qa_flag == True:
    a = pim_etl.io(qa_flag=True)
else:
    a = pim_etl.io()
products = a['products'].fillna('')
relationships = a['relationships'].fillna('')
contributors = a['contributors'].fillna('')
taxonomy = a['taxonomy'].fillna('')
categories = a['categories'].fillna('')
if qa_flag == True:
    qa_products = a['qa']['products'].fillna('')
    qa_relationships = a['qa']['relationships'].fillna('')
    qa_contributors = a['qa']['contributors'].fillna('')
    qa_taxonomy = a['qa']['taxonomy'].fillna('')
    qa_categories = a['qa']['categories'].fillna('')
    
he_standard = products[
    (products['Business Group'] == 'Higher Education') &
    (products['Status'] == 'C') &
    (products['Format'] == 'HCB') &
    (products['Product Type'] == 130) & 
    (products['Restriction Code'] == '99A')
]

hbp_current = products[
    (products['Status'] == 'C') &
    (products['Restriction Code'].isin(['99A','75A']))
]

test = he_standard.sample(n=10)

cores_only = products.drop_duplicates(subset='Product ID', keep='first')

#%% translate from data extracts to Bulk Edit templates
from datetime import datetime
template = pd.read_csv('/Users/nathaniel.hunt/Box Sync/In progreſs/PIM QA test restore/Bulk_Edit_Template_2017_08_17_10_30_43.csv',low_memory=False)
template_cols = list(template.columns)
extract_cols = list(products.columns)
non_overlap = {'Template': [x for x in template_cols if x not in extract_cols], 'Extract': [x for x in extract_cols if x not in template_cols]}
overlap = [x for x in extract_cols if x in template_cols]
cols_translation_core = {  
    #create dict of column names, mapping from extract columns to Bulk Edit templwate columns
    'Product ID': 'Product/Availability',
    'Brochure': 'brochure',
    'Business Group': 'BusinessGroup',
    'Chapter Number': 'chapterNumber',
    'Company About Description 1': 'CompanyAbouts', #need to concatenate here w/ Company About Description 2 etc
    'Company Employee Count': 'CompanyEmployeeCount',
    'Conference Indicator': 'productConference',
    'Copyright Holder Display Name': 'CopyrightDisplayName',
    'Copyright Holder': 'CopyrightHolder',
    'Owner Code': 'CopyrightOwnerCode',
    'Course': 'courseNames',
    'Course Type': 'courseTypes',
    'Demo URL': 'DemoUrl',
    'EBS Product Class': 'productEBSClass',
    'Enroll Now URL': 'EnrollNowUrl',
    'Event Begin Year': 'EventBeginYear',
    'Event End Year': 'EventEndYear',
    'Pre-Abstract': 'Pre-Abstract',
    'Abstract': 'Abstract',
    'Post-Abstract': 'Post-Abstract',
    'Revenue': 'Revenue',
    'Fifty Lessons Video Number': 'FiftyLessonsVideoNumber',
    'Free Trial': 'freeTrial',
    'HE Marketing Best Seller': 'HEMarketingBestSeller',
    'Has Test Bank': 'hasTestBank',
    'Instructional Videos': 'instructionalVideos',
    'Instructor Preview': 'instructorPreview',
    'Learning Objective Description': 'LearningObjectiveDescription',
    'Major Discipline': 'MajorDiscipline',
    'Major Subject': 'MajorSubject',
    'Marketing Category': 'MarketingCategory',
    'Marketing Description': 'MarketingDescription',
    'Marketing Materials Indicator': 'productMarketingMaterials',
    'Content Last Modified Date': 'ModifyDateTime',
    'Parent Number': 'ParentNumber',
    'Parent Title Override': 'parentTitle',
    'Person About Description 1': 'PersonAbouts', #will require concatenation
    'Product Class': 'productClass',
    'Core Product State': 'productState',
    'Product Type': 'ProductTypeCode',
    'Program Type': 'programType',
    'Right Type 1': 'rightTypes', #will require concatenation
    'Right Value 1': 'rightValues', #will require concatenation
    'Series 1': 'SeriesNames', #will require concatenation
    'Source': 'source',
    'Student Preview': 'studentPreview',
    'Subtitle': 'subtitle',
    'Title': 'title',
    'Abbreviated Title': 'TitleAbbrev'
}
cols_translation_avail = {
    'Status': 'Status',
    'Translator': 'Translator',
    'Accounting Rule ID': 'accountingRule',
    'Actual Revision Date': 'ActualRevisionDate',
    'Version list': 'ActualRevisionDateVersion',
    'Alternate ID Type 1': 'altIdTypes', #will require concatenation
    'Alternate ID Value 1': 'altIdValues', #will require concatenation
    'Authornet Flag': 'authornetFlag',
    'Availability Product State': 'availabilityProductState',
    'CL Eligible': 'businessUnitEligibility_CL',
    'Content Last Modified Date': 'ContentLastModifiedDate',
    'Distribution Availablity Date': 'DistributionAvailabilityDate',
    'Estimated Revision Date': 'EstimatedRevisionDate',
    'External Admin URL': 'externalAdminURL',
    'External Product URL': 'externalProductURL',
    'External Resource Path': 'externalResourcePath',
    'Flash Asset': 'flashAsset',
    'HBRG Eligible': 'businessUnitEligibility_HBRG',
    'HE Eligible': 'businessUnitEligibility_HE',
    'HE List Price Degree Granting': 'HEListPriceDegreeGranting',
    'HE List Price Non Degree Granting': 'HEListPriceNonDegreeGranting',
    'List Price': 'ListPrice',
    'Management Screens': 'managementScreens',
    'Multi User': 'multiPlayer',
    'Multi Scenario': 'multiScenario',
    'Operator Message': 'OperatorMessage',
    'Platform': 'platform',
    'Product Change Type': 'productChangeType',
    'Production Message': 'productionMessage',
    'Publication Date': 'PublicationDate',
    'Requirements Description': 'RequirementsDescription',
    'Restriction Code': 'RestrictionCode',
    'Restriction Message': 'RestrictionMessage',
    'Revision Comments': 'RevisionComments',
    'Revision Out Date': 'RevisionOutDate',
    'Revision Requestor': 'RevisionRequestor',
    'Revision Type': 'RevisionType',
    'Single Click Eligible': 'businessUnitEligibility_SingleClick',
    'Status Comments': 'StatusComments',
    'Status Data': 'statusData',
    'Status Date': 'StatusDate',
    'Suppress Sample Indicator': 'suppressSample',
    'Title Override': 'TitleOverride',
    'To Be Retired Indicator': 'toBeRetired',
    'Translation Submit': 'TranslationSubmitDate',
    'Unit Type 1': 'availabilityUnitTypes', #will require concatenation
    'Unit Value 1': 'availabilityUnitValues', #will require concatenation
    'Release Date': 'WebSiteReleaseDate',
    'Withdrawn Obsolete Indicator': 'WithdrawnObsoleteIndicator'
}
all_cols = {}
for key, value in cols_translation_avail.items():
    all_cols[key] = value
for key, value in cols_translation_core.items():
    all_cols[key] = value   
drop_cols = [x for x in extract_cols if x not in all_cols.keys()]

bool_core = [
    'Brochure',
    'Conference Indicator',
    'HE Marketing Best Seller',
    'Has Test Bank',
    'Marketing Materials Indicator'
]

bool_avail = [
    'CL Eligible',
    'Flash Asset',
    'HBRG Eligible',
    'HE Eligible',
    'Management Screens',
    'Multi User',
    'Multi Scenario',
    'Single Click Eligible',
    'Status Data',
    'Suppress Sample Indicator',
    'To Be Retired Indicator',
    'Withdrawn Obsolete Indicator',
    'Authornet Flag'
]

cpy_desc_vals = {
    'ABCC at Nanyang Tech University': '97',
    'Administration': '72',
    'APQC': '14',
    'Babson College': '15',
    'Benson P. Shapiro': '19',
    'Berrett-Koehler Publishers': '96',
    'BOS-IC': '18',
    'Business Enterprise Trust': '16',
    'Business Expert Press': '85',
    'Business Horizons/Indiana Univ.': '17',
    'California Management Review': '20',
    'Case Group': '30',
    'Chief Exec: McGraw-Hill': '31',
    'CLADEA-BALAS Case Consortium': '11',
    'Columbia Business School': '12',
    'Dartmouth University - Tuck Business School': '90',
    'David A. Lax and James K. Sebenius': '76',
    'Davis John': '32',
    'Design Management Inst': '22',
    'Enspire Learning': '23',
    'ESMT - European School of Management & Technology': '95',
    'ESMT - European School of Management and Technology': 'ESMT - European School of Management and Technology',
    'Fifty Lessons Ltd.': '52',
    'Gartner, Inc.': '24',
    'getAbstract': '25',
    'Greif Center for Entrep. Studies-USC Marshall': '109',
    'Harvard Advanced Leadership Initiative': '110',
    'Harvard Business Review': 'Harvard Business Review',
    'Harvard Business School': '84',
    'Harvard Business School - Publishing': '44',
    'Harvard Business School Publishing': '73',
    'Harvard Business School Publishing - Conferences': '69',
    'Harvard Business School Publishing - Corporate Learning': '70',
    'Harvard Business School Publishing - HBD': '13',
    'Harvard Business School Publishing - HBR': '27',
    'Harvard Business School Publishing - Higher Ed': '71',
    'Harvard Kennedy School': '88',
    'Harvard Medical School': '28',
    'Harvard T. H. Chan School of Public Health': '107',
    'Harvard University': '60',
    'HBS': '29',
    'HBS - Baker Library': '420',
    'HBS and G. Evans': '80',
    'HBS and Harvard Kennedy School': '99',
    'HBS and HSPH': '33',
    'HBS and IEM': '34',
    'HBS and INCAE': '35',
    'HBS and INSEAD': '36',
    'HBS and IPADE': '37',
    'HBS and ISTUD': '38',
    'HBS and Stanford U': '39',
    'HBS and Templeton, Oxford': '40',
    'HBS and Wharton School': '41',
    'HBS and Xanedu': '43',
    'HBS Press': '61',
    'HBS Streaming': '42',
    'HEC Montreal Centre for Case Studies': '26',
    'IE Business School': '106',
    'IESE Business School': '47',
    'IESE-INSIGHT MAGAZINE': '93',
    'IMD': '48',
    'Indian Institute of Management-Ahmedabad': '86',
    'Indian Institute of Management-Bangalore': '87',
    'Indian School of Business': '105',
    'INSEAD': '49',
    'Ivey Publishing': '68',
    'John S. Hammond, Ralph L. Keeney, and Howard Raiffa': '77',
    'Journal of Applied Corporate Finance': '46',
    'Journal of Information Technology': '101',
    'Journal of Information Technology Teaching Cases': '111',
    'Kate Ludeman and Eddie Erlandson': '75',
    'Kellogg School of Management, Northwestern Univ.': '50',
    'Ken Dychtwald, Tamara J. Erickson and Robert Morison': '74',
    'Lahore University of Management Sciences': '1002',
    'London Business School': '51',
    "Manchester Craftsmen's Guild": '54',
    'Marketing - HBSP': '53',
    'McGraw-Hill Education': '103',
    'McKinsey and Company': '79',
    'MIT Sloan Management Review': '65',
    'MPG': '55',
    'NBD': '56',
    'New Media': '57',
    'Non-HBS': '63',
    'North American Case Research Association (NACRA)': '94',
    'Pankaj Ghemawat': '58',
    'Perseus': '78',
    'Princeton University Press': '62',
    'Program on Negotiation at Harvard Law School (PON)': '102',
    'Public Education Leadership Project': '59',
    'Richard Ivey School of Business Foundation': 'Richard Ivey School of Business Foundation',
    'Rotman Management Magazine': '82',
    'Social Enterprise Knowledge Network': '64',
    'Soundview': '67',
    'Stanford Graduate School of Business': '91',
    'Stanford University': '66',
    'The Crimson Group': '98',
    'The Wharton School University of Pennsylvania': '100',
    'The Wharton School, University of Pennsylvania': 'The Wharton School, University of Pennsylvania',
    'Thomas R. Piper': '81',
    'Thunderbird School of Global Management': '83',
    'Tsinghua University': '89',
    'UC Berkeley - Haas School of Business': '104',
    'UCB - Haas School of Business': 'UCB - Haas School of Business',
    'University of Hong Kong': '45',
    'University of South Carolina': 'University of South Carolina',
    'University of Southern California-Marshall School of Business': '92',
    'University of Virginia Darden School Foundation': '21',
    'WDI Publishing at the University of Michigan': '108'
}

cpy_display_desc_vals = {
    'ABCC at Nanyang Tech University': '46',
    'Babson College': '13',
    'Berrett-Koehler Publishers': '45',
    'Business Enterprise Trust': '14',
    'Business Expert Press': '36',
    'Business Horizons': '15',
    'California Management Review': '16',
    'Carol LLC': 'Carol LLC',
    'CLADEA-BALAS Case Consortium': '11',
    'Columbia Business School': '12',
    'Columbia Business School': '17',
    'Darden School of Business ': '17', 
    'Design Management Institute': '18',
    'ESMT - European School of Management & Technology': '44',
    'Fifty Lessons': '1002',
    'Greif Center for Entrep. Studies-USC Marshall': '58',
    'Harvard Advanced Leadership Initiative': '59',
    'Harvard Business Press Books': '72',
    'Harvard Business Press Chapters': '73',
    'Harvard Business Publishing': '61',
    'Harvard Business Publishing Newsletters': '69',
    'Harvard Business Review': '60',
    'Harvard Business Review Case Discussion': '68',
    'Harvard Business Review Digital Article': '74',
    'Harvard Business School': '21',
    'Harvard Business School and Harvard Kennedy School': '48',
    'Harvard Business School Press': '67',
    'Harvard Kennedy School': '39',
    'Harvard ManageMentor': '75',
    'Harvard Medical School': '20',
    'Harvard Medical School': '56',
    'HBS Brief Case Teaching Notes': '71',
    'HBS Brief Cases': '70',
    'HEC Montreal Centre for Case Studies': '19',
    'IE Business School': '55',
    'IESE': '23',
    'IESE-Insight Magazine': '42',
    'IMD': '24',
    'Indian Institute of Management-Ahmedabad': '37',
    'Indian Institute of Management-Bangalore': '38',
    'Indian School of Business': '54',
    'Insead': '25',
    'INSEAD': '25',
    'Ivey Publishing': '32',
    'Journal of Information Technology': '50',
    'Kellogg School of Management': '26',
    'McGraw-Hill Education': '52',
    'MIT Sloan Management Review': '30',
    'North American Case Research Association (NACRA)': '43',
    'Perseus': '33',
    'Princeton University Press': '28',
    'Program on Negotiation at Harvard Law School (PON)': '51',
    'Public Education Leadership Project': '27',
    'Rotman Management Magazine': '34',
    'Social Enterprise Knowledge Network': '29',
    'Stanford Graduate School of Business': '31',
    'The Crimson Group': '47',
    'The Wharton School University of Pennsylvania': '49',
    'The Wharton School, University of Pennsylvania': '451',
    'Thunderbird School of Global Management': '35',
    'Tsinghua University': '40',
    'UC Berkeley - Haas School of Business': '53',
    'University of Hong Kong': '22',
    'University of Southern California-Marshall School of Business': '41',
    'WDI Publishing at the University of Michigan': '57'
}

cpy_owner_desc_vals = {
    'ADM': '57',
    'ALI': '89',
    'APQ': '14',
    'BAB': '15',
    'BAK': '411',
    'BEP': '64',
    'BET': '16',
    'BH': '17',
    'BKP': '75',
    'BLB': '978',
    'BOS': '18',
    'BPS': '19',
    'CL': '55',
    'CLB': '11',
    'CMR': '20',
    'COF': '54',
    'COL': '12',
    'DAR': '21',
    'DMI': '22',
    'ENS': '23',
    'ESM': '74',
    'GAB': '25',
    'GAR': '24',
    'HBD': '13',
    'HBK': '78',
    'HBR': '27',
    'HBS': '29',
    'HBX': '63',
    'HE': '56',
    'HEC': '26',
    'HKS': '67',
    'HKU': '31',
    'HMS': '28',
    'HSP': '86',
    'IEB': '85',
    'IES': '33',
    'IIM': '72',
    'IMA': '65',
    'IMB': '66',
    'IMD': '34',
    'INS': '35',
    'ISB': '84',
    'JAC': '32',
    'JIT': '80',
    'KEL': '36',
    'LBS': '37',
    'LES': '38',
    'LUMS': '1002',
    'MAR': '39',
    'MCG': '40',
    'MCH': '82',
    'MKN': '59',
    'MPG': '41',
    'MSB': '71',
    'NAC': '73',
    'NBD': '42',
    'NM': '43',
    'NTU': '76',
    'PEL': '45',
    'PG': '44',
    'PON': '81',
    'PRE': '46',
    'PRN': '47',
    'PRS': '58',
    'PUB': '30',
    'REP': '48',
    'RSM': '61',
    'SKE': '49',
    'SMR': '50',
    'STA': '51',
    'SV': '52',
    'TBS': '69',
    'TCG': '77',
    'TRP': '60',
    'TSI': '68',
    'TSM': '62',
    'TST': '100',
    'UCB': '83',
    'UPW': '79',
    'USC': '88',
    'UWO': '53',
    'WDI': '87'
}

biz_grp_desc_vals = {
    'Conferences': 'CONF',
    'Corporate Learning': 'CORP_LEARNING',
    'Harvard Business Digital': 'HBR_HBD',
    'Harvard Business Review': 'HBR',
    'Harvard Business Review Group': 'HBRG',
    'Higher Education': 'HED',
    'Marketing': 'MARKETING',
    'Press': 'PRESS'
}

date_cols = [
    'Publication Date',
    'Distribution Availablity Date',
    'Status Date',
    'Translation Submit',
    'Content Last Modified Date',
    'Estimated Revision Date'
]

foreign_lang_list = [
    'ALB',
    'ARA',
    'BEN',
    'CAT',
    'CHI',
    'CHT',
    'CZE',
    'DAN',
    'DUT',
    'EST',
    'FIN',
    'FRE',
    'GEO',
    'GER',
    'GRE',
    'HEB',
    'HIN',
    'HRV',
    'HUN',
    'ICE',
    'IND',
    'ITA',
    'JPN',
    'KAZ',
    'KOR',
    'LAV',
    'LIT',
    'MAC',
    'MAY',
    'MON',
    'NOR',
    'PER',
    'POL',
    'POR',
    'RUM',
    'RUS',
    'SLO',
    'SLV',
    'SPA',
    'SRP',
    'SWE',
    'TAM',
    'TEL',
    'THA',
    'TUR',
    'UKR',
    'VIE'
]

#%% 
def ebs(row):
    tmp = row['EBS Product Class']
    if len(tmp) > 1:
        tmp_list = tmp.split(' ')
        return tmp_list[0]
    else:
        return ''
        
def date_reformat(row):
    print(row['Availability ID'])
    for c in date_cols:
        if row[c] != '' and str(row[c]) != 'nan' and type(row[c]) == str:
            row[c] = str(datetime.strptime(str(row[c]),"%M/%d/%Y").isoformat())+'.000-05:00'
    new_list = []
    if row['Actual Revision Date'] != [] and type(row['Actual Revision Date']) == str:
        acrev_list = row['Actual Revision Date'].strip('[').strip(']')
        acrev_list = [x.strip() for x in acrev_list.split(',') if x != '' and len(x) > 2]
        for a in acrev_list:
            b = str(datetime.strptime(a.strip(),"%M-%d-%Y").isoformat())+'.000-05:00'
            new_list.append(b)
        row['Actual Revision Date'] = '^'.join(new_list)
    else:
        row['Actual Revision Date'] = ''
    return row
    # row['Actual Revision Date'].iloc[0]

def main(prod_id,df):
    prod_rows = df[df['Product ID'] == prod_id].fillna('')
    prod_rows['EBS Product Class'] = prod_rows.apply(ebs,axis=1)
    prod_rows['Product Type'] = prod_rows['Product Type'].astype(int)
    prod_rows = prod_rows.apply(date_reformat,axis=1)
    prod_rows['Accounting Rule ID'] = prod_rows['Accounting Rule ID'].astype(str).replace({'': 0}).astype(float).astype(int).replace({0:''})
# prod_rows['Accounting Rule ID'] = prod_rows['Accounting Rule ID'].replace({'': 0}).astype(int).replace({0:None})
    prod_rows['Event Begin Year'] = prod_rows['Event Begin Year'].astype(str).replace({'': 0}).astype(float).astype(int).replace({0:''})
    prod_rows['Event End Year'] = prod_rows['Event End Year'].astype(str).replace({'': 0}).astype(float).astype(int).replace({0:''})
    if '' in prod_rows['Product Class'] == False:
        prod_rows['Product Class'] = prod_rows['Product Class'].replace({'': 0}).astype(int).replace({0:''})
    else:
        prod_rows['Product Class'] = prod_rows['Product Class'].astype(int)
    
    
# prod_rows['Product Class'] = prod_rows['Product Class'].replace({'': 0}).astype(int).replace({0:None})

    prod_rows['Version list'] = prod_rows['Version list'].str.strip('[').str.strip(']').apply(lambda x: '^'.join(x.split(', ')))
    prod_rows['Copyright Holder Display Name'] = prod_rows['Copyright Holder Display Name'].replace(cpy_display_desc_vals)
    prod_rows['Copyright Holder'] = prod_rows['Copyright Holder'].replace(cpy_desc_vals)
    prod_rows['Owner Code'] = prod_rows['Owner Code'].replace(cpy_owner_desc_vals)
    for c in bool_core + bool_avail:
        prod_rows[c] = prod_rows[c].replace({'Y': 'True','N': 'False'})
    prod_rows['Business Group'] = prod_rows['Business Group'].replace(biz_grp_desc_vals)
    #create core-level row
    core_row = prod_rows.iloc[0].copy(deep=True)
    for col in cols_translation_avail.keys():
        core_row[col] = ''

    # concatenate core and avail fields to match Bulk Edit template
    core_concats = {
    'Company About Description 1': ['Company About Description 1','Company About Description 2','Company About Description 3','Company About Description 4','Company About Description 5'],
    'Person About Description 1': ['Person About Description 1','Person About Description 2','Person About Description 3','Person About Description 3','Person About Description 4','Person About Description 5'],
    'Right Type 1': ['Right Type 1','Right Type 2','Right Type 3','Right Type 4'],
    'Right Value 1': ['Right Value 1','Right Value 2','Right Value 3','Right Value 4'],
    'Series 1': ['Series 1','Series 2','Series 3']
    }
    return_dict = {}
    for i in core_concats.keys():
        tmp_list = []
        for a in core_concats[i]:
            if core_row[a] != '':
                tmp_list.append(core_row[a])
        if len(tmp_list) > 0:
            return_dict[i] = '^'.join(tmp_list)
    for i in return_dict.keys():
        core_row[i] = return_dict[i]
    new_prod_rows = pd.DataFrame(columns=prod_rows.columns)
    avail_concats = {
    'Alternate ID Type 1': ['Alternate ID Type 1','Alternate ID Type 2','Alternate ID Type 3','Alternate ID Type 4'],
    'Alternate ID Value 1': ['Alternate ID Value 1','Alternate ID Value 2','Alternate ID Value 3','Alternate ID Value 4'],
    'Unit Value 1': ['Unit Value 1','Unit Value 2'],
    'Unit Type 1': ['Unit Type 1','Unit Type 2']
    }
    for index, value in prod_rows.iterrows():        
        return_dict2 = {}
        for i in avail_concats.keys():
            tmp_list = []
            for a in avail_concats[i]:
                if value[a] != '':
                    tmp_list.append(str(value[a]))
            if len(tmp_list) > 0:
                return_dict2[i] = '^'.join(tmp_list)
        for i in return_dict2.keys():
            value[i] = return_dict2[i]
        new_prod_rows = new_prod_rows.append(value)
    prod_rows = new_prod_rows.copy()

    # remove core-level fields from avail-level
    for col in cols_translation_core.keys():
        prod_rows[col] = ''
        
    # merge core- and avail-level rows, and rename according to Bulk Edit template
    avail_ids = prod_rows['Availability ID']
    prod_rows['Product ID'] = avail_ids
    prod_rows = prod_rows.append(core_row)
    prod_rows = prod_rows.rename(columns=all_cols)
    prod_rows = prod_rows[[x for x in prod_rows.columns if x not in drop_cols]]
    
    
    # done = False
    # remove_list = []
    # while done == False:
    #     var = input('Any availabilities to leave out? \nProvide a comma-separated list below, or type "no":  \n> ')
    #     if var.lower() == 'n' or var.lower() == 'no':
    #         done = True
    #     else:
    #         tmp_list = [x.strip() for x in var.split(',')]
    #         for x in tmp_list:
    #             remove_list.append(str(x).upper())
    #         done = True
    # remove_cleanup = []
    # for i in remove_list: #CHANGE BACK
    #     if '-' in i:
    #         substrings = i.split('-')
    #         if 'ENG' not in substrings:
    #             if substrings[-1] not in foreign_lang_list:
    #                 substrings.append('ENG')
    #         if prod_id not in substrings:
    #             substrings.insert(0, prod_id)
    #         a = '-'.join(substrings)
    #         remove_cleanup.append(a)
    #     else: 
    #         a = prod_id + '-' + i + '-ENG'
    #         remove_cleanup.append(a)
    # prod_rows = prod_rows[prod_rows['Product/Availability'].isin(remove_cleanup) == False]

    return prod_rows
    


# test = main('8293')
# select = products[products['Product ID']=='8293']
# test.to_csv('/Users/nathaniel.hunt/Desktop/test.csv',index=False)

#function to restore metadata from PIM data extract
def restore(df):
    tmp = pd.DataFrame()
    for prod in list(set(df['Product ID'])):
        tmp = tmp.append(main(prod,qa_products))
    return tmp

pt_list = [148,146,150,158,164,144,161]
backup_df = qa_products[qa_products['Product Type'].isin(pt_list)].fillna('')
restored = restore(backup_df)
restored.to_csv('/Users/nathaniel.hunt/Desktop/restore.csv',index=False)
#%%
abby_prods = ['7050','8670','8623','4301','716801','4388','4700','7000','8867','8868','8869','8865']
a = qa_products[qa_products['Product ID'].isin(abby_prods)].fillna('')
a_rest = restore(a)
a_rest.to_csv('/Users/nathaniel.hunt/Desktop/restore abby.csv',index=False)

#%% prepare Bulk Edit for Relationships
cases = products[products['Product Type'] == 130]
include_list = [
                'Recommended for Use With', 
                'Also of Interest', 
                'Allows Purchase Of', 
                'Must Be Purchased With', 
                'Replaces', 
                'Also Recommended (Up-Sell)', 
                'Repurposed To', 
                'Contained By',
                'Requires', 
                'Contains', 
                'Instructed With', 
                'Must be Used With', 
                'Also Recommended (Cross-Sell)', 
                'Related Concept To', 
                'Supplemented By'
                ]
case_rels = relationships[(relationships['Base'].isin(cases['Product ID']) == True) & (relationships['Association Type'].isin(include_list)==True)]

#%% finding supporting cases 
re_include = ['\(.*\)', ' [A-L]$']
re_exclude = ['teaching note', '\(HBR', '\(VHS', '\(Harvard', '\(Commentary', '\(Abridged', '\(Condensed', '\(NTSC', '\(PAL', '\(Quick', '\(video','\(online','\(cd-rom','\(Virtual','Doer\'s Profile','\(NFl\)$']
supporting_cases = products[products['Product Type']==132]
for i in re_include:
    supporting_cases = supporting_cases.append(products[products['Title'].str.contains(i)])
for i in re_exclude:
    supporting_cases = supporting_cases[supporting_cases['Title'].str.contains(i, flags=re.IGNORECASE)==False]
    



#%% supporting_cases = supporting_cases.drop_duplicates(keep='first')
supp = supporting_cases[supporting_cases['Format'].isin(['PDF','HCB'])]
cont = supp[supp['Relationships'].str.contains('Must')==False]
cont = cont[cont['Relationships'].str.contains('Suppl')==False]
cont = cont[cont['Business Group'] == 'Higher Education']
cont2 = cont.drop_duplicates(subset='Product ID',keep='first')





#%% OBIEE audit for RCs
obi = pd.read_csv('/Users/nathaniel.hunt/Desktop/obi updated.csv',low_memory=False)
obi['Availability ID'] = obi['Product Number'] + '-' + obi['Format'] + '-' + obi['Language']
obi['Restriction Code'] = obi['Restriction Code'].fillna('')
obi.fillna('', inplace=True)
missing_formats_and_langs = obi[obi['Availability ID'] == '']
obi = obi.drop(missing_formats_and_langs.index)

pim_missing_rc = pd.DataFrame(columns=obi.columns)
backfill_rc = pd.DataFrame(columns=obi.columns)
backfill_rc['PIM Restriction Code'] = ''
backfill_rc['PIM Status'] = ''
backfill_rc['PIM BU'] = ''
backfill_rc['PIM Availability State'] = ''
backfill_rc['Product Type'] = ''
backfill_rc['HE/CL/HBR Eligibility Flags'] = ''
not_found_rc = pd.DataFrame(columns=obi.columns)
for prod in obi['Availability ID']:
   if prod in list(products['Availability ID']):
       compare = products[products['Availability ID'] == prod]
       if compare['Restriction Code'].iloc[0] == '':
           print(compare['Product ID'].iloc[0])
           pim_missing_rc = pim_missing_rc.append(obi[obi['Availability ID'] == prod])
       else:
           mask = obi[obi['Availability ID'] == prod]
           mask['PIM Restriction Code'] = compare['Restriction Code'].iloc[0]
           mask['PIM BU'] = compare['Business Group'].iloc[0]
           mask['PIM Status'] = compare['Status'].iloc[0]
           mask['PIM Availability State'] = compare['Availability Product State'].iloc[0]
           mask['Product Type'] = compare['Product Type Description'].iloc[0]
           mask['HE/CL/HBR Eligibility Flags'] = '/'.join([compare['HE Eligible'].iloc[0], compare['CL Eligible'].iloc[0], compare['HBRG Eligible'].iloc[0]])
           backfill_rc = backfill_rc.append(mask)
   else:
       not_found_rc = not_found_rc.append(obi[obi['Availability ID'] == prod])
backfill_rc = backfill_rc[list(obi.columns) + ['PIM BU', 'PIM Availability State', 'Product Type', 'PIM Status','PIM Restriction Code', 'HE/CL/HBR Eligibility Flags']]        
        
#%% OBIEE audit for everything else
obi = pd.read_csv('/Users/nathaniel.hunt/Desktop/Untitled Analysis.csv',low_memory=False)
obi['Availability ID'] = obi['Product Number'] + '-' + obi['Format'] + '-' + obi['Language']
obi.fillna('', inplace=True)
missing_formats_and_langs = obi[obi['Availability ID'] == '']
obi = obi.drop(missing_formats_and_langs.index)

col_list = ['Format',
 'Language',
 'Product Status',
 'Product Type',
 'Product Class',
 'List Price',
 'HE Eligible',
 'Copyright Holder',
 'Title',
 'Availability ID']

pim_data_cols = ['Format','Language','Status','Product Type','Product Class','List Price','HE Eligible','Copyright Holder','Title','Availability ID']

cols_zip = dict(zip(col_list, pim_data_cols))
pim_missing_non_rc = pd.DataFrame(columns=obi.columns)
not_found_in_pim_non_rc = pd.DataFrame(columns=obi.columns)
obiee_missing_non_rc = pd.DataFrame(columns=obi.columns)
obiee_missing_non_rc['Missing Data in OBIEE'] = ''
#%%
for prod in obi['Availability ID']:
    if prod in list(products['Availability ID']):
        compare = products[products['Availability ID'] == prod]
        for obiee_col, pim_col in cols_zip.items():
            if compare[pim_col].iloc[0] == '':
                print(pim_col, ':   ', compare['Product ID'].iloc[0])
                pim_missing_non_rc = pim_missing_non_rc.append(obi[obi['Availability ID'] == prod])
            #    else:
            #        mask = obi[obi['Availability ID'] == prod]
            #        mask['PIM Restriction Code'] = compare['Restriction Code'].iloc[0]
            #        backfill_rc = backfill_rc.append(mask)
            else: 
                tmp = obi[obi['Availability ID'] == prod]
                if tmp[obiee_col].iloc[0] == '':
                    tmp['Missing Data in OBIEE'] = obiee_col + ':  ' + str(compare[pim_col].iloc[0])
                    obiee_missing_non_rc = obiee_missing_non_rc.append(tmp)
    else:
        not_found_in_pim_non_rc = not_found_in_pim_non_rc.append(obi[obi['Availability ID'] == prod])

        
#%% check against OBIEE 10g
obi10g = pd.read_csv('/Users/nathaniel.hunt/Box Sync/In progreſs/OBIEE auditing/obi 10g data.csv',low_memory=False)
obi10g['Availability ID'] = obi10g['Product Number'] + '-' + obi10g['Format'] + '-' + obi10g['Language']
obi10g.fillna('', inplace=True)

#%%
obi = pd.read_csv('/Users/nathaniel.hunt/Box Sync/In progreſs/OBIEE auditing/obiee rc.csv',low_memory=False)
obi['Availability ID'] = obi['Product Number'] + '-' + obi['Format'] + '-' + obi['Language']
obi['Restriction Code'] = ''
obi.fillna('', inplace=True)

carol_report = pd.DataFrame(columns=products.columns)
for prod in obi['Availability ID']:
    if prod in list(products['Availability ID']):
        compare = products[products['Availability ID'] == prod]
        if compare['Restriction Code'].iloc[0] != '':
            carol_report = carol_report.append(compare)


#%% HBR list price bulk edits
books = products[(products['Product Type'] == 170) & (products['Status'] == 'C')]
pimport = pd.DataFrame()
cat_1 = books[(books['Format'] == 'HBK') | (books['Format'] == 'PBK')]
cat_1['HE List Price Degree Granting'] = cat_1['List Price']
cat_1['HE List Price Non Degree Granting'] = cat_1['List Price']
cat_2 = books[(books['Format'] == 'PDF') | (books['Format'] == 'EPB')]
cat_2['HE List Price Degree Granting'] = cat_2['List Price'] / 2
cat_2['HE List Price Non Degree Granting'] = cat_2['List Price']
both_cat = cat_1.append(cat_2)
pimport['Product/Availability'] = both_cat['Availability ID']
pimport['ListPrice'] = both_cat['List Price']
pimport['HEListPriceDegreeGranting'] = both_cat['HE List Price Degree Granting']
pimport['HEListPriceNonDegreeGranting'] = both_cat['HE List Price Non Degree Granting']
pimport.to_csv('/Users/nathaniel.hunt/Desktop/pimport.csv',index=False)








#%% Carol purge reports
obi = pd.read_csv('/Users/nathaniel.hunt/Box Sync/In progreſs/OBIEE auditing/obiee rc.csv',low_memory=False)
obi['Availability ID'] = obi['Product Number'] + '-' + obi['Format'] + '-' + obi['Language']
obi['Restriction Code'] = ''
obi.fillna('', inplace=True)

carol_report = pd.DataFrame(columns=products.columns)
for prod in obi['Availability ID']:
    if prod in list(products['Availability ID']):
        compare = products[products['Availability ID'] == prod]
        if compare['Restriction Code'].iloc[0] != '':
            carol_report = carol_report.append(compare)
#%%
carol_rels = ['Supplemented By','Instructed With']
def get_rela(row):
    if row['Association Type'] in carol_rels:
        return row['Target'] + ' ' + row['Association Type']
    else:
        return ''
        
relationships.Relationships = relationships.apply(get_rela, axis=1)

rel_dict_carol = {}
test = relationships.groupby('Base')
for key, item in test:
    rel_dict_carol[key] = '^'.join([x for x in list(item['Relationships']) if x != ''])

#%%

test = carol_report[(carol_report['Copyright Holder'] == 'HBS') & (carol_report['Format'] == 'PDF') & (carol_report['Language'] == 'ENG') & (carol_report['Status'] == 'C') & (carol_report['Restriction Code'] == '99A') == True]


#%% carol purge report
os.chdir('/Users/nathaniel.hunt/Desktop/Collection Partner Reports FY17 Final')
files_list = [x for x in os.listdir() if '.DS_Store' not in x and '~' not in x]

def add_rel(row):
    carol_rels = ['Supplemented By','Instructed With']
    tmp = relationships[relationships['Base'] == row['Prod Num']]
    tmp = tmp[tmp['Association Type'].isin(carol_rels)]
    rel_list = list(tmp['Association Type'])
    targ_list = list(tmp['Target'])
    together = '^'.join([x + ' ' + targ_list[rel_list.index(x)] for x in rel_list])
    return together
def add_rel2(row):
    a = row['Prod Num']
    if a in rel_dict_carol:
        return rel_dict_carol[a]
    else:
        return ''

df_dict = {}
for f in files_list:
    df = pd.read_excel(f)
    df['Relationships'] = df.apply(add_rel2,axis=1)
    df_dict[f] = df
    df.to_excel(f,index=False)
    
#%%
uppers = [x for x in list(set(he_products['Copyright Holder'])) if x != '' and bool(re.search('[A-Z]',x[1:])) == True]
upper_dict = {}
for c in uppers:
    length = len(list(set(
        he_products[
            (he_products['Copyright Holder'] == c) & 
            (he_products['Status'].isin(['C','O'])) &
            ((he_products['Product Type'] == 130) | (he_products['Product Type'] == 132))
            ]['Product ID']
            )))
    df = he_products[he_products['Copyright Holder'] == c]
    upper_dict[c + ' {}'.format(length)] = df
    
#%% testing relationship Bulk Edit    
pimport = pd.DataFrame(columns=['Product/Availability','Association Type','Target','Change Type'])
# test_prods = list(
#         products[
#             (products['Business Group'] == 'Higher Education') & 
#             (products['Status'].isin(['C','O']))
#         ].sample(n=10)['Product ID']
#     )
test_prods = ['W11395', '792091', '810066', 'KEL080', '509013', 'W11596', 'SM56', '809M02', 'KEL054', '914036']
include_list = ['Recommended for Use With', 'Also of Interest', 'Allows Purchase Of', 'Must Be Purchased With', 'Replaces', 'Also Recommended (Up-Sell)', 'Repurposed To', 'Contained By','Requires', 'Contains', 'Instructed With', 'Must be Used With', 'Also Recommended (Cross-Sell)', 'Related Concept To', 'Supplemented By']
existin_rels = relationships[(relationships['Base'].isin(test_prods)) & (relationships['Association Type'].isin(include_list))]
# existin_rels = existin_rels[existin_rels['Association Type'].isin(include_list)]
existin_rels2 = existin_rels[['Base','Association Type','Target']].rename(columns={'Base':'Product/Availability'})
existin_rels2['Change Type'] = 'Delete'
test_prods_df = products[products['Product ID'].isin(test_prods)]
pimport = pimport.append(existin_rels)
test_targets = pd.Series(['SKE154', 'KEL196', 'W16087', 'HKU021', '95A011', '815032', 'W16199', 'HKS749', '394031', '507014'], index=range(10,20))
test_base = pd.Series(list(existin_rels2['Product/Availability']),index=range(10,20))
test_rels = pd.Series(['Instructed With','Instructed With','Instructed With','Supplemented By','Supplemented By','Recommended for Use With','Recommended for Use With','Recommended for Use With','Recommended for Use With','Recommended for Use With'], index=range(10,20))
tmp = pd.DataFrame({'Product/Availability': test_base, 'Association Type': test_rels, 'Target': test_targets, 'Change Type': 'Add'},columns=['Product/Availability','Association Type','Target','Change Type'])
pimport = pimport.append(tmp)
add_new = tmp.copy()
delete_add_new = tmp.copy(deep=True); delete_add_new['Change Type'] = 'Delete'
delete_existing = relationships_reformat(existin_rels2.copy()); delete_existing['Change Type'] = 'Delete'
restore_existing = relationships_reformat(existin_rels2.copy(deep=True)); restore_existing['Change Type'] = 'Add'
os.chdir('/Users/nathaniel.hunt/Desktop')
if 'relationships test' not in os.listdir():
    os.mkdir('relationships test')
os.chdir('relationships test')
add_new.to_csv('1a Add new.csv',index=False)
delete_add_new.to_csv('1b Deleting added new.csv',index=False)
delete_existing.to_csv('2a Deleting existing relationships.csv',index=False)
restore_existing.to_csv('2b Restoring existing relationships.csv',index=False)

# tmp.to_csv('/Users/nathaniel.hunt/Desktop/pimport.csv',index=False)
#%% replace references to reciprocals with main relationships 
rel_pair_dict = {'Allows Purchase Of': 'Must Be Purchased With', 'Also Recommended (Cross-Sell)': 'Also Recommended (Cross-Sell)', 'Also Recommended (Up-Sell)': 'Also Recommended (Up-Sell)', 'Also of Interest': 'Also of Interest', 'Availability': 'Availability', 'Bundle': 'Bundle', 'Contains': 'Contained By', 'Contained By': 'Contains', 'Contributor Alias': 'Contributor Alias', 'Contributors': 'Contributors', 'Instructed With': 'Teaching Instruction For', 'Teaching Instruction For': 'Instructed With', 'Kit': 'Kit', 'Must Be Purchased With': 'Allows Purchase Of', 'Must be Used With': 'Supplemented By', 'Prerequires': 'Prerequires', 'Recommended for Use With': 'Recommended for Use With', 'Related Concept To': 'Related Concept To', 'Replaces': 'Repurposed To', 'Repurposed To': 'Replaces', 'Requires': 'Requires', 'Source': 'Source', 'Supported By': 'Supports', 'Supports': 'Supported By', 'Supplemented By': 'Must be Used With', 'Is Sibling Of': 'Is Sibling Of'}
os.chdir('/Users/nathaniel.hunt/Desktop/relationships bulk edit')
def handle_errors(df):
    f = input('Which error log to parse? \n >  ')
    if '.txt' not in f:
        f += '.txt'
    try:
        fs = open(f, 'r+')
        text = fs.read()
    except:
        print('file not found!')
    text_lines = [x for x in text.split('\n') if 'TARGET' in x]
    bases = [x.split(' ')[0] for x in text_lines]
    assocs = [x.split("'")[1] for x in text_lines]
    targets = [x.split("'")[-2] for x in text_lines]
    if len(assocs) == 0:
        print('\n error log is formatted incorretly!')
        return df
    replace = pd.DataFrame({'Product/Availability': bases, 'Association Type': assocs, 'Target': targets},columns=['Product/Availability','Association Type','Target'])
    return_df = df[df['Product/Availability'].isin(replace['Product/Availability'])==False]
    if 'Sort' in return_df.columns:
        return_df = return_df[['Product/Availability','Association Type','Target']]
    replace = relationships_reformat(replace)
    return_df = return_df.append(replace)
    return return_df


def relationships_reformat(df):
    bulk_edit = pd.DataFrame()
    for key, row in df.iterrows():
        if row['Association Type'] in rel_pair_dict.values():
            row_rel = row['Association Type']
            mask = relationships[(relationships['Target'] == row['Product/Availability']) & (relationships['Base'] == row['Target']) & (relationships['Association Type'] == rel_pair_dict[row_rel])]
            # print(len(mask))
            bulk_edit = bulk_edit.append(mask)
        else:
            bulk_edit = bulk_edit.append(row)
    bulk_edit = bulk_edit[['Base','Association Type','Target']].rename(columns={'Base':'Product/Availability'})
    return bulk_edit

#%% generating sample DF
include_list = ['Recommended for Use With', 'Also of Interest', 'Allows Purchase Of', 'Must Be Purchased With', 'Replaces', 'Also Recommended (Up-Sell)', 'Repurposed To', 'Contained By','Requires', 'Contains', 'Instructed With', 'Must be Used With', 'Also Recommended (Cross-Sell)', 'Related Concept To', 'Supplemented By','Supports','Supported By']


he_current = products[(products['Business Group'] == 'Higher Education') & (products['Status'] == 'C')]
sample_prods = he_current['Product ID'].sample(n=500)
sample_df = relationships[(relationships['Base'].isin(sample_prods)) & (relationships['Association Type'].isin(include_list))].rename(columns={'Base':'Product/Availability'})
sample_df['Change Type'] = 'Delete'
sample_df = sample_df.drop_duplicates(keep='first')
sample_df.to_csv('/Users/nathaniel.hunt/Desktop/relationships test/pimport.csv',index=False)

#%% supporting cases
def pt_lookup(row):
    pt = row['Product Type']
    prod = str(row['Product ID'])
    if type(pt) != None:
        try: 
            return str(prod_ids_and_types[prod])
        except:
            return 'Lookup in PIM'
    else:
        return str(pt)
    
def strip_non_alpha(row):
    prod = str(row['Product ID'])
    prod = ''.join([x for x in prod if x.isalnum() == True])
    return prod

    
    
arrin = pd.read_excel('/Users/nathaniel.hunt/Desktop/TEST DATA for ABC relationship cleanup_9-22-17.xlsx')
arrin['Product ID'] = arrin.apply(strip_non_alpha,axis=1)
arrin = arrin[['Product ID','Title','Product Type','Copyright Holder','Status','Notes']]
arrin = arrin[arrin['Notes'].str.contains('OUT OF PRINT') == False]
arrin = arrin[arrin['Notes'].str.contains('Moved on spreadsheet') == False]
prod_ids_and_types = dict(zip(products['Product ID'],products['Product Type']))
arrin['Product Type'] = arrin.apply(pt_lookup,axis=1)


#%% creating relationships from Arrin's spreadsheet
    # TODO rewrite to look at n# of rows past 130s/134s
    # TODO ask Caitlin which PTs to look at for A cases (130s… 134s too?)
manual = pd.DataFrame()
supp_df = pd.DataFrame(columns=['Product/Availability','Association Type','Target','Change Type','Base Title','Target Title'])
products130s = arrin[arrin['Product Type'] == '130']
for prod in list(set(products130s['Product ID'])):
    prod_row = products130s[products130s['Product ID'] == prod]
    title = str(prod_row['Title'].iloc[0])
    # TODO process title to strip out parentheses and A/B/C/D or I/II/III
    if bool(re.search('\([A-Z]\)',title))==True:
        ind = re.search('\([A-Z]\)',title).start()
        title = title [:ind]
        # roman = False
    # elif bool(re.search(r'I+$',title))==True:
    #     ind = re.search(r'I+$',title).start()
    #     title = title [:ind]
    #     roman = True
    #     romans = romans.append(prod_row)
    else:
        title = title[:9]  
    cpy = prod_row['Copyright Holder'].iloc[0]
    notes = prod_row['Notes'].iloc[0]
    if bool(re.search('eeds updated relationships',notes)) == False or bool(re.search('no A case',notes)) == True:
        manual = manual.append(prod_row)
    else:
        title_re = '^' + re.escape(title) + '\('
        prod_set = arrin[(arrin['Title'].str.contains(title_re) == True) & (arrin['Copyright Holder']==cpy)]
        pt132s = list(prod_set[prod_set['Product Type'] == '132']['Product ID'])
        for target in pt132s:
            tmp = {}
            tmp['Product/Availability'] = prod
            tmp['Association Type'] = 'Supported By'
            tmp['Target'] = target
            tmp['Change Type'] = 'Add'
            tmp['Base Title'] = prod_row['Title'].iloc[0]
            tmp['Target Title'] = prod_set[prod_set['Product ID'] == target]['Title'].iloc[0]
            check_rows = r2[(r2['Product/Availability'] == prod) & (r2['Association Type'] == 'Supported By')]
            if target not in list(check_rows['Target']):
                supp_df = supp_df.append(tmp, ignore_index=True)
sibling_dict = {}
for a in list(set(supp_df['Product/Availability'])):
    a_row = supp_df[supp_df['Product/Availability'] == a]
    sibling_dict[a] = list(a_row['Target'])
arrin_siblings = siblingizer(sibling_dict)
arrin_siblings2 = pd.DataFrame(columns = arrin_siblings.columns)
supp_df = supp_df[['Product/Availability','Association Type','Target','Change Type']]
supp_df.to_csv('/Users/nathaniel.hunt/Desktop/relationships test/arrin test.csv',index=False)
check_r2 = r2[r2['Association Type'] == 'Is Sibling Of']
check_tuple = tuple(zip(check_r2['Product/Availability'],check_r2['Target']))
for key, value in arrin_siblings.iterrows():
    if (value['Product/Availability'],value['Target']) not in check_tuple and (value['Target'],value['Product/Availability']) not in check_tuple:
        arrin_siblings2 = arrin_siblings2.append(value)
arrin_siblings2.to_csv('/Users/nathaniel.hunt/Desktop/relationships test/arrin siblings test.csv',index=False)

#%% 
pt_132list = list(set(products[(products['Product ID'].isin(sample_df['Product/Availability'])) & (products['Product Type'] == 132)]['Product ID']))
pt_132s_with_relationships = list(set(sample_df[sample_df['Product/Availability'].isin(pt_132list)]['Product/Availability']))
a_cases = {}
for i in pt_132s_with_relationships:
    prod_row = products[products['Product ID'] == i]
    a_df = products[(products['Product Type'] == 130) & (products['Title'].str.contains('^' + prod_row['Title'].iloc[0][0:15])) & (products['Format'] == 'HCB')]
    a_try = products[(products['Product Type'] == 130) & (products['Title'].str.contains('^' + prod_row['Title'].iloc[0][0:15])) & (products['Format'] == 'HCB')]['Product ID'].iloc[0]
    all_sups = list(set(products[(products['Product Type'] == 132) & (products['Title'].str.contains('^' + prod_row['Title'].iloc[0][0:15])) & (products['Format'] == 'HCB')]['Product ID']))
    a_cases[a_try] = all_sups
    
#%%
non_rels = ['Contributors','Availability','Taxonomy Terms','Category']
prod_ids_and_types = dict(zip(products['Product ID'],products['Product Type']))
r2 = relationships[relationships['Association Type'].isin(non_rels) == False]
r2 = r2.rename(columns={'Base':'Product/Availability'})
r2['Base PT'] = r2.apply(base_pt_lookup, axis=1, args=(prod_ids_and_types,))
r2['Target PT'] = r2.apply(target_pt_lookup, axis=1, args=(prod_ids_and_types,))
#%%

def a_cases_dictionizer(product_df, relationship_df):
    if 'Base' in relationship_df.columns:
        relationship_df = relationship_df.rename(columns={'Base':'Product/Availability'})
    prod_ids_and_types = dict(zip(product_df['Product ID'],product_df['Product Type']))
    if 'Base PT' not in relationship_df.columns:
        relationship_df['Base PT'] = relationship_df.apply(base_pt_lookup, axis=1, args=(prod_ids_and_types,))
    if 'Target PT' not in relationship_df.columns:
        relationship_df['Target PT'] = relationship_df.apply(target_pt_lookup, axis=1, args=(prod_ids_and_types,))
    he_current = product_df[(product_df['Business Group'] == 'Higher Education') & (product_df['Status'] == 'C')]
    pt130s = he_current[he_current['Product Type'] == 130]
    a_case_rels = relationship_df[(relationship_df['Product/Availability'].isin(pt130s['Product ID'])) & (relationship_df['Target PT'] == 132)]
    a_case_dict = {}
    for a in list(set(a_case_rels['Product/Availability'])):
        a_row = a_case_rels[a_case_rels['Product/Availability'] == a]
        a_case_dict[a] = list(a_row['Target'])
    return a_case_dict
    # for case in a_case_rels['Product/Availability']:
    
def base_pt_lookup(row,prod_ids_and_types):
    p_id = row['Product/Availability']
    if p_id in prod_ids_and_types.keys():
        return prod_ids_and_types[p_id]
    else:
        return ''
    
def target_pt_lookup(row,prod_ids_and_types):
    p_id = row['Target']
    if p_id in prod_ids_and_types.keys():
        return prod_ids_and_types[p_id]
    else:
        return ''

def siblingizer(dictionary):
    siblings = pd.DataFrame(columns=['Product/Availability','Association Type','Target','Change Type'])
    for k, v in dictionary.items():
        for i in v:
            if i not in siblings['Product/Availability']:
                for x in [x for x in v if x != i]:
                    pairs = tuple(zip(siblings['Product/Availability'],siblings['Target']))
                    if (i, x) not in pairs and (x, i) not in pairs:
                        tmp_dict = {}
                        tmp_dict['Product/Availability'] = i
                        tmp_dict['Association Type'] = 'Is Sibling Of'
                        tmp_dict['Target'] = x
                        tmp_dict['Change Type'] = 'Add'
                        siblings = siblings.append(tmp_dict,ignore_index=True)
    siblings = siblings.drop_duplicates(keep='first')
    return siblings
#siblings.to_csv('/Users/nathaniel.hunt/Desktop/relationships test/siblings.csv',index=False)

#%%
all_cols = ['Product ID',
 'Availability Part Number',
 'Availability ID',
 'Format',
 'Language',
 'Core Product State',
 'Availability Product State',
 'Title',
 'Abbreviated Title',
 'Product Class',
 'Business Group',
 'Marketing Category',
 'Pre-Abstract',
 'Abstract',
 'Post-Abstract',
 'Marketing Description',
 'Learning Objective Description',
 'Event Begin Year',
 'Event End Year',
 'Company About Description 1',
 'Company About Description 2',
 'Company About Description 3',
 'Company About Description 4',
 'Company About Description 5',
 'Company Employee Count',
 'Person About Description 1',
 'Person About Description 2',
 'Person About Description 3',
 'Person About Description 4',
 'Person About Description 5',
 'Revenue',
 'Course',
 'Course Type',
 'Program Type',
 'Copyright Holder',
 'Right Type 1',
 'Right Type 2',
 'Right Type 3',
 'Right Type 4',
 'Right Value 1',
 'Right Value 2',
 'Right Value 3',
 'Right Value 4',
 'Series 1',
 'Series 2',
 'Series 3',
 'Major Discipline',
 'Major Subject',
 'Demo URL',
 'Enroll Now URL',
 'Fifty Lessons Video Number',
 'Parent Number',
 'Product Type',
 'Source',
 'Authornet Flag',
 'List Price',
 'Publication Date',
 'Distribution Availablity Date',
 'Release Date',
 'Status',
 'Status Comments',
 'Restriction Code',
 'Restriction Message',
 'Requirements Description',
 'Alternate ID Type 1',
 'Alternate ID Type 2',
 'Alternate ID Type 3',
 'Alternate ID Type 4',
 'Alternate ID Value 1',
 'Alternate ID Value 2',
 'Alternate ID Value 3',
 'Alternate ID Value 4',
 'Operator Message',
 'Unit Value 1',
 'Unit Value 2',
 'Unit Type 1',
 'Unit Type 2',
 'Product Change Type',
 'Status Date',
 'Actual Revision Date',
 'Version list',
 'Estimated Revision Date',
 'Revision Out Date',
 'Revision Requestor',
 'Revision Comments',
 'Revision Type',
 'Withdrawn Obsolete Indicator',
 'Title Override',
 'Translator',
 'Translation Submit',
 'Content Last Modified Date',
 'Production Message',
 'External Product URL',
 'External Admin URL',
 'Add Timestamp',
 'Last Updated Time',
 'Insert Update Flag',
 'Core Add Timestamp',
 'Core Last Updated Timestamp',
 'To Be Retired Indicator',
 'Suppress Sample Indicator',
 'Content Modified Date Time Aspect',
 'CL Eligible',
 'HBRG Eligible',
 'HE Eligible',
 'Single Click Eligible',
 'EBS Product Class',
 'Accounting Rule ID',
 'Marketing Materials Indicator',
 'Conference Indicator',
 'Protagonist First Name 1',
 'Protagonist Middle Name 1',
 'Protagonist Last Name 1',
 'Protagonist Job Title 1',
 'Protagonist Gender 1',
 'Protagonist Location 1',
 'Protagonist Race 1',
 'Protagonist First Name 2',
 'Protagonist Middle Name 2',
 'Protagonist Last Name 2',
 'Protagonist Job Title 2',
 'Protagonist Gender 2',
 'Protagonist Location 2',
 'Protagonist Race 2',
 'Protagonist First Name 3',
 'Protagonist Middle Name 3',
 'Protagonist Last Name 3',
 'Protagonist Job Title 3',
 'Protagonist Gender 3',
 'Protagonist Location 3',
 'Protagonist Race 3',
 'Owner Code',
 'Copyright Holder Display Name',
 'Product Type Description',
 'Format Description',
 'Parent Number Description',
 'Product Status Description',
 'HE List Price Degree Granting',
 'HE List Price Non Degree Granting',
 'Management Screens',
 'Status Data',
 'Flash Asset',
 'Multi Scenario',
 'Multi User',
 'Platform',
 'External Resource Path',
 'Instructional Videos',
 'Brochure',
 'HE Marketing Best Seller',
 'Student Preview',
 'Instructor Preview',
 'Parent Title Override',
 'Subtitle',
 'Chapter Number',
 'Free Trial',
 'Has Test Bank',
 'Executive Summary',
 'Contributors',
 'Taxonomy',
 'Categories',
 'Relationships']

def col_grab(col):
    return products[products[col].isnull() == False]

brochures = col_grab('Brochure')
platform = col_grab('Platform')
ext_res = col_grab('External Resource Path')
inst_vid = col_grab('Instructional Videos')
inst_preview = col_grab('Instructor Preview')
free_trial = col_grab('Free Trial')



#%% importing OBIEE sales data
obi = pd.read_csv('/Users/nathaniel.hunt/Desktop/OBIEE sales.csv',low_memory=False)
obi['Availability ID'] = obi['Product Number'] + '-' + obi['Format'] + '-' + obi['Language']
obi.fillna('', inplace=True)
missing_formats_and_langs = obi[obi['Availability ID'] == '']
obi = obi.drop(missing_formats_and_langs.index)
obi_sales = {}
obi_sales = dict(zip(obi['Availability ID'],obi['Sales Amount']))

def sales_fillout(row):
    a = row['Availability ID']
    if a in obi_sales.keys():
        return obi_sales[a]
    else:
        return ''

obi_sales_units = dict(zip(obi['Availability ID'],obi['Sales Units']))

def sales_fillout_units(row):
    a = row['Availability ID']
    if a in obi_sales_units.keys():
        return obi_sales_units[a]
    else:
        return ''
products['Sales Amount'] = products.apply(sales_fillout,axis=1);
products['Sales Units'] = products.apply(sales_fillout_units,axis=1)
sales = products[(products['Sales Amount'] != '')]
sales['Sales Amount'] = sales['Sales Amount'].astype(float)
sales['Sales Units'] = sales['Sales Units'].astype(float)
sales = sales[sales['Sales Units'] >= 1]
sales_w_negative = sales.copy(deep=True)
sales = sales[(sales['Sales Amount'] > 0) & (sales['Sales Units'] >= 1)]
sales['Avg. Sale Price'] = sales['Sales Amount']/sales['Sales Units']
sales['Avg. Sale Price'] = sales['Avg. Sale Price'].astype(float)
sales = sales[['Availability ID','Product Type','Title','Pre-Abstract','Abstract','Post-Abstract','Major Discipline','Major Subject','Copyright Holder','Taxonomy','Relationships','Sales Amount','Sales Units','Avg. Sale Price']]
sales = sales[sales['Sales Amount'] > 0]
sales['Sales Amount'] = sales['Sales Amount'].astype(float)
sales_means_disc = sales.groupby(['Major Discipline'],as_index=False).mean(); sales_means_disc['Avg. Sale Price'] = sales_means_disc['Sales Amount']/sales_means_disc['Sales Units']
sales_means_cpy = sales.groupby(['Copyright Holder'],as_index=False).mean(); sales_means_cpy['Avg. Sale Price'] = sales_means_cpy['Sales Amount']/sales_means_cpy['Sales Units']
sales_pt = sales.groupby(['Product Type'],as_index=False).mean(); sales_pt['Avg. Sale Price'] = sales_pt['Sales Amount']/sales_pt['Sales Units']

#%%
def taxo_count(row):
    taxo = row['Taxonomy'].split('^')
    return len(taxo)

def catg(row):
    if row['Pre-Abstract'].strip() != '' and row['Post-Abstract'].strip() == '':
        return 'Has Pre-Abstract'
    elif row['Post-Abstract'].strip() != '' and row['Pre-Abstract'].strip() == '':
        return 'Has Post-Abstract'
    elif row['Pre-Abstract'].strip() != '' and row['Post-Abstract'].strip() != '':
        return 'Has Both Pre- and Post-Abstract'
    else:
        return 'Has Neither'
sales['Abstract Length'] = sales['Abstract'].str.strip().str.len()
sales['Pre- and Post Abstract'] = sales.apply(catg,axis=1)
sales['Taxonomy Term Count'] = sales.apply(taxo_count,axis=1)
sales_abs = sales.groupby(['Pre- and Post Abstract'],as_index=False).mean(); sales_abs['Avg. Sale Price'] = sales_abs['Sales Amount']/sales_abs['Sales Units']
sales_abs = sales_abs[['Pre- and Post Abstract','Sales Amount','Sales Units','Avg. Sale Price']]

#%% plotting
sales_abs.plot(x=sales_abs['Pre- and Post Abstract'],y=['Avg. Sale Price'],kind='barh')
sales_means_cpy.plot(x='Copyright Holder', y='Avg. Sale Price', kind='barh',figsize=(8,16))
bins = list(range(0,2000,20))
t = pd.cut(sales['Abstract Length'],bins)
test = sales.groupby(t).mean(); test.plot(x=test.index,y='Avg. Sale Price',kind='barh',rot=0,figsize=(8,24))
sales[sales['Taxonomy Term Count'] > 0].groupby('Taxonomy Term Count',as_index=False).mean().plot(x='Taxonomy Term Count',y='Avg. Sale Price',kind='hist',bins=30,figsize=(12,8))


#%% gathering all 130s and 132s
tite_dict = dict(zip(products['Product ID'],products['Title']))
avail_tite_dict = dict(zip(products['Availability ID'],products['Title']))
pt_dict = dict(zip(products['Product ID'],products['Product Type']))

pt130s = products[(products['Product Type'] == 130) & (products['Status'].isin(['C','F'])) & (products['Business Group'] == 'Higher Education')]
bases = list(set(pt130s['Product ID']))
supplements = relationships[relationships['Association Type'] == 'Supplemented By']
supported = relationships[ (relationships['Association Type'] == 'Supplemented By') & (relationships['Base'].isin(supplements['Base'])==False)]
reverse = relationships[relationships['Association Type'] == 'Must be Used With']
reverse = reverse[reverse['Target'].isin(supplements['Base']) == False]


#%% 
main_cases = pd.DataFrame(columns=['Main Case','Case Title','Relationships'])
supp_dict = {}
for prod in bases:
    tmp = supplements[supplements['Base'] == prod]
    tite = tite_dict[prod]
    tmp_dict = {}
    if len(list(tmp['Target'])) > 2:
        tmp_dict['Main Case'] = prod
        tmp_dict['Case Title'] = tite
        l = []
        targ_sorted = sorted(list(tmp['Target']))
        for targ in targ_sorted:
            if targ in pt_dict and (pt_dict[targ] == 130 or pt_dict[targ] == 132):
                if targ in tite_dict.keys():
                    l.append(targ + ' ' + tite_dict[targ])
                elif targ in avail_tite_dict.keys():
                    l.append(targ + ' ' + avail_tite_dict[targ])
                else:
                    l.append(targ)
        chars = [' '.join(x.split(' ')[1:]) for x in l]
        nums = [' '.join(x.split(' ')[:1]) for x in l]
        tup = sorted(list(zip(chars, nums)))
        l = [' '.join([y,x]) for x, y in tup]
        tmp_dict['Relationships'] = '\n'.join(l)
        
        main_cases = main_cases.append(tmp_dict,ignore_index=True)
    
supp_dict['UV5984'] = list(supplements[supplements['Base'] == 'UV5984']['Target'])
main_cases = main_cases[main_cases['Relationships'] != '']
main_cases.to_excel('/Users/nathaniel.hunt/Desktop/test.xlsx',index=False)


#%% set siblings for Core Curriculum products 

def cc_siblingizer(cc_list):
    siblings = pd.DataFrame(columns=['Product/Availability','Association Type','Target','Change Type'])
    for cc in cc_list:
        if cc not in siblings['Product/Availability']:
            for other_cc in [x for x in cc_list if x != cc]:
                pairs = tuple(zip(siblings['Product/Availability'],siblings['Target']))
                if (cc, other_cc) not in pairs and (other_cc, cc) not in pairs:
                    tmp_dict = {}
                    tmp_dict['Product/Availability'] = cc
                    tmp_dict['Association Type'] = 'Is Sibling Of'
                    tmp_dict['Target'] = other_cc
                    tmp_dict['Change Type'] = 'Add'
                    siblings = siblings.append(tmp_dict,ignore_index=True)
    siblings = siblings.drop_duplicates(keep='first')
    return siblings
    
marketing_CCs = ['8153','8191','8176','8171','8171','8167','8145','8182','8219','8197','8140','8158','8208','8208','8149','8213','8224','8186']

siblings = cc_siblingizer(marketing_CCs)
siblings.to_csv('/Users/nathaniel.hunt/Desktop/cc sibling test.csv',index=False) 
    
# def siblingizer(dictionary):
#     siblings = pd.DataFrame(columns=['Product/Availability','Association Type','Target','Change Type'])
#     for a_case, list_of_supplements in dictionary.items():
#         for supplement in list_of_supplements:
#             if supplement not in siblings['Product/Availability']:
#                 for other_supplement in [x for x in list_of_supplements if x != supplement]:
#                     pairs = tuple(zip(siblings['Product/Availability'],siblings['Target']))
#                     if (supplement, other_supplement) not in pairs and (other_supplement, supplement) not in pairs:
#                         tmp_dict = {}
#                         tmp_dict['Product/Availability'] = supplement
#                         tmp_dict['Association Type'] = 'Is Sibling Of'
#                         tmp_dict['Target'] = x
#                         tmp_dict['Change Type'] = 'Add'
#                         siblings = siblings.append(tmp_dict,ignore_index=True)
#     siblings = siblings.drop_duplicates(keep='first')
#     return siblings