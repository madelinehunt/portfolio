# -*- coding: utf-8 -*-
'''
A script which ingests PIM product data, and replaces taxonomy terms that are
notated as "orphaned" with taxonomy terms that are more valid--based on a list
of taxonomy terms given to me by Daniel Becker. 
'''

#%% defines terms to replace directly, and terms that need lookup
import pandas as pd # this script requires the Pandas data analysis library
import os

replacements = pd.read_excel('/Users/nathaniel.hunt/Desktop/alfresco_extract-taxonomy-data_DBcomments.xlsx',sheetname='Term Substitions')
bad_term_ids = [] #list to hold terms needing replacement (that have term IDs)
term_ids_to_lookup = [] #list to hold terms needing replacement (that *don't* have term IDs)

#sorts and cleanses term strings into the two lists
for term in replacements['Unknown Term (Missing From Synaptica)']:
    if '(' in term and 'ID' not in term:
        t = term.strip(')').strip('(').split(' ')[-1].strip('(')
        bad_term_ids.append(t)
    else:
        term_ids_to_lookup.append(term)

lookup_dict = {j:i for (i, j) in [[a,b] for a,b in [x.split('/') for x in term_ids_to_lookup]]} #creates a dict to prepare for searching for IDs for terms that don't have them
term_ids_to_lookup = [x.strip(' (No ID)') if 'No ID' in x else x for x in term_ids_to_lookup] #removes Daniel's notes from the term string

#%% imports data from PIM
from pim_etl import pim_etl
a = pim_etl.io()
products = a['products'].fillna('')
relationships = a['relationships'].fillna('')
taxonomy = a['taxonomy'].fillna('')

#%% check to make sure we're not missing any terms, by looking them up from taxonomy DataFrame
for term, subject in lookup_dict.items():
    taxo_select = taxonomy[
        (taxonomy['Category'] == subject) &
        (taxonomy['Term'] == term)
        ]
    try:
        t = str(taxo_select['Term ID'].iloc[0])
        if t != 0 and t != '0':
            bad_term_ids.append(str(t))
    except:
        print(taxo_select['Term ID'])

#%% 
replace_term_dict = dict(zip(taxonomy['Term ID'].astype(str),taxonomy['Term'])) #creates a taxo dict of terms
replace_subj_dict = dict(zip(taxonomy['Term ID'].astype(str),taxonomy['Category'])) #creates a taxo dict of categories

subs = {} #looks up the replacements that Daniel defined
for term in bad_term_ids:
    select = replacements[replacements['Unknown Term (Missing From Synaptica)'].str.contains(term)]
    subs[str(term)] = str(select['Replacement Term ID'].iloc[0])
    
subs_no_ids = {} #looks up the replacements that Daniel defined (terms without IDs)
for term in term_ids_to_lookup:
    select = replacements[replacements['Unknown Term (Missing From Synaptica)'].str.contains(term)]
    subs_no_ids[str(term)] = str(select['Replacement Term ID'].iloc[0])

replace_with = {} #creates a dict of old term to new term for replacing them
for old, new in subs.items():
    new_term = '/'.join([replace_subj_dict[new],replace_term_dict[new]])
    old_term = '/'.join([replace_subj_dict[old],replace_term_dict[old]])
    replace_with[old_term] = new_term
    
replace_with_no_ids = {} #creates a dict of old term to new term for replacing them (terms without IDs)
for i, v in subs_no_ids.items():
    replace_with_no_ids[i] = '/'.join([replace_subj_dict[v],replace_term_dict[v]])

#%%

#defines a vectorized function to perform string manipulation and replace old terms with new ones
def multi_replace(row,term_dict):
    remove_terms = ['Geography/Australia & Oceania']
    t = row['Taxonomy'].split('^')
    replace = [x for x in list(term_dict.keys()) if x in t]
    replacements = [term_dict[x] for x in replace]
    clean_list = [x for x in t if x not in replace and x not in remove_terms]
    clean_list += replacements
    clean_str = '^'.join(list(set(clean_list)))
    return pd.Series({'Product/Availability': row['Product ID'], 'pimCategories': clean_str})

#defines a function to take in all product data and the replacement dict, and apply multi_replace to each row
def substitute(term_dict, df):
    ret_df = pd.DataFrame(columns=['Product/Availability','pimCategories'])
    for old_term, new_term in term_dict.items():
        select = df[df['Taxonomy'].str.contains(old_term)]
        if len(select) >= 1:
            replacement_series = select.apply(multi_replace, axis=1, args=(term_dict,))
            ret_df = ret_df.append(replacement_series,ignore_index=True)
        else:
            print(old_term)
    return ret_df


#%%

# run this line to get data for all taxo terms--including ones that don't have IDs:
replace_with.update(replace_with_no_ids)

both = substitute(replace_with,products).drop_duplicates(keep='first')

#%% export and QA testing
remove_list = ['0171CF','0174CF','118028','118029','118031','118052','118702','118703','118704','218001',
'218016','218037','218701','218713','318001','318007','318022','318042','318043','318049',
'318065','318066','318076','318092','318093','318098','318099','418023','418036','418043',
'418050','418051','418053','518001','518002','518034','518044','518050','518053','518054',
'518055','518056','518061','518065','518066','518070','518072','616037','618012','618016',
'618019','618020','618032','618036','618704','718002','718010','718017','718018','718019',
'718022','718024','718026','718031','718032','718033','718034','718403','718430','718441',
'718442','718456','718457','7892TA','7892TC','818031','818044','818056','818063','818069',
'818076','8690','8691','8692','8730','8731','8732','8733','8734','8735','8736','8737','8738',
'8739','8740','918017','918018','918019','A189A','A189B','B5902','B5903','B5904','BG1706','BH843',
'BH844','BH845','BH860','BH861','BH863','BH866','BH869','BH870','BH872','BH873','BH875','BR1801',
'CMR668','CU190','E572B','E633','E633TN','E637','E637TN','E643','E643TN','ES1771','ES1778','GS90',
'H001I4','H001J4','H0024X','H002CV','H002EA','H00309','H0033M','H00343','H003C8','H003LT','H003ME',
'H003PS','H003VF','H003X7','H003XZ','H0040E','H0041X','H00437','H00450','H004G2','H006H0','H006H3',
'H006KC','H006QU','H006ZX','H0071P','H0077Y','H0084T','H0084V','H0085F','H0085J','H008FZ','H008MV',
'H008VL','H009HA','H009L4','H009Y6','H00AC5','H00AGU','H00AIF','H00AL9','H00AOV','H00AXN','H00B2K',
'H00HYA','H00KHW','H00LM8','H00X5O','H011OC','H03S8W','H03VD5','H03XYN','H03Y1G','H03Y8Y','H03YHK',
'H03YQ1','H03YV6','H03YXN','H03Z0E','H03Z1P','H03Z4R','H03ZAD','H03ZAW','H03ZGG','H03ZHC','H03ZO9',
'H03ZPC','H03ZRN','H03ZRS','H03ZXI','H03ZZ3','H04017','H0403F','H0403Q','H040AK','H040J9','H040KZ',
'H040MZ','H0412S','H0413F','H0413I','H041ER','H041ET','H041JO','H041L3','H041ML','H041V7','H042A7',
'H042H2','H042HR','H042KU','H042OE','H042QH','H042R3','H042RS','H042UG','H042V3','H0436U','H043CP',
'H043EX','H043IH','H043J8','H043LD','H043OT','H043QN','H043U2','H043X7','H0440X','H0442O','H0448F',
'H044BF','H044GJ','H044I3','H044KG','HEC191','HEC192','HEC197','HEC198','HEC199','HEC200','HEC201',
'HK1115','HK1116','HK1117','HK1118','HK1119','HK1120','IIR189','IMB653','IMB654','IMB655','IMB657',
'IMB658','IMB659','IMD883','IMD884','IMD885','IMD886','IN1393','IN1394','IN1395','IN1396','IN1399',
'IN1400','IN1401','IN1406','IN1407','IN1411','IN1412','IN1415','IN1416','IN1417','IN1418','IN1419',
'IN1420','IN1421','IN1422','IN1423','IN1424','IN1425','IN1426','ISB094','ISB095','KE1016','KE1017',
'KE1018','KE1019','KE1020','KE1021','KE1022','KE1030','KE1031','KE1032','KE1033','KE1042','KE1043',
'KE1044','KE1045','MH0043','MH0044','MH0045','MH0046','MH0049','MH0050','MH0052','MH0053','MH0054',
'MH0055','MH0056','MH043T','MH044T','MH045T','MH046T','MH049T','MH050T','MH052T','MH053T','MH054T',
'MH055T','MH056T','NA0507','NA0508','NTU133','NTU134','NTU148','NTU149','NTU154','NTU155','R1706K',
'R1801A','R1801E','ROT354','ROT356','SM278','SMR634','TB0509','TB0510','UV1282','UV7329','UV7330',
'UV7331','UV7341','UV7342','UV7367','UV7368','UV7391','UV7392','W07404','W14014','W14463','W16284',
'W16922','W16923','W17568','W17569','W17570','W17571','W17574','W17575','W17582','W17583','W17584',
'W17587','W17588','W17589','W17590','W17593','W17604','W17605','W17606','W17607','W17623','W17624',
'W17625','W17633','W17634','W17642','W17643','W17652','W17657','W17658','W17660','W17661','W17702',
'W17703','W17704','W17705','W17730','W17731','W17748','W17749','W17754','W17755','W17756','W17764',
'W17765','W17767','W17779','W17780','W17794']

qa = input('Is this for QA testing?  > ')
if qa.lower() == 'y' or qa.lower() == 'yes':
    both = both[~both['Product/Availability'].isin(remove_list)]

both.to_csv('/Users/nathaniel.hunt/Desktop/Daniel taxonomy update.csv',index=False)
