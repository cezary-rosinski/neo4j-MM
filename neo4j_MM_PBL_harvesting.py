#Notes --> https://docs.google.com/document/d/1mQIW65vWUZ6FY6vhfTfOpmA8O3OLnsUzEZzXLVZhE4U/edit?pli=1#heading=h.teu1qln8tb46
#%% import
import cx_Oracle
import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
sys.path.insert(1, 'C:/Users/Cezary/Documents/Global-trajectories-of-Czech-Literature')
from marc_functions import read_mrk, mrk_to_df
from pbl_credentials import pbl_user, pbl_password
from my_functions import gsheet_to_df#, marc_parser_dict_for_field
import pandas as pd
import numpy as np
from tqdm import tqdm
import regex as re
from SPARQLWrapper import SPARQLWrapper, JSON
from urllib.error import HTTPError, URLError
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import pickle
import requests
sys.path.insert(1, 'C:/Users/Cezary/Documents/SPUB-project')
from geonames_accounts import geonames_users
import random
from glob import glob
import spacy
from collections import ChainMap
import gender_guesser.detector as gender

nlp = spacy.load('pl_core_news_lg')

#%% def

def wikidata_simple_dict_resp(results):
    results = results['results']['bindings']
    dd = defaultdict(list)
    for d in results:
        for key, value in d.items():
            dd[key].append(value)
    dd = {k:set([tuple(e.items()) for e in v]) for k,v in dd.items()}
    dd = {k:list([dict((x,y) for x,y in e) for e in v]) for k,v in dd.items()}
    return dd

def query_wikidata_person_with_viaf(viaf):
    # viaf_id = 49338782
    viaf_id = re.findall('\d+', viaf)[0]
    user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql", agent=user_agent)
    sparql.setQuery(f"""PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                SELECT distinct ?author WHERE {{ 
                  ?author wdt:P214 "{viaf_id}" ;
                SERVICE wikibase:label {{ bd:serviceParam wikibase:language "pl". }}}}""")
    sparql.setReturnFormat(JSON)
    while True:
        try:
            results = sparql.query().convert()
            break
        except HTTPError:
            time.sleep(2)
        except URLError:
            time.sleep(5)
    results = wikidata_simple_dict_resp(results)  
    viafy_wiki[viaf] = results
    return results

def get_wikidata_info(wikidata_url):
# for wikidata_url in tqdm(wikidata_ids[1000:1050]):
    # wikidata_url = wikidata_ids[0]
    wikidata_id = re.findall('Q.+', wikidata_url)[0]
    result = requests.get(f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json').json()
    claims = ['P21', 'P19', 'P20', 'P569', 'P570']
    temp_dict = {}
    for claim in claims:
        temp_dict.setdefault(claim, None)
        try:
            temp_dict[claim] = result.get('entities').get(wikidata_id).get('claims').get(claim)[0].get('mainsnak').get('datavalue').get('value').get('id', result.get('entities').get(wikidata_id).get('claims').get(claim)[0].get('mainsnak').get('datavalue').get('value').get('time'))
        except (AttributeError, TypeError):
            pass
    wikidata_response[wikidata_url] = temp_dict

def get_wikidata_label(wikidata_id, list_of_languages):
    if not wikidata_id.startswith('Q'):
        wikidata_id = f'Q{wikidata_id}'
    r = requests.get(f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json').json()
    old_wikidata_id = wikidata_id
    if wikidata_id != list(r.get('entities').keys())[0]:
        wikidata_id = list(r.get('entities').keys())[0]
    record_languages = set(r.get('entities').get(wikidata_id).get('labels').keys())
    for language in list_of_languages:
        if language in record_languages:
            return (old_wikidata_id, wikidata_id, r.get('entities').get(wikidata_id).get('labels').get(language).get('value'))
        else:
            return (old_wikidata_id, wikidata_id, r.get('entities').get(wikidata_id).get('labels').get(list(record_languages)[0]).get('value'))

#%%PBL connection
# cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\Cezary\Desktop\sqldeveloper\instantclient_19_6")
dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user=pbl_user, password=pbl_password, dsn=dsn_tns, encoding='windows-1250')

#%% PBL queries
pbl_query = """select * from pbl_tworcy"""
pbl_tworcy = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)
pbl_tworcy = pbl_tworcy[['TW_TWORCA_ID', 'TW_NAZWISKO', 'TW_IMIE', 'TW_DZ_DZIAL_ID', 'TW_UWAGI']]
print('pbl_tworcy')
pbl_query = """select * from pbl_autorzy"""
pbl_autorzy = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)
pbl_autorzy = pbl_autorzy[['AM_AUTOR_ID', 'AM_NAZWISKO', 'AM_IMIE', 'AM_KRYPTONIM']]
print('pbl_autorzy')
pbl_query = """select * from IBL_OWNER.pbl_zapisy_tworcy"""
pbl_zapisy_tworcy = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)
print('pbl_zapisy_tworcy')
pbl_query = """select * from pbl_zapisy_autorzy"""
pbl_zapisy_autorzy = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)
print('pbl_zapisy_autorzy')
pbl_query = """select * from IBL_OWNER.pbl_zrodla"""
pbl_zrodla = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)
print('pbl_zrodla')
pbl_query = """select * from IBL_OWNER.pbl_zapisy"""
pbl_zapisy = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)
pbl_zapisy = pbl_zapisy[['ZA_ZAPIS_ID', 'ZA_TYPE', 'ZA_RO_ROK', 'ZA_RZ_RODZAJ1_ID', 'ZA_RZ_RODZAJ2_ID', 'ZA_DZ_DZIAL1_ID',
       'ZA_DZ_DZIAL2_ID', 'ZA_TYTUL', 'ZA_OPIS_WSPOLTWORCOW', 'ZA_MIEJSCE_WYDANIA', 'ZA_WY_WYDAWNICTWO_ID', 'ZA_ROK_WYDANIA', 'ZA_OPIS_FIZYCZNY_KSIAZKI', 'ZA_ZR_ZRODLO_ID', 'ZA_ZRODLO_ROK', 'ZA_ZRODLO_NR', 'ZA_ZRODLO_STR']]
#następnym razem zastosować select z.za_zapis_id, z.za_type, z.za_ro_rok, z.za_rz_rodzaj1_id, z.za_rz_rodzaj2_id, z.za_tytul, z.za_rok_wydania, z.za_opis_fizyczny_ksiazki, z.za_zr_zrodlo_id, z.za_zrodlo_rok, z.za_zrodlo_nr, z.za_zrodlo_str from IBL_OWNER.pbl_zapisy z
print('pbl_zapisy')
pbl_query = """select * from IBL_OWNER.pbl_rodzaje_zapisow"""
pbl_rodzaje_zapisow = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)
print('pbl_rodzaje_zapisow')
pbl_query = """select * from IBL_OWNER.pbl_wydawnictwa"""
pbl_wydawnictwa = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)
print('pbl_wydawnictwa')
pbl_query = """select * from IBL_OWNER.pbl_zapisy_wydawnictwa"""
pbl_zapisy_wydawnictwa = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)
print('pbl_zapisy_wydawnictwa')
pbl_query = """select * from pbl_zapisy z
where z.za_type like 'IR'
and z.za_rz_rodzaj1_id = 301"""
pbl_nagrody = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)
print('pbl_nagrody')
dfs = {'pbl_rodzaje_zapisow': pbl_rodzaje_zapisow, 'pbl_tworcy': pbl_tworcy, 'pbl_autorzy': pbl_autorzy, 'pbl_zapisy_tworcy': pbl_zapisy_tworcy, 'pbl_wydawnictwa': pbl_wydawnictwa, 'pbl_zapisy_wydawnictwa': pbl_zapisy_wydawnictwa, 'pbl_zapisy': pbl_zapisy, 'pbl_zrodla': pbl_zrodla, 'pbl_nagrody': pbl_nagrody, 'pbl_zapisy_autorzy': pbl_zapisy_autorzy}

for k,v in tqdm(dfs.items()):
    test_dict = v.groupby(v.columns.values[0]).apply(lambda x: x.to_dict('records')).to_dict()
    with open(f'{k}.pickle', 'wb') as handle:
        pickle.dump(test_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
        
#%% Data
files = [f for f in glob('*.pickle', recursive=True)]

for file in tqdm(files):
    with open(file, 'rb') as handle:
        name = file.split('.')[0]
        exec(f'{name}=pickle.load(handle)')
        exec(f'{name}=pd.concat([pd.DataFrame(e) for e in {name}.values()])')

#%%Entities
###Person --> na razie tu tylko twórcy, docelowo mają być wszystkie osoby

# old_persons_file = pd.read_excel(r"C:\Users\Cezary\Downloads\kartoteka osób - 11.10.2018 - gotowe_po_konfliktach.xlsx")
# tw_tworca_id_list = [int(e) for e in old_persons_file['tw_tworca_id'].drop_duplicates().dropna().to_list()]

tworcy_autorzy = pd.read_excel(r"C:\Users\Cezary\Downloads\kartoteka osób - 11.10.2018 - gotowe_po_konfliktach.xlsx")[['TW_TWORCA_ID', 'AM_AUTOR_ID']]

tworcy_autorzy = tworcy_autorzy.loc[(tworcy_autorzy['TW_TWORCA_ID'].notnull()) &
                                    (tworcy_autorzy['AM_AUTOR_ID'].notnull())]

autorzy_tworcy_dict = dict(zip(tworcy_autorzy['AM_AUTOR_ID'], tworcy_autorzy['TW_TWORCA_ID']))

ksiazki_debiutantow = pd.read_excel(r"D:\IBL\Tabele dla MM\2. książki debiutantów.xlsx")
debiutanci = ksiazki_debiutantow['id twórcy'].drop_duplicates().to_list()
debiutanci_update_mm = gsheet_to_df('1b-RAmZuFQgMSnd5yI6EZM2kRnL7vjAY1Yj3iQA_JPnI', 'ID')

# tw_tworca_id_new = pbl_query.loc[~pbl_query['TW_TWORCA_ID'].isin(tw_tworca_id_list)][['TW_TWORCA_ID', 'TW_NAZWISKO', 'TW_IMIE']]
# tw_tworca_id_new.to_excel('test.xlsx', index=False)

mapowanie_bn_pbl = ['1_Bhwzo0xu4yTn8tF0ZNAZq9iIAqIxfcrjeLVCm_mggM', '1L-7Zv9EyLr5FeCIY_s90rT5Hz6DjAScCx6NxfuHvoEQ', '1cEz73dGN2r2-TTc702yne9tKfH9PQ6UyAJ2zBSV6Jb0']
#UWAGA --> ogarnąć, że czy_ten_sam ma więcej akceptowalnych wartości
df = pd.DataFrame()
for file in tqdm(mapowanie_bn_pbl):
    temp_df = gsheet_to_df(file, 'pbl_bn')
    temp_df = temp_df.loc[temp_df['czy_ten_sam'].isin(['tak', 'raczej tak'])]
    df = pd.concat([df, temp_df])
    
pbl_viaf = df.copy()[['pbl_id', 'viaf']].rename(columns={'pbl_id': 'TW_TWORCA_ID'})
pbl_viaf['TW_TWORCA_ID'] = pbl_viaf['TW_TWORCA_ID'].astype(np.int64)

pbl_persons = pd.merge(pbl_tworcy, pbl_viaf, on='TW_TWORCA_ID', how='left')
viafy = pbl_persons['viaf'].drop_duplicates().dropna().to_list()

pbl_bn_names = {k:[e for e in v.split('|') if e] for k,v in dict(zip(temp_df['pbl_id'].to_list(), temp_df['BN_name'].to_list())).items()}
bn_names_pbl = {}
for k,v in pbl_bn_names.items():
    for e in v:
        bn_names_pbl.setdefault(e,k)
        
pbl_persons = pbl_persons.groupby('TW_TWORCA_ID').head(1)
pbl_persons['creator'] = True

pbl_persons = pd.merge(pbl_persons, tworcy_autorzy, on='TW_TWORCA_ID', how='outer')

autorzy_merged = [int(e) for e in pbl_persons['AM_AUTOR_ID'].dropna().drop_duplicates().to_list()]
autorzy_to_be_merged = pbl_autorzy.loc[~pbl_autorzy['AM_AUTOR_ID'].isin(autorzy_merged)].rename(columns={'AM_NAZWISKO': 'TW_NAZWISKO', 'AM_IMIE': 'TW_IMIE'})
autorzy_to_be_merged['creator'] = False

pbl_persons = pd.concat([pbl_persons, autorzy_to_be_merged])


# path = r"F:\Cezary\Documents\IBL\BN\bn_all\2023-01-23/"
# files = [f for f in glob(path + '*.mrk', recursive=True)]

# result = {}
# for file in tqdm(files):
#     # file = files[-1]
#     file = read_mrk(file)
#     for dictionary in file:
#         # dictionary = file[0]
#         if '100' in dictionary:
#             if '$a' in dictionary.get('100')[0]:
#                 try:
#                     author = dict(ChainMap(*marc_parser_dict_for_field(dictionary.get('100')[0], '\\$')))
#                     author = f"{author.get('$a')} {author.get('$d')}" if all(e in author for e in ['$a', '$d']) else author.get('$a')
#                     if author:
#                         author = author[:-1] if author[-1] == '.' else author
#                         if author in bn_names_pbl:
#                             if '008' in dictionary:
#                                 year = dictionary.get('008')[0][7:11]
#                                 if year.isnumeric():
#                                     result.setdefault(bn_names_pbl.get(author), set()).add(int(year))
#                 except IndexError:
#                     pass
                
# with open('person_bn_publishing_years.p', 'wb') as fp:
#     pickle.dump(result, fp, protocol=pickle.HIGHEST_PROTOCOL)
    
with open('person_bn_publishing_years.p', 'rb') as fp:
    result_bn_years = pickle.load(fp)

result_bn_years = {k:min(v) for k,v in result_bn_years.items()}
# viafy = viafy[:100]

# viafy_wiki = {}
# with ThreadPoolExecutor() as executor:
#     list(tqdm(executor.map(query_wikidata_person_with_viaf,viafy), total=len(viafy)))
# viafy_wiki = {k:v['author'][0]['value'] for k,v in viafy_wiki.items() if v}

# with open('viaf_wikidata_match.p', 'wb') as fp:
#     pickle.dump(viafy_wiki, fp, protocol=pickle.HIGHEST_PROTOCOL)
    
with open('viaf_wikidata_match.p', 'rb') as fp:
    viafy_wiki = pickle.load(fp)

pbl_persons['wikidata'] = pbl_persons['viaf'].apply(lambda x: viafy_wiki.get(x))

wikidata_ids = list(viafy_wiki.values())

# wikidata_response = {}
# with ThreadPoolExecutor() as executor:
#     list(tqdm(executor.map(get_wikidata_info, wikidata_ids), total=len(wikidata_ids)))
    
# with open('wikidata_response.p', 'wb') as fp:
#     pickle.dump(wikidata_response, fp, protocol=pickle.HIGHEST_PROTOCOL)
    
with open('wikidata_response.p', 'rb') as fp:
    wikidata_response = pickle.load(fp)

labels_dict = {'P21': 'gender', 'P569': 'born', 'P570': 'died', 'P19': 'birthPlace', 'P20': 'deathPlace'}

for label in tqdm(labels_dict):
    pbl_persons[labels_dict.get(label)] = pbl_persons['wikidata'].apply(lambda x: wikidata_response.get(x).get(label) if x in wikidata_response else x)
    
pbl_persons['debutant'] = pbl_persons['TW_TWORCA_ID'].apply(lambda x: x in debiutanci)
pbl_persons = pbl_persons[['TW_TWORCA_ID', 'TW_IMIE', 'TW_NAZWISKO', 'gender', 'born', 'died', 'birthPlace', 'deathPlace', 'debutant', 'creator', 'AM_AUTOR_ID', 'AM_KRYPTONIM']].rename(columns={'TW_TWORCA_ID': 'personId', 'TW_IMIE': 'name', 'TW_NAZWISKO': 'surname'})

pbl_persons.drop_duplicates(inplace=True)

wikidata_ids = set(pbl_persons['gender'].drop_duplicates().dropna().to_list() + pbl_persons['birthPlace'].drop_duplicates().dropna().to_list() + pbl_persons['deathPlace'].drop_duplicates().dropna().to_list())

# with ThreadPoolExecutor() as executor:
#     wikidata_response = list(tqdm(executor.map(lambda p: get_wikidata_label(p, ['pl', 'en']), wikidata_ids)))
# wikidata_labels = dict([(f'Q{a[1:]}',c) for a,b,c in wikidata_response])

# with open('wikidata_labels.p', 'wb') as fp:
#     pickle.dump(wikidata_labels, fp, protocol=pickle.HIGHEST_PROTOCOL)

with open('wikidata_labels.p', 'rb') as fp:
    wikidata_labels = pickle.load(fp)

for column in ['gender', 'birthPlace', 'deathPlace']:
    pbl_persons[column] = pbl_persons[column].apply(lambda x: wikidata_labels.get(x))
    
pbl_persons['label'] = pbl_persons['name'] + ' ' + pbl_persons['surname']
pbl_persons['label'] = pbl_persons[['label', 'AM_KRYPTONIM']].apply(lambda x: x['AM_KRYPTONIM'] if pd.isnull(x['label']) else x['label'], axis=1)

gwiazdkowicze = pbl_tworcy.loc[pbl_tworcy['TW_UWAGI'].str.strip() == 'GWIAZDKOWICZ']['TW_TWORCA_ID'].to_list()

pbl_persons['featured'] = pbl_persons['personId'].apply(lambda x: True if x in gwiazdkowicze else False)

pbl_persons_dzialy = dict(zip(pbl_tworcy['TW_TWORCA_ID'].to_list(), pbl_tworcy['TW_DZ_DZIAL_ID'].to_list()))
pbl_query = """select * from pbl_dzialy"""
pbl_dzialy = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)
pbl_dzialy_dict = dict(zip(pbl_dzialy['DZ_DZIAL_ID'].to_list(), pbl_dzialy['DZ_NAZWA'].to_list()))
pbl_persons_dzialy = {k:pbl_dzialy_dict.get(v) for k,v in pbl_persons_dzialy.items()}
pbl_persons['natLit'] = pbl_persons['personId'].apply(lambda x: pbl_persons_dzialy.get(x))

d = gender.Detector()
pbl_persons['gender'] = pbl_persons[['gender', 'name']].apply(lambda x: 'male' if x['gender'] and x['gender'] == 'mężczyzna' else 'female' if x['gender'] and x['gender'] in ['femina cisgenera', 'kobieta'] else 'other' if x['gender'] and x['gender'] == 'niebinarność' else d.get_gender(x['name']), axis=1)

# pbl_persons['debutant_new'] = pbl_persons[['personId', 'debutant']].apply(lambda x: False if x['personId'] in result_bn_years and result_bn_years.get(x['personId']) < 1990 else True if x['personId'] in result_bn_years and result_bn_years.get(x['personId']) >= 1990 else x['debutant'], axis=1)

# test = pbl_persons[['personId', 'debutant', 'debutant_new', 'label']]
# test['test'] = test[['debutant', 'debutant_new']].apply(lambda x: x['debutant'] == x['debutant_new'], axis=1)
# test = test.loc[test['test'] == False]

# pbl_persons['personId'] = pbl_persons[['personId', 'AM_AUTOR_ID']].apply(lambda x: f"person_1_{int(x['personId'])}" if pd.notnull(x['personId']) else f"person_2_{int(x['AM_AUTOR_ID'])}", axis=1)
#muszę poczekać z zapisem, żeby ustalić, kto ma secondary
# pbl_persons.to_csv('entities_person.csv', index=False)


# w pierwszej kolejności w person dać tylko twórców, dać im stałe identyfikatory, pobrać z wiki dodatkowe informacje
# jak zdefiniować debiutantów? --> po 15.04 dane od PH z retro

###Journal

pbl_journals = pbl_zrodla.copy()[['ZR_ZRODLO_ID', 'ZR_TYTUL', 'ZR_MIEJSCE_WYD']].rename(columns={'ZR_ZRODLO_ID': 'journalId', 'ZR_TYTUL': 'name'})
pbl_journals['journalId'] = pbl_journals['journalId'].apply(lambda x: f"journal_{x}")
pbl_journals_with_place = pbl_journals.copy()
pbl_journals.drop(columns='ZR_MIEJSCE_WYD', inplace=True)

pbl_journals.to_csv('entities_journal.csv', index=False)
# pbl_journals.to_excel('entities_journal.xlsx', index=False)

###JournalArticle

pbl_journal_articles = pbl_zapisy.loc[pbl_zapisy['ZA_TYPE'].isin(['IZA', 'PU'])]
pbl_journal_articles = pd.merge(pbl_journal_articles, pbl_rodzaje_zapisow, left_on='ZA_RZ_RODZAJ1_ID', right_on='RZ_RODZAJ_ID', how='left')
pbl_journal_articles = pd.merge(pbl_journal_articles, pbl_zrodla, left_on='ZA_ZR_ZRODLO_ID', right_on='ZR_ZRODLO_ID', how='left')

pbl_journal_articles_dzialy = dict(zip(pbl_journal_articles['ZA_ZAPIS_ID'].to_list(),  pbl_journal_articles['ZA_DZ_DZIAL1_ID'].to_list()))

pbl_query = """select * from pbl_dzialy"""
pbl_dzialy = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)
pbl_dzialy_dict = dict(zip(pbl_dzialy['DZ_DZIAL_ID'].to_list(), [e[:-4] if 'Hasła osobowe (literatura polska)' in e else e for e in pbl_dzialy['DZ_NAZWA'].to_list()]))
pbl_journal_articles_dzialy = {k:pbl_dzialy_dict.get(v) for k,v in pbl_journal_articles_dzialy.items()}
pbl_journal_articles['natLit'] = pbl_journal_articles['ZA_ZAPIS_ID'].apply(lambda x: pbl_journal_articles_dzialy.get(x))

pbl_journal_articles = pbl_journal_articles.copy()[['ZA_ZAPIS_ID', 'ZA_TYTUL', 'RZ_NAZWA', 'ZA_ZRODLO_NR', 'ZA_ZRODLO_STR', 'ZA_ZRODLO_ROK', 'ZA_TYPE', 'natLit', 'ZA_OPIS_WSPOLTWORCOW']].rename(columns={'ZA_ZAPIS_ID': 'jArticleId', 'ZA_TYTUL': 'title', 'RZ_NAZWA': 'genre', 'ZA_ZRODLO_NR': 'issue', 'ZA_ZRODLO_ROK': 'year', 'ZA_ZRODLO_STR': 'numberOfPages'})

pbl_journal_articles['translation'] = pbl_journal_articles['ZA_OPIS_WSPOLTWORCOW'].apply(lambda x: True if pd.notnull(x) and any(e in x.lower() for e in ['tł.', 'tłum.', 'przeł', 'przekł']) else False)

pbl_journal_articles['type'] = pbl_journal_articles['ZA_TYPE'].apply(lambda x: 'Literature' if x == 'PU' else 'Secondary')
pbl_journal_articles.drop(columns=['ZA_TYPE', 'ZA_OPIS_WSPOLTWORCOW'], inplace=True)
pbl_journal_articles['jArticleId'] = pbl_journal_articles['jArticleId'].apply(lambda x: f"journalarticle_{x}")

def count_pages(x):
    if pd.isnull(x):
        return None
    elif len(re.findall('\d+', x)) == 1:
        return 1
    elif len(re.findall('\d+-\d+', x)) == 1:
        y = [int(e) for e in re.findall('\d+-\d+', x)[0].split('-')]
        if y[-1] > y[0]:
            return y[-1] - y[0] + 1
        else:
            return int(str(y[0])[:-1] + str(y[-1])) - y[0] + 1
    elif re.findall('\d+', x) and ',' in x and '-' not in x:
        return len(re.findall('(?<![a-z] )\d+', x))
    elif re.findall('\d+', x) and ',' in x and '-' in x:
        return sum([ele[-1] - ele[0] + 1 for ele in [[int(el) for el in e.split('-')] for e in re.findall('\d+-\d+', x)]])      

pbl_journal_articles['numberOfPages'] = pbl_journal_articles['numberOfPages'].apply(lambda x: count_pages(x))

pbl_journal_articles.loc[pbl_journal_articles['jArticleId'] == 'journalarticle_1852232', 'year'] = 2008

pbl_journal_articles.to_csv('entities_journal_article.csv', index=False)
# pbl_journal_articles.to_excel('entities_journal_article.xlsx', index=False)

###Book

pbl_books = pbl_zapisy.loc[pbl_zapisy['ZA_TYPE'] == 'KS']
pbl_books = pd.merge(pbl_books, pbl_rodzaje_zapisow, left_on='ZA_RZ_RODZAJ1_ID', right_on='RZ_RODZAJ_ID', how='left')

pbl_books_dzialy = dict(zip(pbl_books['ZA_ZAPIS_ID'].to_list(),  pbl_books['ZA_DZ_DZIAL1_ID'].to_list()))

pbl_query = """select * from pbl_dzialy"""
pbl_dzialy = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)
pbl_dzialy_dict = dict(zip(pbl_dzialy['DZ_DZIAL_ID'].to_list(), [e[:-4] if 'Hasła osobowe (literatura polska)' in e else e for e in pbl_dzialy['DZ_NAZWA'].to_list()]))
pbl_books_dzialy = {k:pbl_dzialy_dict.get(v) for k,v in pbl_books_dzialy.items()}
pbl_books['natLit'] = pbl_books['ZA_ZAPIS_ID'].apply(lambda x: pbl_books_dzialy.get(x))

pbl_books = pbl_books.copy()[['ZA_ZAPIS_ID', 'ZA_TYTUL', 'ZA_RO_ROK', 'RZ_NAZWA', 'ZA_OPIS_FIZYCZNY_KSIAZKI', 'natLit', 'ZA_OPIS_WSPOLTWORCOW']].rename(columns={'ZA_ZAPIS_ID': 'bookId', 'ZA_TYTUL': 'title', 'ZA_RO_ROK': 'year', 'RZ_NAZWA': 'genre', 'ZA_OPIS_FIZYCZNY_KSIAZKI': 'numberOfPages'})

pbl_books['translation'] = pbl_books['ZA_OPIS_WSPOLTWORCOW'].apply(lambda x: True if pd.notnull(x) and any(e in x.lower() for e in ['tł.', 'tłum.', 'przeł', 'przekł']) else False)
pbl_books.drop(columns=['ZA_OPIS_WSPOLTWORCOW'], inplace=True)

pbl_books['bookId'] = pbl_books['bookId'].apply(lambda x: f"book_{x}")

def count_pages_books(x):
    if pd.isnull(x):
        return None
    else:
        try:
            return re.findall('\d+', x.split('s.')[0])[0]
        except: None

pbl_books['numberOfPages'] = pbl_books['numberOfPages'].apply(lambda x: count_pages_books(x))

pbl_books.to_csv('entities_book.csv', index=False)
# pbl_books.to_excel('entities_book.xlsx', index=False)

###Publisher
# pbl_publishers = pbl_query.copy()[['WY_WYDAWNICTWO_ID', 'WY_NAZWA', 'WY_MIASTO']].rename(columns={'WY_WYDAWNICTWO_ID': 'publisherId', 'WY_NAZWA': 'name', 'WY_MIASTO': 'locatedIn'})
pbl_publishers = pbl_wydawnictwa.copy()[['WY_WYDAWNICTWO_ID', 'WY_NAZWA', 'WY_MIASTO']].rename(columns={'WY_WYDAWNICTWO_ID': 'publisherId', 'WY_NAZWA': 'name'})

pbl_publishers['publisherId'] = pbl_publishers['publisherId'].apply(lambda x: f"publisher_{x}")
pbl_publishers_with_place = pbl_publishers.copy()
pbl_publishers.drop(columns='WY_MIASTO', inplace=True)

pbl_publishers.to_csv('entities_publisher.csv', index=False)
# pbl_publishers.to_excel('entities_publisher.xlsx', index=False)

###Location

kartoteka_miejsc_PBL = gsheet_to_df('1p6avLXYVk5M0kyWAF7zkVel1N7Nh4WH-ykPQN0tYK0c', 'Sheet1')
kartoteka_miejsc_PBL = kartoteka_miejsc_PBL.loc[kartoteka_miejsc_PBL['status INO'] != 'INO']
grouped = kartoteka_miejsc_PBL.groupby('query name')

pbl_location = pd.DataFrame()

for name, group in tqdm(grouped):
    if any(pd.notnull(e) for e in group['decyzja'].to_list()):
        test_df = group.loc[group['decyzja'] == 'tak']
    else:
        test_df = group.head(1)
    pbl_location = pd.concat([pbl_location, test_df])
    
pbl_location = pbl_location[['geonamesId', 'query name', 'countryName']].rename(columns={'query name': 'pblName', 'geonamesId': 'locationId', 'countryName': 'country'})

pbl_places = pbl_location['locationId'].drop_duplicates().dropna().to_list()

def harvest_geonames(geoname_id):
    user = random.choice(geonames_users)
    #w funkcję wpisać losowanie randomowego username
    try:
        r = requests.get(f'http://api.geonames.org/getJSON?geonameId={geoname_id}&username={user}').json()
        geonames_resp[geoname_id] = {k:v for k,v in r.items() if k in ['lat', 'lng', 'name', 'countryName']}
    except KeyError:
        harvest_geonames(geoname_id)

geonames_resp = {}
with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(harvest_geonames, pbl_places), total=len(pbl_places)))

while any(bool(geonames_resp.get(e)) == False for e in geonames_resp):
    places2 = {k for k,v in geonames_resp.items() if not v}

    with ThreadPoolExecutor() as executor:
        list(tqdm(executor.map(harvest_geonames, places2), total=len(places2)))

geonames_df = pd.DataFrame().from_dict(geonames_resp, orient='index').reset_index(drop=False, names='locationId').rename(columns={'name': 'city', 'geonamesId': 'locationId', 'countryName': 'country'})
geonames_df['coordinates'] = geonames_df.apply(lambda x: f"{x['lat']}, {x['lng']}", axis=1)
geonames_df.drop(columns=['lat', 'lng'], inplace=True)
geonames_df['locationId'] = geonames_df['locationId'].apply(lambda x: f"location_{x}")
pbl_location['locationId'] = pbl_location['locationId'].apply(lambda x: f"location_{x}")
geonames_df_with_pbl = pd.merge(geonames_df, pbl_location[['locationId', 'pblName']], on='locationId', how='left')

geonames_df.to_csv('entities_location.csv', index=False)
# geonames_df.to_excel('entities_location.xlsx', index=False)

###Prize
pbl_prizes = pbl_nagrody.loc[(pbl_nagrody['ZA_TYTUL'].notnull()) &
                             (pbl_nagrody['ZA_TYTUL'].str.lower().str.contains('\(materiały ogólne\)|\(ogólne\)', regex=True) == False)][['ZA_ZAPIS_ID', 'ZA_TYTUL', 'ZA_RO_ROK']].rename(columns={'ZA_ZAPIS_ID': 'prizeId', 'ZA_TYTUL': 'name'})
pbl_prizes['prizeId'] = pbl_prizes['prizeId'].apply(lambda x: f"prize_{x}")

def get_prize_year(x):
    try:
        no_of_brackets = x['name'].count('(')
        if no_of_brackets == 0:
            return x['ZA_RO_ROK']
        elif no_of_brackets >= 1:
            return re.findall('\d{4}', x['name'])[-1]
    except IndexError:
        return x['ZA_RO_ROK']

def remove_year_from_prize(x):
    no_of_brackets = x.count('(')
    if no_of_brackets == 0:
        return x
    elif no_of_brackets == 1:
        return x.split('(')[0].strip()
    elif no_of_brackets == 2:
        return '('.join(x.split('(')[:-1]).strip()
        
pbl_prizes['year'] = pbl_prizes.apply(lambda x: get_prize_year(x), axis=1)
pbl_prizes['name'] = pbl_prizes['name'].apply(lambda x: remove_year_from_prize(x))
pbl_prizes.drop(columns='ZA_RO_ROK', inplace=True)

pbl_prizes.to_csv('entities_prize.csv', index=False)
# pbl_prizes.to_excel('entities_prize.xlsx', index=False)

#%%Relations
###WasPublishedIn

published_journal = pbl_zapisy.loc[pbl_zapisy['ZA_TYPE'].isin(['IZA', 'PU'])]
published_journal = pd.merge(published_journal, pbl_zrodla, left_on='ZA_ZR_ZRODLO_ID', right_on='ZR_ZRODLO_ID', how='left')[['ZA_ZAPIS_ID', 'ZR_ZRODLO_ID']].rename(columns={'ZA_ZAPIS_ID': 'jArticleId', 'ZR_ZRODLO_ID': 'journalId'})
published_journal = published_journal.loc[published_journal['journalId'].notnull()]
published_journal['jArticleId'] = published_journal['jArticleId'].apply(lambda x: f"journalarticle_{x}")
published_journal['journalId'] = published_journal['journalId'].apply(lambda x: f"journal_{int(x)}")

published_journal.to_csv('relations_published_journal.csv', index=False)
# published_journal.to_excel('relations_was_published_journal.xlsx', index=False)

###Published

published_book = pbl_zapisy.loc[pbl_zapisy['ZA_TYPE'] == 'KS']
published_book = pd.merge(published_book, pbl_zapisy_wydawnictwa, left_on='ZA_ZAPIS_ID', right_on='ZAWY_ZA_ZAPIS_ID', how='left')
published_book = pd.merge(published_book, pbl_wydawnictwa, left_on='ZAWY_WY_WYDAWNICTWO_ID', right_on='WY_WYDAWNICTWO_ID', how='left')[['ZA_ZAPIS_ID', 'WY_WYDAWNICTWO_ID']].rename(columns={'ZA_ZAPIS_ID': 'bookId', 'WY_WYDAWNICTWO_ID': 'publisherId'})
published_book = published_book.loc[published_book['publisherId'].notnull()]
published_book['bookId'] = published_book['bookId'].apply(lambda x: f"book_{x}")
published_book['publisherId'] = published_book['publisherId'].apply(lambda x: f"publisher_{int(x)}")

published_book.to_csv('relations_published_book.csv', index=False)
# published_book.to_excel('relations_published.xlsx', index=False)

###Wrote

written_by_book1 = pbl_zapisy.loc[(pbl_zapisy['ZA_TYPE'] == 'KS') &
                                 (pbl_zapisy['ZA_RZ_RODZAJ1_ID'] == 1)]
written_by_book1 = pd.merge(written_by_book1, pbl_zapisy_tworcy, left_on='ZA_ZAPIS_ID', right_on='ZATW_ZA_ZAPIS_ID', how='inner')
written_by_book1 = pd.merge(written_by_book1, pbl_tworcy, left_on='ZATW_TW_TWORCA_ID', right_on='TW_TWORCA_ID', how='left')[['TW_TWORCA_ID', 'ZA_ZAPIS_ID']].rename(columns={'TW_TWORCA_ID': 'personId', 'ZA_ZAPIS_ID': 'id'})
written_by_book1['personId'] = written_by_book1['personId'].apply(lambda x: f"person_1_{x}")

written_by_book2 = pbl_zapisy.loc[(pbl_zapisy['ZA_TYPE'] == 'KS')]
written_by_book2 = written_by_book2.loc[~written_by_book2['ZA_ZAPIS_ID'].isin(written_by_book1['id'])]
written_by_book2 = pd.merge(written_by_book2, pbl_zapisy_autorzy, left_on='ZA_ZAPIS_ID', right_on='ZAAM_ZA_ZAPIS_ID', how='inner')
written_by_book2 = pd.merge(written_by_book2, pbl_autorzy, left_on='ZAAM_AM_AUTOR_ID', right_on='AM_AUTOR_ID', how='left')

written_by_book2['personId'] = written_by_book2['AM_AUTOR_ID'].apply(lambda x: f"person_1_{int(autorzy_tworcy_dict.get(x))}" if x in autorzy_tworcy_dict else None)

written_by_book3 = written_by_book2[written_by_book2['personId'].isnull()][['AM_AUTOR_ID', 'ZA_ZAPIS_ID']].rename(columns={'AM_AUTOR_ID': 'personId', 'ZA_ZAPIS_ID': 'id'})
written_by_book2 = written_by_book2[written_by_book2['personId'].notnull()][['personId', 'ZA_ZAPIS_ID']].rename(columns={'ZA_ZAPIS_ID': 'id'})
written_by_book3['personId'] = written_by_book3['personId'].apply(lambda x: f"person_2_{x}")

secondary_authors = written_by_book2['personId'].to_list()
secondary_authors.extend(written_by_book3['personId'].to_list())

written_by_book = pd.concat([written_by_book1, written_by_book2, written_by_book3])
written_by_book['id'] = written_by_book['id'].apply(lambda x: f"book_{x}")

written_by_article1 = pbl_zapisy.loc[pbl_zapisy['ZA_TYPE'] == 'PU']
written_by_article1 = pd.merge(written_by_article1, pbl_zapisy_tworcy, left_on='ZA_ZAPIS_ID', right_on='ZATW_ZA_ZAPIS_ID', how='inner')
written_by_article1 = pd.merge(written_by_article1, pbl_tworcy, left_on='ZATW_TW_TWORCA_ID', right_on='TW_TWORCA_ID', how='left')[['TW_TWORCA_ID', 'ZA_ZAPIS_ID']].rename(columns={'TW_TWORCA_ID': 'personId', 'ZA_ZAPIS_ID': 'id'})
written_by_article1['personId'] = written_by_article1['personId'].apply(lambda x: f"person_1_{x}")

written_by_article2 = pbl_zapisy.loc[pbl_zapisy['ZA_TYPE'] == 'IZA']
written_by_article2 = pd.merge(written_by_article2, pbl_zapisy_autorzy, left_on='ZA_ZAPIS_ID', right_on='ZAAM_ZA_ZAPIS_ID', how='inner')
written_by_article2 = pd.merge(written_by_article2, pbl_autorzy, left_on='ZAAM_AM_AUTOR_ID', right_on='AM_AUTOR_ID', how='left')

written_by_article2['personId'] = written_by_article2['AM_AUTOR_ID'].apply(lambda x: f"person_1_{int(autorzy_tworcy_dict.get(x))}" if x in autorzy_tworcy_dict else None)

written_by_article3 = written_by_article2[written_by_article2['personId'].isnull()][['AM_AUTOR_ID', 'ZA_ZAPIS_ID']].rename(columns={'AM_AUTOR_ID': 'personId', 'ZA_ZAPIS_ID': 'id'})
written_by_article2 = written_by_article2[written_by_article2['personId'].notnull()][['personId', 'ZA_ZAPIS_ID']].rename(columns={'ZA_ZAPIS_ID': 'id'})
written_by_article3['personId'] = written_by_article3['personId'].apply(lambda x: f"person_2_{x}")

secondary_authors.extend(written_by_article2['personId'].to_list())
secondary_authors.extend(written_by_article3['personId'].to_list())

written_by_article = pd.concat([written_by_article1, written_by_article2, written_by_article3])
written_by_article['id'] = written_by_article['id'].apply(lambda x: f"journalarticle_{x}")

written_by = pd.concat([written_by_book, written_by_article])

written_by.to_csv('relations_wrote.csv', index=False)

#uzupełnienie pbl persons

secondary_authors = set(secondary_authors)
secondary_authors = {e:True for e in secondary_authors}

pbl_persons['secondary'] = pbl_persons['AM_AUTOR_ID'].apply(lambda x: secondary_authors.get(x, False))
pbl_persons['personId'] = pbl_persons[['personId', 'AM_AUTOR_ID']].apply(lambda x: f"person_1_{int(x['personId'])}" if pd.notnull(x['personId']) else f"person_2_{int(x['AM_AUTOR_ID'])}", axis=1)

debiutanci_update_mm = debiutanci_update_mm['Id'].to_list()
pbl_persons['debutant'] = pbl_persons[['personId', 'debutant']].apply(lambda x: False if x['personId'] in debiutanci_update_mm else x['debutant'], axis=1)
pbl_persons.loc[pbl_persons['personId'].isin(['person_1_53117', 'person_1_240429']), 'creator'] = False
pbl_persons.loc[pbl_persons['personId'].isin(['person_1_53117', 'person_1_240429']), 'debutant'] = False

pbl_persons.to_csv('entities_person.csv', index=False)

# written_by.to_excel('relations_written_by.xlsx', index=False)

###WasAwarded

pbl_nagrody_adnotacje = pbl_nagrody.loc[pbl_nagrody['ZA_ADNOTACJE'].notnull()]
pbl_nagrody_adnotacje = dict(zip(pbl_nagrody_adnotacje['ZA_ZAPIS_ID'], pbl_nagrody_adnotacje['ZA_ADNOTACJE']))

pbl_laureaci = {}
for k,v in tqdm(pbl_nagrody_adnotacje.items()):
    sample_txt = nlp(v)
    for word in sample_txt.ents:
        if word.label_ == 'persName':
            if len(word.lemma_) > 3:
                if k not in pbl_laureaci:
                    pbl_laureaci[k] = [word.lemma_]
                else:
                    pbl_laureaci[k].append(word.lemma_)

pbl_persons_dict = dict(zip(pbl_persons['name'] + ' ' + pbl_persons['surname'], pbl_persons['personId']))

awarded = []
for k,v in pbl_laureaci.items():
    for element in v:
        if (person_id:=pbl_persons_dict.get(element)):
            awarded.append((person_id, f'prize_{k}'))
        
awarded = pd.DataFrame(awarded, columns = ['personId', 'prizeId'])

awarded.to_csv('relations_was_awarded.csv', index=False)
# awarded.to_excel('relations_awarded.xlsx', index=False)

###LocatedIn
pbl_geo_names = dict(zip(geonames_df_with_pbl['pblName'], geonames_df_with_pbl['locationId']))

located_in_publisher = pbl_publishers_with_place.copy()
located_in_publisher['locationId'] = located_in_publisher['WY_MIASTO'].apply(lambda x: pbl_geo_names.get(x))
located_in_publisher = located_in_publisher.loc[located_in_publisher['locationId'].notnull()][['publisherId', 'locationId']].rename(columns={'publisherId': 'id'})

located_in_journal = pbl_journals_with_place.copy()
located_in_journal['locationId'] = located_in_journal['ZR_MIEJSCE_WYD'].apply(lambda x: pbl_geo_names.get(x))
located_in_journal = located_in_journal.loc[located_in_journal['locationId'].notnull()][['journalId', 'locationId']].rename(columns={'journalId': 'id'})

located_in_book = pbl_zapisy.loc[pbl_zapisy['ZA_TYPE'] == 'KS']
located_in_book = pd.merge(located_in_book, pbl_zapisy_wydawnictwa, left_on='ZA_ZAPIS_ID', right_on='ZAWY_ZA_ZAPIS_ID', how='left')
located_in_book = pd.merge(located_in_book, pbl_wydawnictwa, left_on='ZAWY_WY_WYDAWNICTWO_ID', right_on='WY_WYDAWNICTWO_ID', how='left')[['ZA_ZAPIS_ID', 'WY_MIASTO']].rename(columns={'ZA_ZAPIS_ID': 'id', 'WY_MIASTO': 'locationId'})
located_in_book['locationId'] = located_in_book['locationId'].apply(lambda x: pbl_geo_names.get(x))
located_in_book = located_in_book.loc[located_in_book['locationId'].notnull()]
located_in_book['id'] = located_in_book['id'].apply(lambda x: f"book_{x}")

located_in = pd.concat([located_in_publisher, located_in_journal, located_in_book])

located_in.to_csv('relations_located_in.csv', index=False)
# located_in.to_excel('relations_located_in.xlsx', index=False)

###IsAbout
is_about_book = pbl_zapisy.loc[(pbl_zapisy['ZA_TYPE'] == 'KS') &
                               (pbl_zapisy['ZA_RZ_RODZAJ1_ID'].isin([2, 764]))]                              
is_about_book = pd.merge(is_about_book, pbl_zapisy_tworcy, left_on='ZA_ZAPIS_ID', right_on='ZATW_ZA_ZAPIS_ID', how='left')
is_about_book = pd.merge(is_about_book, pbl_tworcy, left_on='ZATW_TW_TWORCA_ID', right_on='TW_TWORCA_ID', how='left')[['TW_TWORCA_ID', 'ZA_ZAPIS_ID']].rename(columns={'TW_TWORCA_ID': 'personId', 'ZA_ZAPIS_ID': 'id'})
is_about_book = is_about_book.loc[is_about_book['personId'].notnull()]
is_about_book['personId'] = is_about_book['personId'].apply(lambda x: f"person_1_{int(x)}")
is_about_book['id'] = is_about_book['id'].apply(lambda x: f"book_{x}")

is_about_article = pbl_zapisy.loc[pbl_zapisy['ZA_TYPE'] == 'IZA']
is_about_article = pd.merge(is_about_article, pbl_zapisy_tworcy, left_on='ZA_ZAPIS_ID', right_on='ZATW_ZA_ZAPIS_ID', how='left')
is_about_article = pd.merge(is_about_article, pbl_tworcy, left_on='ZATW_TW_TWORCA_ID', right_on='TW_TWORCA_ID', how='left')[['TW_TWORCA_ID', 'ZA_ZAPIS_ID']].rename(columns={'TW_TWORCA_ID': 'personId', 'ZA_ZAPIS_ID': 'id'})

is_about_article = is_about_article.loc[is_about_article['personId'].notnull()]

is_about_article['personId'] = is_about_article['personId'].apply(lambda x: f"person_1_{int(x)}")
is_about_article['id'] = is_about_article['id'].apply(lambda x: f"journalarticle_{x}")

is_about = pd.concat([is_about_book, is_about_article])

is_about.to_csv('relations_is_about.csv', index=False)
# is_about.to_excel('relations_is_about.xlsx', index=False)

#%% notatki

# test = old_persons_file.sample(1000)


# pbl_articles_query = """select z.za_zapis_id "rekord_id", z.za_type "typ", rz.rz_rodzaj_id "rodzaj_zapisu_id", rz.rz_nazwa "rodzaj_zapisu", dz.dz_dzial_id "dzial_id", dz.dz_nazwa "dzial", to_char(tw.tw_tworca_id) "tworca_id", tw.tw_nazwisko "tworca_nazwisko", tw.tw_imie "tworca_imie", to_char(a.am_autor_id) "autor_id", (case when a.am_nazwisko is null then a.am_kryptonim else a.am_nazwisko end) "autor_nazwisko", a.am_imie "autor_imie", z.za_tytul "tytul", z.za_opis_wspoltworcow "wspoltworcy", fo.fo_nazwa "funkcja_osoby", to_char(os.os_osoba_id) "wspoltworca_id", os.os_nazwisko "wspoltworca_nazwisko", os.os_imie "wspoltworca_imie", z.za_adnotacje "adnotacja", z.za_adnotacje2 "adnotacja2", z.za_adnotacje3 "adnotacja3", to_char(zr.zr_zrodlo_id) "czasopismo_id", zr.zr_tytul "czasopismo", z.za_zrodlo_rok "rok", z.za_zrodlo_nr "numer", z.za_zrodlo_str "strony",z.za_tytul_oryginalu,z.za_te_teatr_id,z.ZA_UZYTK_WPIS_DATA,z.ZA_UZYTK_MOD_DATA,z.ZA_TYPE
#                     from pbl_zapisy z
#                     full outer join IBL_OWNER.pbl_zapisy_tworcy zt on zt.zatw_za_zapis_id=z.za_zapis_id
#                     full outer join IBL_OWNER.pbl_tworcy tw on zt.zatw_tw_tworca_id=tw.tw_tworca_id
#                     full outer join IBL_OWNER.pbl_zapisy_autorzy za on za.zaam_za_zapis_id=z.za_zapis_id
#                     full outer join IBL_OWNER.pbl_autorzy a on za.zaam_am_autor_id=a.am_autor_id
#                     full outer join IBL_OWNER.pbl_zrodla zr on zr.zr_zrodlo_id=z.za_zr_zrodlo_id
#                     full outer join IBL_OWNER.pbl_dzialy dz on dz.dz_dzial_id=z.za_dz_dzial1_id
#                     full outer join IBL_OWNER.pbl_rodzaje_zapisow rz on rz.rz_rodzaj_id=z.za_rz_rodzaj1_id
#                     full outer join IBL_OWNER.pbl_udzialy_osob uo on uo.uo_za_zapis_id = z.za_zapis_id
#                     full outer join IBL_OWNER.pbl_osoby os on uo.uo_os_osoba_id=os.os_osoba_id
#                     full outer join IBL_OWNER.pbl_funkcje_osob fo on fo.fo_symbol=uo.uo_fo_symbol
#                     where z.za_type in ('IZA','PU')
#                     and zr.zr_tytul is not null
#                     and zr.zr_tytul not like 'x'"""                  
# pbl_sh_query1 = """select hpz.hz_za_zapis_id,hp.hp_nazwa,khp.kh_nazwa
#                 from IBL_OWNER.pbl_hasla_przekrojowe hp
#                 join IBL_OWNER.pbl_hasla_przekr_zapisow hpz on hpz.hz_hp_haslo_id=hp.hp_haslo_id
#                 join IBL_OWNER.pbl_klucze_hasla_przekr khp on khp.kh_hp_haslo_id=hp.hp_haslo_id
#                 join IBL_OWNER.pbl_hasla_zapisow_klucze hzk on hzk.hzkh_hz_hp_haslo_id=hp.hp_haslo_id and hzk.hzkh_kh_klucz_id=khp.kh_klucz_id and hzk.hzkh_hz_za_zapis_id=hpz.hz_za_zapis_id"""               
# pbl_sh_query2 = """select hpz.hz_za_zapis_id,hp.hp_nazwa,to_char(null) "KH_NAZWA"
#                 from IBL_OWNER.pbl_hasla_przekrojowe hp
#                 join IBL_OWNER.pbl_hasla_przekr_zapisow hpz on hpz.hz_hp_haslo_id=hp.hp_haslo_id"""

# pbl_persons_index_query = """select odi_za_zapis_id, odi_nazwisko, odi_imie
#                 from IBL_OWNER.pbl_osoby_do_indeksu"""

# pbl_books = pd.read_sql(pbl_books_query, con=connection).fillna(value = np.nan)