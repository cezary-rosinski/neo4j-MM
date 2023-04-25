#Notes --> https://docs.google.com/document/d/1mQIW65vWUZ6FY6vhfTfOpmA8O3OLnsUzEZzXLVZhE4U/edit?pli=1#heading=h.teu1qln8tb46
#%% import
import cx_Oracle
import sys
sys.path.insert(1, 'C:/Users/Cezary/Documents/IBL-PAN-Python')
from pbl_credentials import pbl_user, pbl_password
from my_functions import gsheet_to_df
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

#%%PBL connection

dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user=pbl_user, password=pbl_password, dsn=dsn_tns, encoding='windows-1250')

#%% Harvesting

#%%Entities
###Person --> na razie tu tylko twórcy, docelowo mają być wszystkie osoby

# old_persons_file = pd.read_excel(r"C:\Users\Cezary\Downloads\kartoteka osób - 11.10.2018 - gotowe_po_konfliktach.xlsx")
# tw_tworca_id_list = [int(e) for e in old_persons_file['tw_tworca_id'].drop_duplicates().dropna().to_list()]

ksiazki_debiutantow = pd.read_excel(r"F:\Cezary\Documents\IBL\Tabele dla MM\2. książki debiutantów.xlsx")
debiutanci = ksiazki_debiutantow['id twórcy'].drop_duplicates().to_list()

pbl_query = """select * from pbl_tworcy tw"""
pbl_query = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)

# tw_tworca_id_new = pbl_query.loc[~pbl_query['TW_TWORCA_ID'].isin(tw_tworca_id_list)][['TW_TWORCA_ID', 'TW_NAZWISKO', 'TW_IMIE']]
# tw_tworca_id_new.to_excel('test.xlsx', index=False)

mapowanie_bn_pbl = ['1_Bhwzo0xu4yTn8tF0ZNAZq9iIAqIxfcrjeLVCm_mggM', '1L-7Zv9EyLr5FeCIY_s90rT5Hz6DjAScCx6NxfuHvoEQ', '1cEz73dGN2r2-TTc702yne9tKfH9PQ6UyAJ2zBSV6Jb0']
#UWAGA --> ogarnąć, że czy_ten_sam ma więcej akceptowalnych wartości
df = pd.DataFrame()
for file in tqdm(mapowanie_bn_pbl):
    temp_df = gsheet_to_df(file, 'pbl_bn')
    temp_df = temp_df.loc[temp_df['czy_ten_sam'].isin('tak', 'raczej tak')]
    df = pd.concat([df, temp_df])
    
pbl_viaf = df.copy()[['pbl_id', 'viaf']].rename(columns={'pbl_id': 'TW_TWORCA_ID'})
pbl_viaf['TW_TWORCA_ID'] = pbl_viaf['TW_TWORCA_ID'].astype(np.int64)

pbl_persons = pd.merge(pbl_query, pbl_viaf, on='TW_TWORCA_ID', how='left')
viafy = pbl_persons['viaf'].drop_duplicates().dropna().to_list()
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
pbl_persons = pbl_persons[['TW_TWORCA_ID', 'TW_IMIE', 'TW_NAZWISKO', 'gender', 'born', 'died', 'birthPlace', 'deathPlace', 'debutant']].rename(columns={'TW_TWORCA_ID': 'authorId', 'TW_IMIE': 'name', 'TW_NAZWISKO': 'surname'})
pbl_persons['authorId'] = pbl_persons['authorId'].apply(lambda x: f"author_{x}")
pbl_persons['creator'] = True

pbl_persons.to_excel('test_persons.xlsx', index=False)


# w pierwszej kolejności w person dać tylko twórców, dać im stałe identyfikatory, pobrać z wiki dodatkowe informacje
# jak zdefiniować debiutantów? --> po 15.04 dane od PH z retro


###Journal

pbl_query = """select * from IBL_OWNER.pbl_zrodla"""
pbl_query = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)

pbl_journals = pbl_query.copy()[['ZR_ZRODLO_ID', 'ZR_TYTUL']].rename(columns={'ZR_ZRODLO_ID': 'journalId', 'ZR_TYTUL': 'name'})
pbl_journals['journalId'] = pbl_journals['journalId'].apply(lambda x: f"journal_{x}")

pbl_journals.to_excel('test_journals.xlsx', index=False)

###JournalArticle
pbl_query = """select * from IBL_OWNER.pbl_zapisy z
full outer join IBL_OWNER.pbl_rodzaje_zapisow rz on rz.rz_rodzaj_id=z.za_rz_rodzaj1_id
full outer join IBL_OWNER.pbl_zrodla zr on zr.zr_zrodlo_id=z.za_zr_zrodlo_id
where z.za_type in ('IZA', 'PU')"""
pbl_query = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)

pbl_journal_articles = pbl_query.copy()[['ZA_ZAPIS_ID', 'ZA_TYTUL', 'RZ_NAZWA', 'ZA_ZRODLO_NR', 'ZA_ZRODLO_STR', 'ZA_ZRODLO_ROK', 'ZA_TYPE']].rename(columns={'ZA_ZAPIS_ID': 'jArticleId', 'ZA_TYTUL': 'title', 'RZ_NAZWA': 'genre', 'ZA_ZRODLO_NR': 'issue', 'ZA_ZRODLO_ROK': 'year', 'ZA_ZRODLO_STR': 'numberOfPages'})

pbl_journal_articles['type'] = pbl_journal_articles['ZA_TYPE'].apply(lambda x: 'Literature' if x == 'PU' else 'Secondary')
pbl_journal_articles.drop(columns='ZA_TYPE', inplace=True)
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

pbl_journal_articles.to_excel('test_journal_articles.xlsx', index=False)

###Book
pbl_query = """select * from IBL_OWNER.pbl_zapisy z
full outer join IBL_OWNER.pbl_rodzaje_zapisow rz on rz.rz_rodzaj_id=z.za_rz_rodzaj1_id
where z.za_type like 'KS'"""
pbl_query = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)

pbl_books = pbl_query.copy()[['ZA_ZAPIS_ID', 'ZA_TYTUL', 'ZA_RO_ROK', 'RZ_NAZWA', 'ZA_OPIS_FIZYCZNY_KSIAZKI']].rename(columns={'ZA_ZAPIS_ID': 'bookId', 'ZA_TYTUL': 'title', 'ZA_RO_ROK': 'year', 'RZ_NAZWA': 'genre', 'ZA_OPIS_FIZYCZNY_KSIAZKI': 'numberOfPages'})

pbl_books['bookId'] = pbl_books['bookId'].apply(lambda x: f"book_{x}")

def count_pages_books(x):
    if pd.isnull(x):
        return None
    else:
        try:
            return re.findall('\d+', x.split('s.')[0])[0]
        except: None

pbl_books['numberOfPages'] = pbl_books['numberOfPages'].apply(lambda x: count_pages_books(x))

pbl_books.to_excel('test_books.xlsx', index=False)

###Publisher
pbl_query = """select * from IBL_OWNER.pbl_wydawnictwa"""
pbl_query = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)

# pbl_publishers = pbl_query.copy()[['WY_WYDAWNICTWO_ID', 'WY_NAZWA', 'WY_MIASTO']].rename(columns={'WY_WYDAWNICTWO_ID': 'publisherId', 'WY_NAZWA': 'name', 'WY_MIASTO': 'locatedIn'})
pbl_publishers = pbl_query.copy()[['WY_WYDAWNICTWO_ID', 'WY_NAZWA', 'WY_MIASTO']].rename(columns={'WY_WYDAWNICTWO_ID': 'publisherId', 'WY_NAZWA': 'name'})

pbl_publishers['publisherId'] = pbl_publishers['publisherId'].apply(lambda x: f"publisher_{x}")

pbl_publishers.to_excel('test_publishers.xlsx', index=False)

###Location


!!!TUTAJ!!!
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
    
pbl_location = pbl_location[['geonamesId', 'query name', 'countryName']].rename(columns={'query name': 'city', 'geonamesId': 'locationId', 'countryName': 'country'})
pbl_location['coordinates'] = ''

pbl_places = pbl_location['locationId'].drop_duplicates().to_list()

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

places2 = {k for k,v in geonames_resp.items() if not v}

geonames_resp = {}
with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(harvest_geonames, places2), total=len(places2)))

pbl_location.to_excel('test_locations.xlsx', index=False)

###Prize
pbl_query = """select * from pbl_zapisy z
where z.za_type like 'IR'"""
pbl_query = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)

pbl_prizes = pbl_query.copy()[['ZA_ZAPIS_ID', 'ZA_TYTUL']].rename(columns={'ZA_ZAPIS_ID': 'prizeId', 'ZA_TYTUL': 'name'})
pbl_prizes['prizeId'] = pbl_prizes['prizeId'].apply(lambda x: f"prize_{x}")

pbl_prizes.to_excel('test_prizes.xlsx', index=False)

#%%Relations
###PublishedIn

pbl_query = """select * from pbl_zapisy z
inner join IBL_OWNER.pbl_zrodla zr on zr.zr_zrodlo_id=z.za_zr_zrodlo_id
where z.za_type in ('IZA', 'PU')"""
pbl_query = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)

published_in = pbl_query.copy()[['ZA_ZAPIS_ID', 'ZR_ZRODLO_ID', 'ZA_RO_ROK']].rename(columns={'ZA_ZAPIS_ID': 'journalArticleId', 'ZR_ZRODLO_ID': 'journalId', 'ZA_RO_ROK': 'year'})
published_in['journalArticleId'] = published_in['journalArticleId'].apply(lambda x: f"article_{x}")
published_in['journalId'] = published_in['journalId'].apply(lambda x: f"journal_{x}")

published_in.to_excel('test_published_in.xlsx', index=False)

###PublishedBy

pbl_query = """select * from pbl_zapisy z
inner join IBL_OWNER.pbl_zapisy_wydawnictwa zw on zw.zawy_za_zapis_id=z.za_zapis_id
inner join IBL_OWNER.pbl_wydawnictwa wy on wy.wy_wydawnictwo_id=zw.zawy_wy_wydawnictwo_id
where z.za_type like 'KS'"""
pbl_query = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)

published_by = pbl_query.copy()[['ZA_ZAPIS_ID', 'WY_WYDAWNICTWO_ID']].rename(columns={'ZA_ZAPIS_ID': 'bookId', 'WY_WYDAWNICTWO_ID': 'publisherId'})

published_by['bookId'] = published_by['bookId'].apply(lambda x: f"book_{x}")
published_by['publisherId'] = published_by['publisherId'].apply(lambda x: f"publisher_{x}")

published_by.to_excel('test_published_by.xlsx', index=False)

###WrittenBy
pbl_query = """select * from pbl_zapisy z
inner join IBL_OWNER.pbl_rodzaje_zapisow rz on rz.rz_rodzaj_id=z.za_rz_rodzaj1_id
inner join IBL_OWNER.pbl_zapisy_tworcy zt on zt.zatw_za_zapis_id=z.za_zapis_id
inner join IBL_OWNER.pbl_tworcy tw on tw.tw_tworca_id=zt.zatw_tw_tworca_id
where z.za_type like 'KS'
and rz.rz_nazwa like 'książka twórcy (podmiotowa)'"""
pbl_query = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)

#UWAGA --> na razie tylko twórcy i tylko książki
written_by_book = pbl_query.copy()[['TW_TWORCA_ID', 'ZA_ZAPIS_ID']].rename(columns={'TW_TWORCA_ID': 'authorId', 'ZA_ZAPIS_ID': 'bookId'})

written_by_book['authorId'] = written_by_book['authorId'].apply(lambda x: f"author_{x}")
written_by_book['bookId'] = written_by_book['bookId'].apply(lambda x: f"book_{x}")

written_by_book.to_excel('test_written_by_book.xlsx', index=False)

#%% notatki

test = old_persons_file.sample(1000)


pbl_articles_query = """select z.za_zapis_id "rekord_id", z.za_type "typ", rz.rz_rodzaj_id "rodzaj_zapisu_id", rz.rz_nazwa "rodzaj_zapisu", dz.dz_dzial_id "dzial_id", dz.dz_nazwa "dzial", to_char(tw.tw_tworca_id) "tworca_id", tw.tw_nazwisko "tworca_nazwisko", tw.tw_imie "tworca_imie", to_char(a.am_autor_id) "autor_id", (case when a.am_nazwisko is null then a.am_kryptonim else a.am_nazwisko end) "autor_nazwisko", a.am_imie "autor_imie", z.za_tytul "tytul", z.za_opis_wspoltworcow "wspoltworcy", fo.fo_nazwa "funkcja_osoby", to_char(os.os_osoba_id) "wspoltworca_id", os.os_nazwisko "wspoltworca_nazwisko", os.os_imie "wspoltworca_imie", z.za_adnotacje "adnotacja", z.za_adnotacje2 "adnotacja2", z.za_adnotacje3 "adnotacja3", to_char(zr.zr_zrodlo_id) "czasopismo_id", zr.zr_tytul "czasopismo", z.za_zrodlo_rok "rok", z.za_zrodlo_nr "numer", z.za_zrodlo_str "strony",z.za_tytul_oryginalu,z.za_te_teatr_id,z.ZA_UZYTK_WPIS_DATA,z.ZA_UZYTK_MOD_DATA,z.ZA_TYPE
                    from pbl_zapisy z
                    full outer join IBL_OWNER.pbl_zapisy_tworcy zt on zt.zatw_za_zapis_id=z.za_zapis_id
                    full outer join IBL_OWNER.pbl_tworcy tw on zt.zatw_tw_tworca_id=tw.tw_tworca_id
                    full outer join IBL_OWNER.pbl_zapisy_autorzy za on za.zaam_za_zapis_id=z.za_zapis_id
                    full outer join IBL_OWNER.pbl_autorzy a on za.zaam_am_autor_id=a.am_autor_id
                    full outer join IBL_OWNER.pbl_zrodla zr on zr.zr_zrodlo_id=z.za_zr_zrodlo_id
                    full outer join IBL_OWNER.pbl_dzialy dz on dz.dz_dzial_id=z.za_dz_dzial1_id
                    full outer join IBL_OWNER.pbl_rodzaje_zapisow rz on rz.rz_rodzaj_id=z.za_rz_rodzaj1_id
                    full outer join IBL_OWNER.pbl_udzialy_osob uo on uo.uo_za_zapis_id = z.za_zapis_id
                    full outer join IBL_OWNER.pbl_osoby os on uo.uo_os_osoba_id=os.os_osoba_id
                    full outer join IBL_OWNER.pbl_funkcje_osob fo on fo.fo_symbol=uo.uo_fo_symbol
                    where z.za_type in ('IZA','PU')
                    and zr.zr_tytul is not null
                    and zr.zr_tytul not like 'x'"""                  
pbl_sh_query1 = """select hpz.hz_za_zapis_id,hp.hp_nazwa,khp.kh_nazwa
                from IBL_OWNER.pbl_hasla_przekrojowe hp
                join IBL_OWNER.pbl_hasla_przekr_zapisow hpz on hpz.hz_hp_haslo_id=hp.hp_haslo_id
                join IBL_OWNER.pbl_klucze_hasla_przekr khp on khp.kh_hp_haslo_id=hp.hp_haslo_id
                join IBL_OWNER.pbl_hasla_zapisow_klucze hzk on hzk.hzkh_hz_hp_haslo_id=hp.hp_haslo_id and hzk.hzkh_kh_klucz_id=khp.kh_klucz_id and hzk.hzkh_hz_za_zapis_id=hpz.hz_za_zapis_id"""               
pbl_sh_query2 = """select hpz.hz_za_zapis_id,hp.hp_nazwa,to_char(null) "KH_NAZWA"
                from IBL_OWNER.pbl_hasla_przekrojowe hp
                join IBL_OWNER.pbl_hasla_przekr_zapisow hpz on hpz.hz_hp_haslo_id=hp.hp_haslo_id"""

pbl_persons_index_query = """select odi_za_zapis_id, odi_nazwisko, odi_imie
                from IBL_OWNER.pbl_osoby_do_indeksu"""

pbl_books = pd.read_sql(pbl_books_query, con=connection).fillna(value = np.nan)