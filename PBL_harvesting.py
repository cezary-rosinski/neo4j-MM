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

#%%PBL connection

dsn_tns = cx_Oracle.makedsn('pbl.ibl.poznan.pl', '1521', service_name='xe')
connection = cx_Oracle.connect(user=pbl_user, password=pbl_password, dsn=dsn_tns, encoding='windows-1250')

#%% Harvesting

##Entities
###Person

# old_persons_file = pd.read_excel(r"C:\Users\Cezary\Downloads\kartoteka osób - 11.10.2018 - gotowe_po_konfliktach.xlsx")
# tw_tworca_id_list = [int(e) for e in old_persons_file['tw_tworca_id'].drop_duplicates().dropna().to_list()]

ksiazki_debiutantow = pd.read_excel(r"F:\Cezary\Documents\IBL\Tabele dla MM\2. książki debiutantów.xlsx")
debiutanci = ksiazki_debiutantow['id twórcy'].drop_duplicates().to_list()

pbl_query = """select * from pbl_tworcy tw"""
pbl_query = pd.read_sql(pbl_query, con=connection).fillna(value = np.nan)

# tw_tworca_id_new = pbl_query.loc[~pbl_query['TW_TWORCA_ID'].isin(tw_tworca_id_list)][['TW_TWORCA_ID', 'TW_NAZWISKO', 'TW_IMIE']]
# tw_tworca_id_new.to_excel('test.xlsx', index=False)

mapowanie_bn_pbl = ['1_Bhwzo0xu4yTn8tF0ZNAZq9iIAqIxfcrjeLVCm_mggM', '1L-7Zv9EyLr5FeCIY_s90rT5Hz6DjAScCx6NxfuHvoEQ', '1cEz73dGN2r2-TTc702yne9tKfH9PQ6UyAJ2zBSV6Jb0']

df = pd.DataFrame()
for file in tqdm(mapowanie_bn_pbl):
    temp_df = gsheet_to_df(file, 'pbl_bn')
    temp_df = temp_df.loc[temp_df['czy_ten_sam'] == 'tak']
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

wikidata_response = {}
with ThreadPoolExecutor() as executor:
    list(tqdm(executor.map(get_wikidata_info, wikidata_ids), total=len(wikidata_ids)))

labels_dict = {'P21': 'gender', 'P569': 'born', 'P570': 'died', 'P19': 'birthPlace', 'P20': 'deathPlace'}

for label in tqdm(labels_dict):
    pbl_persons[labels_dict.get(label)] = pbl_persons['wikidata'].apply(lambda x: wikidata_response.get(x).get(label) if x in wikidata_response else x)

pbl_persons.to_excel('test_persons.xlsx', index=False)


# w pierwszej kolejności w person dać tylko twórców, dać im stałe identyfikatory, pobrać z wiki dodatkowe informacje
# jak zdefiniować debiutantów? --> po 15.04 dane od PH z retro


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