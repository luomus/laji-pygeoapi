import pandas as pd
import numpy as np

def compute_coordinate_uncertainty(gdf):
    # computed_coordinate_uncertainty
    return gdf

def compute_var_from_id_threatened_status(gdf):
    #computed_var_from_id_threatened_status
    return gdf

def compute_var_red_list_status(gdf):
    #computed_var_red_list_status
    return gdf

def compute_var_occurrence_status(gdf):
    #computed_var_occurrence_status
    return gdf

def compute_var_from_id_regulatory_status(gdf):
    #computed_var_from_id_regulatory_status
    return gdf

def compute_var_from_id_primary_habitat(habitat_column):
    # Mapping data
    habitat_dict = {
        'http://tun.fi/MKV.habitatM': 'M - Metsät',
        'http://tun.fi/MKV.habitatMk': 'Mk - kangasmetsät',
        'http://tun.fi/MKV.habitatMkk': 'Mkk - kuivahkot ja sitä karummat kankaat',
        'http://tun.fi/MKV.habitatMkt': 'Mkt - tuoreet ja lehtomaiset kankaat',
        'http://tun.fi/MKV.habitatMl': 'Ml - lehdot (myös kuusivaltaiset)',
        'http://tun.fi/MKV.habitatMlt': 'Mlt - tuoreet ja kuivat lehdot',
        'http://tun.fi/MKV.habitatMlk': 'Mlk - kosteat lehdot',
        'http://tun.fi/MKV.habitatMt': 'Mt - tunturikoivikot (pois lukien tunturikoivulehdot)',
        'http://tun.fi/MKV.habitatMtl': 'Mtl - tunturikoivulehdot',
        'http://tun.fi/MKV.habitatS': 'S - Suot',
        'http://tun.fi/MKV.habitatSl': 'Sl - letot',
        'http://tun.fi/MKV.habitatSla': 'Sla - avoletot',
        'http://tun.fi/MKV.habitatSlr': 'Slr - lettorämeet',
        'http://tun.fi/MKV.habitatSlk': 'Slk - lettokorvet',
        'http://tun.fi/MKV.habitatSn': 'Sn - nevat',
        'http://tun.fi/MKV.habitatSnk': 'Snk - karut nevat',
        'http://tun.fi/MKV.habitatSnr': 'Snr - rehevät nevat',
        'http://tun.fi/MKV.habitatSr': 'Sr - rämeet',
        'http://tun.fi/MKV.habitatSrk': 'Srk - karut rämeet',
        'http://tun.fi/MKV.habitatSrr': 'Srr - rehevät rämeet',
        'http://tun.fi/MKV.habitatSk': 'Sk - korvet',
        'http://tun.fi/MKV.habitatSkk': 'Skk - karut korvet',
        'http://tun.fi/MKV.habitatSkr': 'Skr - rehevät korvet',
        'http://tun.fi/MKV.habitatV': 'V - Vedet',
        'http://tun.fi/MKV.habitatVi': 'Vi - Itämeri',
        'http://tun.fi/MKV.habitatVik': 'Vik - kallio- ja lohkarepohjat',
        'http://tun.fi/MKV.habitatVim': 'Vim - muta- ja liejupohjat',
        'http://tun.fi/MKV.habitatVis': 'Vis - sorapohjat',
        'http://tun.fi/MKV.habitatVih': 'Vih - hiekkapohjat',
        'http://tun.fi/MKV.habitatVie': 'Vie - sekapohjat',
        'http://tun.fi/MKV.habitatVip': 'Vip - pelagiaali',
        'http://tun.fi/MKV.habitatVs': 'Vs - järvet ja lammet',
        'http://tun.fi/MKV.habitatVsk': 'Vsk - karut järvet ja lammet',
        'http://tun.fi/MKV.habitatVsr': 'Vsr - rehevät järvet ja lammet',
        'http://tun.fi/MKV.habitatVa': 'Va - lampareet ja allikot (myös rimmet)',
        'http://tun.fi/MKV.habitatVj': 'Vj - joet',
        'http://tun.fi/MKV.habitatVp': 'Vp - purot ja norot',
        'http://tun.fi/MKV.habitatVk': 'Vk - kosket',
        'http://tun.fi/MKV.habitatVl': 'Vl - lähteiköt',
        'http://tun.fi/MKV.habitatR': 'R - Rannat',
        'http://tun.fi/MKV.habitatRi': 'Ri - Itämeren rannat',
        'http://tun.fi/MKV.habitatRim': 'Rim - rantametsät',
        'http://tun.fi/MKV.habitatRimt': 'Rimt - tulvametsät',
        'http://tun.fi/MKV.habitatRiml': 'Riml - metsäluhdat',
        'http://tun.fi/MKV.habitatRip': 'Rip - rantapensaikot',
        'http://tun.fi/MKV.habitatRin': 'Rin - niittyrannat',
        'http://tun.fi/MKV.habitatRil': 'Ril - luhtarannat',
        'http://tun.fi/MKV.habitatRir': 'Rir - ruovikot',
        'http://tun.fi/MKV.habitatRis': 'Ris - sora-, somerikko- ja kivikkorannat',
        'http://tun.fi/MKV.habitatRih': 'Rih - hietikkorannat',
        'http://tun.fi/MKV.habitatRit': 'Rit - avoimet tulvarannat',
        'http://tun.fi/MKV.habitatRj': 'Rj - järven- ja joenrannat',
        'http://tun.fi/MKV.habitatRjm': 'Rjm - rantametsät',
        'http://tun.fi/MKV.habitatRjmt': 'Rjmt - tulvametsät',
        'http://tun.fi/MKV.habitatRjml': 'Rjml - metsäluhdat',
        'http://tun.fi/MKV.habitatRjp': 'Rjp - rantapensaikot',
        'http://tun.fi/MKV.habitatRjn': 'Rjn - niittyrannat',
        'http://tun.fi/MKV.habitatRjl': 'Rjl - luhtarannat',
        'http://tun.fi/MKV.habitatRjr': 'Rjr - ruovikot',
        'http://tun.fi/MKV.habitatRjs': 'Rjs - sora-, somerikko- ja kivikkorannat',
        'http://tun.fi/MKV.habitatRjh': 'Rjh - hietikkorannat',
        'http://tun.fi/MKV.habitatRjt': 'Rjt - avoimet tulvarannat',
        'http://tun.fi/MKV.habitatK': 'K - Kalliot ja kivikot',
        'http://tun.fi/MKV.habitatKk': 'Kk - kalkkikalliot ja -louhokset, myös paljas kalkkimaa',
        'http://tun.fi/MKV.habitatKs': 'Ks - serpentiinikalliot ja -maa',
        'http://tun.fi/MKV.habitatKr': 'Kr - kalliorotkot, rotkolaaksot ja kurut',
        'http://tun.fi/MKV.habitatKl': 'Kl - luolat ja halkeamat',
        'http://tun.fi/MKV.habitatKm': 'Km - karut ja keskiravinteiset kalliot',
        'http://tun.fi/MKV.habitatT': 'T - Tunturipaljakat',
        'http://tun.fi/MKV.habitatTk': 'Tk - tunturikankaat',
        'http://tun.fi/MKV.habitatTn': 'Tn - tunturiniityt',
        'http://tun.fi/MKV.habitatTu': 'Tu - lumenviipymät',
        'http://tun.fi/MKV.habitatTp': 'Tp - tunturikangaspensaikot',
        'http://tun.fi/MKV.habitatTl': 'Tl - paljakan kalliot ja kivikot',
        'http://tun.fi/MKV.habitatTll': 'Tll - paljakan karut ja keskiravinteiset kalliot ja kivikot',
        'http://tun.fi/MKV.habitatTlk': 'Tlk - paljakan kalkkikalliot ja -kivikot',
        'http://tun.fi/MKV.habitatTls': 'Tls - paljakan serpentiinikalliot ja -kivikot',
        'http://tun.fi/MKV.habitatTlr': 'Tlr - paljakan kalliorotkot, rotkolaaksot ja kurut',
        'http://tun.fi/MKV.habitatTlä': 'Tlä - paljakan lähteiköt ja tihkupinnat',
        'http://tun.fi/MKV.habitatTs': 'Ts - paljakan suot',
        'http://tun.fi/MKV.habitatTj': 'Tj - paljakan järvet ja lammet (sis. rannat)',
        'http://tun.fi/MKV.habitatTv': 'Tv - paljakan virtavedet (sis. rannat)',
        'http://tun.fi/MKV.habitatTa': 'Ta - paljakan lampareet ja allikot',
        'http://tun.fi/MKV.habitatI': 'I - Perinneympäristöt ja muut ihmisen muuttamat ympäristöt',
        'http://tun.fi/MKV.habitatIn': 'In - kuivat niityt, kedot ja nummet',
        'http://tun.fi/MKV.habitatIt': 'It - tuoreet niityt',
        'http://tun.fi/MKV.habitatIh': 'Ih - hakamaat, lehdesniityt ja metsälaitumet',
        'http://tun.fi/MKV.habitatIk': 'Ik - kosteat niityt (muut kuin rantaniityt)',
        'http://tun.fi/MKV.habitatIo': 'Io - ojat ja muut kaivannot',
        'http://tun.fi/MKV.habitatIv': 'Iv - viljelymaat',
        'http://tun.fi/MKV.habitatIp': 'Ip - puistot, pihamaat ja puutarhat',
        'http://tun.fi/MKV.habitatIu': 'Iu - uuselinympäristöt',
        'http://tun.fi/MKV.habitatIr': 'Ir - rakennukset ja rakenteet',
        'http://tun.fi/MKV.habitatU': '? - Elinympäristö tuntematon'
    }

    habitat_column = habitat_column.map(habitat_dict)
    return habitat_column

def compute_var_from_id_municipality(gdf):
    #computed_var_from_id_municipality
    return gdf

def compute_var_from_id_collection(gdf):
    #computedd_var_from_id_collection
    return gdf

def compute_var_from_id_municipality(gdf):
    #computed_var_from_id_municipality
    return gdf
    
def compute_var_from_id_atlas_class(atlas_class_col):
    # Data from https://schema.laji.fi/alt/MY.atlasClassEnum
    data = [
        {"id": "http://tun.fi/MY.atlasClassEnumA", "value": {"en": "Unlikely breeding", "fi": "Epätodennäköinen pesintä", "sv": "Osannolik häckning"}},
        {"id": "http://tun.fi/MY.atlasClassEnumB", "value": {"en": "Possible breeding", "fi": "Mahdollinen pesintä", "sv": "Möjlig häckning"}},
        {"id": "http://tun.fi/MY.atlasClassEnumC", "value": {"fi": "Todennäköinen pesintä", "en": "Probable breeding", "sv": "Sannolik häckning"}},
        {"id": "http://tun.fi/MY.atlasClassEnumD", "value": {"fi": "Varma pesintä", "en": "Confirmed breeding", "sv": "Säker häckning"}}
    ]

    id_to_finnish = {item['id']: item['value']['fi'] for item in data}
    return atlas_class_col.map(id_to_finnish)

def compute_var_from_id_atlas_code(atlas_code_col):
    # Data from the provided JSON string
    data = [
        {"id": "http://tun.fi/MY.atlasCodeEnum1", "value": {"fi": "1 Epätodennäköinen pesintä: havaittu lajin yksilö, havainto ei viittaa pesintään.", "sv": "1 Osannolik häckning; Art observerad som vistats i rutan under häckningstid, men som högst sannolikt inte häckar där.", "en": "1 Breeding unlikely; Species detected in the grid during the breeding season, but almost certainly does not breed there"}},
        {"id": "http://tun.fi/MY.atlasCodeEnum2", "value": {"en": "2 Possible breeding; A solitary bird detected once in suitable breeding habitat, and breeding of the species in the grid is possible", "fi": "2 Mahdollinen pesintä: yksittäinen lintu kerran, on sopivaa pesimäympäristöä.", "sv": "2 Möjlig häckning; Ensam fågel observerad en gång (t.ex. sjungande eller spelande hane) i för arten typisk häckningsbiotop, och artens häckning i rutan är möjlig."}},
        {"id": "http://tun.fi/MY.atlasCodeEnum3", "value": {"en": "3 Possible breeding; A pair detected once in a suitable breeding habitat, and breeding of the species in the grid is possible", "sv": "3 Möjlig häckning; Par observerat en gång i lämplig häckningsbiotop, och artens häckning i rutan är möjlig.", "fi": "3 Mahdollinen pesintä: pari kerran, on sopivaa pesimäympäristöä."}},
        {"id": "http://tun.fi/MY.atlasCodeEnum4", "value": {"en": "4 Probable breeding; A singing or a displaying male observed at the same site in different days", "fi": "4 Todennäköinen pesintä: koiras reviirillä (esim. laulaa) eri päivinä.", "sv": "4 Möjlig häckning; Sjungande, spelande eller uppträdande hane observerad på samma plats (dvs. på ett bestående revir) under flera dagar."}},
        {"id": "http://tun.fi/MY.atlasCodeEnum5", "value": {"sv": "5 Möjlig häckning; Observerats hona eller par på samma plats under flera dagar.", "en": "5 Probable breeding; A female or a pair observed at the same site in different days", "fi": "5 Todennäköinen pesintä: naaras tai pari reviirillä eri päivinä."}},
        {"id": "http://tun.fi/MY.atlasCodeEnum6", "value": {"en": "6 Probable breeding; A bird or a pair observed", "sv": "6 Sannolik häckning; Fågel eller par setts", "fi": "6 Todennäköinen pesintä: linnun tai parin havainto viittaa vahvasti pesintään."}},
        {"id": "http://tun.fi/MY.atlasCodeEnum61", "value": {"en": "61 Probable breeding; A bird or a pair observed visiting frequently at the probable nest", "sv": "61 Sannolik häckning; Fågel eller par setts återkommande besöka en sannolik boplats", "fi": "61 Todennäköinen pesintä: lintu tai pari käy usein todennäköisellä pesäpaikalla."}},
        {"id": "http://tun.fi/MY.atlasCodeEnum62", "value": {"en": "62 Probable breeding; A bird or a pair observed building a nest", "sv": "62 Sannolik häckning; Fågel eller par setts bygga bo", "fi": "62 Todennäköinen pesintä: lintu tai pari rakentaa pesää tai vie pesämateriaalia."}},
        {"id": "http://tun.fi/MY.atlasCodeEnum63", "value": {"en": "63 Probable breeding; A bird or a pair observed giving alarm calls because of proximity to nest or brood", "fi": "63 Todennäköinen pesintä: lintu tai pari varoittelee ehkä pesästä tai poikueesta.", "sv": "63 Sannolik häckning; Fågel eller par setts varna för att bo eller kull uppenbarligen är i närheten"}},
        {"id": "http://tun.fi/MY.atlasCodeEnum64", "value": {"fi": "64 Todennäköinen pesintä: lintu tai pari houkuttelee pois ehkä pesältä / poikueelta.", "en": "64 Probable breeding; A bird or a pair observed displaying broken wing -act", "sv": "64 Sannolik häckning; Fågel eller par setts spelande vingskadad"}},
        {"id": "http://tun.fi/MY.atlasCodeEnum65", "value": {"en": "65 Probable breeding; A bird or a pair observed attacking the observer", "sv": "65 Sannolik häckning; Fågel eller par setts anfalla", "fi": "65 Todennäköinen pesintä: lintu tai pari hyökkäilee, lähellä ehkä pesä / poikue."}},
        {"id": "http://tun.fi/MY.atlasCodeEnum66", "value": {"en": "66 Todennäköinen pesintä: asuttu tai koristeltu pesä, ei tietoa munista / poikasista.", "fi": "66 Todennäköinen pesintä; Nähty pesä, jossa samanvuotista rakennusmateriaalia tai ravintojätettä; ei kuitenkaan varmaa todistetta munista tai poikasista", "sv": "66 Sannolik häckning; Fågel eller par setts bo iakttaget med samma års bobyggnadsmaterial eller födorester; men ej säkra bevis på ägg eller ungar"}},
        {"id": "http://tun.fi/MY.atlasCodeEnum7", "value": {"sv": "7 Säker häckning; Indirekt bevis på säker häckning konstaterat", "fi": "7 Varma pesintä: havaittu epäsuora todiste varmasta pesinnästä.", "en": "7 Confirmed breeding; Indirect evidence of verified breeding detected"}},
        {"id": "http://tun.fi/MY.atlasCodeEnum71", "value": {"en": "71 Confirmed breeding; Indirect evidence of verified breeding detected: nest found with signs indicating that is has been used in the same year", "fi": "71 Varma pesintä: nähty pesässä saman vuoden munia, kuoria, jäänteitä. Voi olla epäonnistunut.", "sv": "71 Säker häckning; Indirekt bevis på säker häckning konstaterat bo iakttaget där häckning ägt rum detta år, då boet innehöll ägg eller äggskal, lämningar av ungar, rester av fjäderslidor el. dyl"}},
        {"id": "http://tun.fi/MY.atlasCodeEnum72", "value": {"en": "72 Confirmed breeding; Indirect evidence of verified breeding detected: a bird seen entering or coming out from the nest in a way that suggests breeding", "fi": "72 Varma pesintä: käy pesällä pesintään viittaavasti. Munia / poikasia ei havaita (kolo tms.).", "sv": "72 Säker häckning; Indirekt bevis på säker häckning konstaterat: fågel iakttagen som besöker bo på ett sätt som klart pekar på häckning (ägg eller ungar dock ej sedda; t.ex. fåglar häckande i håligheter eller högt)"}},
        {"id": "http://tun.fi/MY.atlasCodeEnum73", "value": {"sv": "73 Säker häckning; Indirekt bevis på säker häckning konstaterat: nyligen flygga ungar eller dunungar observerade, när dessa kan anses vara födda i rutan", "fi": "73 Varma pesintä: juuri lentokykyiset poikaset tai untuvikot oletettavasti ruudulta.", "en": "73 Confirmed breeding; Indirect evidence of verified breeding detected: fledglings or young detected so that they can be assumed to have hatched within the grid"}},
        {"id": "http://tun.fi/MY.atlasCodeEnum74", "value": {"fi": "74 Varma pesintä: emo kantaa ruokaa tai poikasten ulosteita, pesintä oletettavasti ruudulla.", "sv": "74 Säker häckning; Indirekt bevis på säker häckning konstaterat: förälder iakttagen bärande föda till ungar, eller ungars avföring; boet kan antas ligga inom rutan", "en": "74 Confirmed breeding; Indirect evidence of verified breeding detected: a parent carrying food to nestlings or faeces of nestlings away from the nest"}},
        {"id": "http://tun.fi/MY.atlasCodeEnum75", "value": {"sv": "75 Säker häckning; Indirekt bevis på säker häckning konstaterat: förälder sedd ruvande i boet", "fi": "75 Varma pesintä; Havaittu epäsuora todiste varmasta pesinnästä: nähty pesässä hautova emo", "en": "75 Varma pesintä: nähty pesässä hautova emo."}},
        {"id": "http://tun.fi/MY.atlasCodeEnum8", "value": {"en": "8 Confirmed breeding; Direct evidence of verified breeding detected", "fi": "8 Varma pesintä: havaittu suora todiste varmasta pesinnästä.", "sv": "8 Säker häckning; Direkt bevis på säker häckning konstaterat"}},
        {"id": "http://tun.fi/MY.atlasCodeEnum81", "value": {"sv": "81 Säker häckning; Direkt bevis på säker häckning konstaterat: ungars läten hörda från boet", "fi": "81 Varma pesintä: kuultu poikasten ääntelyä pesässä (kolo / pesä korkealla).", "en": "81 Confirmed breeding; Direct evidence of a verified breeding detected: begging or other calls of nestlings heard from the nest"}},
        {"id": "http://tun.fi/MY.atlasCodeEnum82", "value": {"sv": "82 Säker häckning; Direkt bevis på säker häckning konstaterat: bo iakttaget med ägg eller ungar", "en": "82 Confirmed breeding; Direct evidence of a verified breeding detected: a nest found with eggs or nestlings", "fi": "82 Varma pesintä: nähty pesässä munia tai poikasia."}}
    ]

    id_to_finnish = {item['id']: item['value']['fi'] for item in data}
    return atlas_code_col.map(id_to_finnish)

def compute_variables(gdf):
    gdf['Atlasluokka'] = compute_var_from_id_atlas_class(gdf['Atlasluokka'])
    gdf['Atlaskoodi'] = compute_var_from_id_atlas_code(gdf['Atlaskoodi'])
    gdf['Ensisijainen_habitaatti'] = compute_var_from_id_primary_habitat(gdf['Ensisijainen_habitaatti'])
    return gdf
