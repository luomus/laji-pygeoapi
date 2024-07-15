import pandas as pd
import numpy as np
from dotenv import load_dotenv
import requests, json, os
import geopandas as gpd

def compute_var_from_id_threatened_status(threatened_status_column):
    threatened_statuses = {
    "http://tun.fi/MX.threatenedStatusStatutoryProtected": "Lakisääteinen",
    "http://tun.fi/MX.threatenedStatusThreatened": "Uhanalainen",
    "http://tun.fi/MX.threatenedStatusNearThreatened": "Silmälläpidettävä",
    "http://tun.fi/MX.threatenedStatusOther": "Muu"
    }
    threatened_status_column = threatened_status_column.map(threatened_statuses)
    return threatened_status_column

def compute_var_red_list_status(red_list_column):
    red_list_statuses = {
    "http://tun.fi/MX.iucnEX": "EX - Sukupuuttoon kuolleet",
    "http://tun.fi/MX.iucnEW": "EW - Luonnosta hävinneet",
    "http://tun.fi/MX.iucnRE": "RE - Suomesta hävinneet",
    "http://tun.fi/MX.iucnCR": "CR - Äärimmäisen uhanalaiset",
    "http://tun.fi/MX.iucnEN": "EN - Erittäin uhanalaiset",
    "http://tun.fi/MX.iucnVU": "VU - Vaarantuneet",
    "http://tun.fi/MX.iucnNT": "NT - Silmälläpidettävät",
    "http://tun.fi/MX.iucnLC": "LC - Elinvoimaiset",
    "http://tun.fi/MX.iucnDD": "DD - Puutteellisesti tunnetut",
    "http://tun.fi/MX.iucnNA": "NA - Arviointiin soveltumattomat",
    "http://tun.fi/MX.iucnNE": "NE - Arvioimatta jätetyt"
    }

    red_list_column = red_list_column.map(red_list_statuses)
    return red_list_column

def compute_var_from_id_regulatory_status(regulatory_status_column):

    regulatory_statuses = {
        "http://tun.fi/MX.finlex160_1997_appendix4_2021": "Uhanalaiset lajit (LSA 2023/1066, liite 6)",
        "http://tun.fi/MX.finlex160_1997_appendix4_specialInterest_2021": "Erityisesti suojeltavat lajit (LSA 2023/1066, liite 6)",
        "http://tun.fi/MX.finlex160_1997_appendix2a": "Koko maassa rauhoitetut eläinlajit (LSA 2023/1066, liite 1)",
        "http://tun.fi/MX.finlex160_1997_appendix2b": "Pohjois-Pohjanmaan ja Kainuun maakuntien eteläpuolella rauhoitetut eläinlajit (LSA 2023/1066, liite 2)",
        "http://tun.fi/MX.finlex160_1997_appendix3a": "Koko maassa rauhoitetut kasvilajit (LSA 2023/1066, liite 3)",
        "http://tun.fi/MX.finlex160_1997_appendix3b": "Pohjois-Pohjanmaan ja Kainuun maakuntien eteläpuolella rauhoitetut putkilokasvit (LSA 2023/1066, liite 4)",
        "http://tun.fi/MX.finlex160_1997_appendix3c": "Pohjois-Pohjanmaan, Kainuun ja Lapin maakunnissa rauhoitetut putkilokasvit (LSA 2023/1066, liite 5)",
        "http://tun.fi/MX.finlex160_1997_largeBirdsOfPrey": "Suuret petolinnut (LSL 2023/9, 73 §)",
        "http://tun.fi/MX.finlex1066_2023_appendix7": "Suomessa esiintyvät Euroopan unionin tiukkaa suojelua edellyttävät eliölajit  (LSA 2023/1066, liite 7)",
        "http://tun.fi/MX.habitatsDirectiveAnnexII": "EU:n luontodirektiivin II-liite",
        "http://tun.fi/MX.habitatsDirectiveAnnexIV": "EU:n luontodirektiivin IV-liite",
        "http://tun.fi/MX.habitatsDirectiveAnnexV": "EU:n luontodirektiivin V-liite",
        "http://tun.fi/MX.primaryInterestInEU": "EU:n ensisijaisesti suojeltavat lajit (luontodirektiivin II-liite)",
        "http://tun.fi/MX.habitatsDirectiveAnnexIIExceptionGranted": "EU:n luontodirektiivin II-liite, Suomi saanut varauman koskien tätä lajia",
        "http://tun.fi/MX.habitatsDirectiveAnnexII_FinlandNaturaSpecies": "EU:n luontodirektiivin liite II, Suomen Natura-lajit",
        "http://tun.fi/MX.habitatsDirectiveAnnexIVExceptionGranted": "EU:n luontodirektiivin IV-liite, Suomi saanut varauman koskien tätä lajia",
        "http://tun.fi/MX.habitatsDirectiveAnnexVExceptionGranted": "EU:n luontodirektiivin V-liite, Suomi saanut varauman koskien tätä lajia",
        "http://tun.fi/MX.birdsDirectiveStatusAppendix1": "EU:n lintudirektiivin I-liite",
        "http://tun.fi/MX.birdsDirectiveStatusAppendix2A": "EU:n lintudirektiivin II/A-liite",
        "http://tun.fi/MX.birdsDirectiveStatusAppendix2B": "EU:n lintudirektiivin II/B-liite",
        "http://tun.fi/MX.birdsDirectiveStatusAppendix3A": "EU:n lintudirektiivin III/A-liite",
        "http://tun.fi/MX.birdsDirectiveStatusAppendix3B": "EU:n lintudirektiivin III/B-liite",
        "http://tun.fi/MX.birdsDirectiveStatusMigratoryBirds": "EU:n lintudirektiivin muuttolinnut",
        "http://tun.fi/MX.cites_appendixI": "CITES-sopimus, liite I",
        "http://tun.fi/MX.cites_appendixII": "CITES-sopimus, liite II",
        "http://tun.fi/MX.cites_appendixIII": "CITES-sopimus, liite III",
        "http://tun.fi/MX.euRegulation_cites_appendixA": "EU-lainsäädäntö koskien CITES-sopimusta, liite A",
        "http://tun.fi/MX.euRegulation_cites_appendixB": "EU-lainsäädäntö koskien CITES-sopimusta, liite B",
        "http://tun.fi/MX.euRegulation_cites_appendixD": "EU-lainsäädäntö koskien CITES-sopimusta, liite D",
        "http://tun.fi/MX.finnishEnvironmentInstitute2020protectionPrioritySpecies": "Kiireellisesti suojeltavat lajit (SYKE 2020)",
        "http://tun.fi/MX.finnishEnvironmentInstitute2010protectionPrioritySpecies": "VANHA Kiireellisesti suojeltavat lajit (SYKE 2010-2011)",
        "http://tun.fi/MX.gameBird": "Riistalintu (Metsästyslaki 1993/615)",
        "http://tun.fi/MX.gameMammal": "Riistanisäkäs (Metsästyslaki 1993/615; 2019/683)",
        "http://tun.fi/MX.unprotectedSpecies": "Rauhoittamaton eläin (Metsästyslaki 1993/615)",
        "http://tun.fi/MX.nationallySignificantInvasiveSpecies": "Haitallinen vieraslaji (Kansallinen luettelo) (VN 1725/2015)",
        "http://tun.fi/MX.euInvasiveSpeciesList": "EU:ssa haitalliseksi säädetty vieraslaji (EU:n vieraslajiluettelo) (EU 2016/1141; 2017/1263; 2019/1262; 2022/1203)",
        "http://tun.fi/MX.quarantinePlantPest": "Karanteenituhooja",
        "http://tun.fi/MX.qualityPlantPest": "Laatutuhooja",
        "http://tun.fi/MX.otherPlantPest": "Muu kasvintuhooja",
        "http://tun.fi/MX.nationalInvasiveSpeciesStrategy": "Kansallinen vieraslajistrategia (VN 2012)",
        "http://tun.fi/MX.otherInvasiveSpeciesList": "Muu vieraslaji",
        "http://tun.fi/MX.controllingRisksOfInvasiveAlienSpecies": "Kansallisesti haitalliseksi säädetty vieraslaji (Kansallinen vieraslajiluettelo) (VN 704/2019, VN 912/2023)",
        "http://tun.fi/MX.finnishEnvironmentInstitute20072010forestSpecies": "VANHA Uhanalaisten lajien turvaaminen metsätaloudessa -hankkeessa 2007-2010 laadittu metsälajiluettelo",
        "http://tun.fi/MX.finnishEnvironmentInstitute2020conservationProjectSpecies": "Metsäisten suojelualueiden konnektiviteetti - SUMI-hankkeessa 2020 laadittu metsälajiluettelo",
        "http://tun.fi/MX.finnishEnvironmentInstitute2020conservationProjectAapamireSpecies": "Aapasuolajit - SUMI-hankkeessa 2020 laadittu uhanalaisten tai silmälläpidettävien aapasuolajien luettelo",
        "http://tun.fi/MX.finnishEnvironmentInstitute2020conservationProjectVascularSpecies": "Putkilokasvien toiminnallinen monimuotoisuus - SUMI-hankkeessa 2022 laadittu luettelo",
        "http://tun.fi/MX.cropWildRelative": "Viljelykasvien luonnonvarainen sukulainen (CWR Prioriteettilaji)",
        "http://tun.fi/MX.finnishEnvironmentInstitute20192021forestSpecies": "Uhanalaisten lajien esiintymien turvaaminen metsätaloudessa - Lajiturva-hankkeessa 2019-2021 laadittu lajiluettelo",
        "http://tun.fi/MX.forestCentreSpecies": "Metsänkäyttöilmoitusten automaattimenettelyssä käytettävä lajiluettelo (MKI-OHKE 2023)",
        "http://tun.fi/MX.regionallyThreatened2020_1a": "Alueellisesti uhanalainen 2020 - 1a Hemiboreaalinen, Ahvenanmaa",
        "http://tun.fi/MX.regionallyThreatened2020_1b": "Alueellisesti uhanalainen 2020 - 1b Hemiboreaalinen, Lounainen rannikkomaa",
        "http://tun.fi/MX.regionallyThreatened2020_2a": "Alueellisesti uhanalainen 2020 - 2a Eteläboreaalinen, Lounaismaa ja Pohjanmaan rannikko",
        "http://tun.fi/MX.regionallyThreatened2020_2b": "Alueellisesti uhanalainen 2020 - 2b Eteläboreaalinen, Järvi-Suomi",
        "http://tun.fi/MX.regionallyThreatened2020_3a": "Alueellisesti uhanalainen 2020 - 3a Keskiboreaalinen, Pohjanmaa",
        "http://tun.fi/MX.regionallyThreatened2020_3b": "Alueellisesti uhanalainen 2020 - 3b Keskiboreaalinen, Pohjois-Karjala-Kainuu",
        "http://tun.fi/MX.regionallyThreatened2020_3c": "Alueellisesti uhanalainen 2020 - 3c Keskiboreaalinen, Lapin kolmio",
        "http://tun.fi/MX.regionallyThreatened2020_4a": "Alueellisesti uhanalainen 2020 - 4a Pohjoisboreaalinen, Koillismaa",
        "http://tun.fi/MX.regionallyThreatened2020_4b": "Alueellisesti uhanalainen 2020 - 4b Pohjoisboreaalinen, Perä-Pohjola",
        "http://tun.fi/MX.regionallyThreatened2020_4c": "Alueellisesti uhanalainen 2020 - 4c Pohjoisboreaalinen, Metsä-Lappi",
        "http://tun.fi/MX.regionallyThreatened2020_4d": "Alueellisesti uhanalainen 2020 - 4d Pohjoisboreaalinen, Tunturi-Lappi",
        "http://tun.fi/MX.finlex160_1997_appendix1": "VANHA Kalalajit, joihin sovelletaan luonnonsuojelulakia (LSA 1997/160, liite 1)",
        "http://tun.fi/MX.finlex160_1997_appendix4": "VANHA Uhanalaiset lajit (LSA 1997/160, liite 4 2013/471)",
        "http://tun.fi/MX.finlex160_1997_appendix4_specialInterest": "VANHA Erityisesti suojeltavat lajit (LSA 1997/160, liite 4 2013/471)"
    }


    # Function to map the values
    def map_values(cell):
        values = cell.split(', ')
        mapped_values = [regulatory_statuses.get(value, value) for value in values]
        return '; '.join(mapped_values)

    # Apply the function to the dataframe
    regulatory_status_column = regulatory_status_column.apply(map_values)

    return regulatory_status_column

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

def compute_var_from_individual_count(individual_count_col):
    return individual_count_col.apply(lambda x: 'paikalla' if x > 0 else 'poissa')

def compute_var_from_record_basis(record_basis_col):
    record_basis = {
        "PRESERVED_SPECIMEN": "Näyte",
        "LIVING_SPECIMEN": "Elävä näyte",
        "FOSSIL_SPECIMEN": "Fossiili",
        "SUBFOSSIL_SPECIMEN": "Subfossiili",
        "SUBFOSSIL_AMBER_INCLUSION_SPECIMEN": "Meripihkafossiili",
        "MICROBIAL_SPECIMEN": "Mikrobinäyte",
        "HUMAN_OBSERVATION_UNSPECIFIED": "Havaittu",
        "HUMAN_OBSERVATION_SEEN": "Nähty",
        "HUMAN_OBSERVATION_HEARD": "Kuultu",
        "HUMAN_OBSERVATION_PHOTO": "Valokuvattu",
        "HUMAN_OBSERVATION_INDIRECT": "Epäsuora havainto (jäljet, ulosteet, yms)",
        "HUMAN_OBSERVATION_HANDLED": "Käsitelty (otettu kiinni, ei näytettä)",
        "HUMAN_OBSERVATION_VIDEO": "Videoitu",
        "HUMAN_OBSERVATION_RECORDED_AUDIO": "Äänitetty",
        "MACHINE_OBSERVATION_UNSPECIFIED": "Laitteen tekemä havainto",
        "MACHINE_OBSERVATION_PHOTO": "Laitteen tekemä havainto, valokuva",
        "MACHINE_OBSERVATION_VIDEO": "Laitteen tekemä havainto, video",
        "MACHINE_OBSERVATION_AUDIO": "Laitteen tekemä havainto, ääni",
        "MACHINE_OBSERVATION_GEOLOGGER": "Geopaikannin",
        "MACHINE_OBSERVATION_SATELLITE_TRANSMITTER": "Satelliittipaikannus",
        "LITERATURE": "Kirjallisuustieto",
        "MATERIAL_SAMPLE": "Materiaalinäyte",
        "MATERIAL_SAMPLE_AIR": "Materiaalinäyte: ilmanäyte",
        "MATERIAL_SAMPLE_SOIL": "Materiaalinäyte: maaperänäyte",
        "MATERIAL_SAMPLE_WATER": "Materiaalinäyte: vesinäyte"
    }

    return record_basis_col.map(record_basis)

def compute_var_from_collection_id(collection_id_col, collection_names):
    # Get only the IDs without URLs (e.g. 'http://tun.fi/HR.3553' to 'HR.3553')
    ids = pd.Series(collection_id_col.str.split('/').str[-1])

    # Map values
    ids = ids.map(collection_names)
    return ids

def compute_ely_area(gdf_with_geom_and_ids, ely_geojson_path):
    """
    Computes the ELY areas for each row in the GeoDataFrame.

    Parameters:
    gdf_with_geom_and_ids (geopandas.GeoDataFrame): GeoDataFrame with geometry and IDs.
    ely_geojson_path (str): Path to the GeoJSON file containing ELY area geometries.

    Returns:
    pandas.Series: Series with ELY areas for each row, separated by ';' if there are multiple areas.
    """
    # Read the ELY areas GeoJSON data
    ely_gdf = gpd.read_file(ely_geojson_path)
    gdf_with_geom_and_ids = gdf_with_geom_and_ids.copy()

    # Ensure both GeoDataFrames use the same coordinate reference system (CRS)
    if gdf_with_geom_and_ids.crs != ely_gdf.crs:
        ely_gdf = ely_gdf.to_crs(gdf_with_geom_and_ids.crs)

    # Perform spatial join to find which ELY areas each row is within
    joined_gdf = gpd.sjoin(gdf_with_geom_and_ids, ely_gdf, how="left", predicate="within")

    # Group by the original indices and aggregate the ELY area names
    ely_areas = joined_gdf.groupby(joined_gdf.index)['nimi'].agg(lambda x: '; '.join(x.dropna().unique()))

    # Ensure the resulting Series aligns with the original GeoDataFrame's indices
    ely_areas = ely_areas.reindex(gdf_with_geom_and_ids.index, fill_value='')

    return ely_areas


def compute_variables(gdf, collection_names, ely_geojson_path):
    # Get "Atlasluokka"
    if 'unit.atlasClass' in gdf.columns:
        gdf['unit.atlasClass'] = compute_var_from_id_atlas_class(gdf['unit.atlasClass'])
    else:
        gdf['unit.atlasClass'] = None

    # Get "Atlaskoodi"
    if 'unit.atlasCode' in gdf.columns:
        gdf['unit.atlasCode'] = compute_var_from_id_atlas_code(gdf['unit.atlasCode'])
    else:
        gdf['unit.atlasCode'] = None

    # Get "Ensisijainen_biotooppi"
    if 'unit.linkings.originalTaxon.primaryHabitat.habitat' in gdf.columns:
        gdf['unit.linkings.originalTaxon.primaryHabitat.habitat'] = compute_var_from_id_primary_habitat(gdf['unit.linkings.originalTaxon.primaryHabitat.habitat'])
    else:
        gdf['unit.linkings.originalTaxon.primaryHabitat.habitat'] = None
    
    # Get "Uhanalaisuusluokka"
    if 'unit.linkings.originalTaxon.latestRedListStatusFinland.status' in gdf.columns:
        gdf['unit.linkings.originalTaxon.latestRedListStatusFinland.status'] = compute_var_red_list_status(gdf['unit.linkings.originalTaxon.latestRedListStatusFinland.status'])
    else:
        gdf['unit.linkings.originalTaxon.latestRedListStatusFinland.status'] = None

    # Get "Lajiturva"
    if 'unit.linkings.taxon.threatenedStatus' in gdf.columns:
        gdf['unit.linkings.taxon.threatenedStatus'] = compute_var_from_id_threatened_status(gdf['unit.linkings.taxon.threatenedStatus'])
    else:
        gdf['unit.linkings.taxon.threatenedStatus'] = None

    # Get "Hallinnollinen_asema"
    if 'unit.linkings.originalTaxon.administrativeStatuses' in gdf.columns:
        gdf['unit.linkings.originalTaxon.administrativeStatuses'] = compute_var_from_id_regulatory_status(gdf['unit.linkings.originalTaxon.administrativeStatuses'])
    else:
        gdf['unit.linkings.originalTaxon.administrativeStatuses'] = None

    # Get 'Esiintyman_tila'
    if 'unit.interpretations.individualCount' in gdf.columns:
        gdf['compute_from_individual_count'] = compute_var_from_individual_count(gdf['unit.interpretations.individualCount']) # Note: calculated from different column
    else:
        gdf['compute_from_individual_count'] = None

    # Get 'Havaintotapa'
    if 'unit.recordBasis' in gdf.columns:
        gdf['unit.recordBasis'] = compute_var_from_record_basis(gdf['unit.recordBasis'])
    else:
        gdf['unit.recordBasis'] = None

    # Get 'Aineisto'
    if 'document.collectionId' in gdf.columns:
        gdf['compute_from_collection_id'] = compute_var_from_collection_id(gdf['document.collectionId'], collection_names) # Note: calculated from different column
    else:
        gdf['compute_from_collection_id'] = None

    # Get 'Vastuualue'
    gdf['Vastuualue'] = compute_ely_area(gdf[['unit.unitId','geometry']], ely_geojson_path)
    
    return gdf