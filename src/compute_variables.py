import pandas as pd
import numpy as np
from dotenv import load_dotenv
import geopandas as gpd

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

threatened_statuses = {
    "http://tun.fi/MX.threatenedStatusStatutoryProtected": "Lakisääteinen",
    "http://tun.fi/MX.threatenedStatusThreatened": "Uhanalainen",
    "http://tun.fi/MX.threatenedStatusNearThreatened": "Silmälläpidettävä",
    "http://tun.fi/MX.threatenedStatusOther": "Muu"
}

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

record_qualities = {
    "EXPERT_VERIFIED": "Asiantuntijan vahvistama",
    "COMMUNITY_VERIFIED": "Yhteisön varmistama",
    "NEUTRAL": "Ei arvioitu",
    "UNCERTAIN": "Epävarma",
    "ERRONEOUS": "Virheellinen"
    }

reasons = {
    "DEFAULT_TAXON_CONSERVATION" :"Lajitiedon sensitiivisyys",
    "NATURA_AREA_CONSERVATION":"Lajin paikkatiedot salataan Natura-alueella",
    "WINTER_SEASON_TAXON_CONSERATION":"Talvehtimishavainnot",
    "BREEDING_SEASON_TAXON_CONSERVATION":"Pesintäaika",
    "CUSTOM":"Tiedon tuottajan rajoittama aineisto",
    "USER_HIDDEN":"Käyttäjän karkeistamat nimi- ja/tai paikkatiedot",
    "DATA_QUARANTINE_PERIOD":"Tutkimusaineiston karenssiaika",
    "ONLY_PRIVATE":"Tiedon tuottaja on antanut aineiston vain viranomaiskäyttöön",
    "ADMIN_HIDDEN":"Ylläpitäjän karkeistama",
    "BREEDING_SITE_CONSERVATION":"Lisääntymispaikan sensitiivisyys (esimerkiksi pesä)",
    "USER_HIDDEN_LOCATION":"Käyttäjän karkeistamat paikkatiedot",
    "USER_HIDDEN_TIME":"Käyttäjän karkeistama aika",
    "USER_PERSON_NAMES_HIDDEN":"Henkilönimet piiloitettu"
    }

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

atlas_classes = { # Data from https://schema.laji.fi/alt/MY.atlasClassEnum
    "http://tun.fi/MY.atlasClassEnumA": "Epätodennäköinen pesintä",
    "http://tun.fi/MY.atlasClassEnumB": "Mahdollinen pesintä",
    "http://tun.fi/MY.atlasClassEnumC": "Todennäköinen pesintä",
    "http://tun.fi/MY.atlasClassEnumD": "Varma pesintä"
    }

atlas_codes =  {
    "http://tun.fi/MY.atlasCodeEnum1": "1 Epätodennäköinen pesintä: havaittu lajin yksilö, havainto ei viittaa pesintään.",
    "http://tun.fi/MY.atlasCodeEnum2": "2 Mahdollinen pesintä: yksittäinen lintu kerran, on sopivaa pesimäympäristöä.",
    "http://tun.fi/MY.atlasCodeEnum3": "3 Mahdollinen pesintä: pari kerran, on sopivaa pesimäympäristöä.",
    "http://tun.fi/MY.atlasCodeEnum4": "4 Todennäköinen pesintä: koiras reviirillä (esim. laulaa) eri päivinä.",
    "http://tun.fi/MY.atlasCodeEnum5": "5 Todennäköinen pesintä: naaras tai pari reviirillä eri päivinä.",
    "http://tun.fi/MY.atlasCodeEnum6": "6 Todennäköinen pesintä: linnun tai parin havainto viittaa vahvasti pesintään.",
    "http://tun.fi/MY.atlasCodeEnum61": "61 Todennäköinen pesintä: lintu tai pari käy usein todennäköisellä pesäpaikalla.",
    "http://tun.fi/MY.atlasCodeEnum62": "62 Todennäköinen pesintä: lintu tai pari rakentaa pesää tai vie pesämateriaalia.",
    "http://tun.fi/MY.atlasCodeEnum63": "63 Todennäköinen pesintä: lintu tai pari varoittelee ehkä pesästä tai poikueesta.",
    "http://tun.fi/MY.atlasCodeEnum64": "64 Todennäköinen pesintä: lintu tai pari houkuttelee pois ehkä pesältä / poikueelta.",
    "http://tun.fi/MY.atlasCodeEnum65": "65 Todennäköinen pesintä: lintu tai pari hyökkäilee, lähellä ehkä pesä / poikue.",
    "http://tun.fi/MY.atlasCodeEnum66": "66 Todennäköinen pesintä: lintu tai pari varoittelee ehkä pesästä tai poikueesta.",
    "http://tun.fi/MY.atlasCodeEnum7": "7 Varma pesintä: havaittu epäsuora todiste varmasta pesinnästä.",
    "http://tun.fi/MY.atlasCodeEnum71": "71 Varma pesintä: nähty pesässä saman vuoden munia, kuoria, jäänteitä. Voi olla epäonnistunut.",
    "http://tun.fi/MY.atlasCodeEnum72": "72 Varma pesintä: käy pesällä pesintään viittaavasti. Munia / poikasia ei havaita (kolo tms.).",
    "http://tun.fi/MY.atlasCodeEnum73": "73 Varma pesintä: juuri lentokykyiset poikaset tai untuvikot oletettavasti ruudulta.",
    "http://tun.fi/MY.atlasCodeEnum74": "74 Varma pesintä: emo kantaa ruokaa tai poikasten ulosteita, pesintä oletettavasti ruudulla.",
    "http://tun.fi/MY.atlasCodeEnum75": "75 Varma pesintä: nähty pesässä hautova emo.",
    "http://tun.fi/MY.atlasCodeEnum8": "8 Varma pesintä: havaittu suora todiste varmasta pesinnästä.",
    "http://tun.fi/MY.atlasCodeEnum81": "81 Varma pesintä: kuultu poikasten ääntelyä pesässä (kolo / pesä korkealla).",
    "http://tun.fi/MY.atlasCodeEnum82": "82 Varma pesintä: nähty pesässä munia tai poikasia."
}

abundance_units = {
    "INDIVIDUAL_COUNT": "Yksilömäärä",
    "PAIR_COUNT": "Parimäärä",
    "NEST": "Pesien lukumäärä",
    "BREEDING_SITE": "Lisääntymispaikkojen lukumäärä (kivi, kolo, ym)",
    "FEEDING_SITE": "Ruokailupaikkojen lukumäärä",
    "COLONY": "Yhdyskuntien lukumäärä",
    "FRUIT_BODY": "Itiöemien lukumäärä",
    "SPROUT": "Versojen lukumäärä",
    "HUMMOCK": "Mättäiden/tuppaiden lukumäärä",
    "THALLUS": "Sekovarsien lukumäärä",
    "FLOWER": "Kukkien lukumäärä",
    "SPOT": "Laikkujen lukumäärä",
    "TRUNK": "Runkojen lukumäärä",
    "QUEEN": "Kuningattarien lukumäärä",
    "SHELL": "Kuorien lukumäärä",
    "DROPPINGS": "Jätösten/papanakasojen lukumäärä",
    "MARKS": "(Syömä)jälkien lukumäärä",
    "INDIRECT": "Epäsuorien jälkien lukumäärä",
    "SQUARE_DM": "Neliödesimetri (dm^2)",
    "SQUARE_M": "Neliömetri (m^2)",
    "RELATIVE_DENSITY": "Suhteellinen tiheys",
    "OCCURS_DOES_NOT_OCCUR": "Esiintyy/ei esiinny"
}

sexes = {'MALE': 'Uros',
    'FEMALE': 'Naaras',
    'WORKER': 'Työläinen',
    'UNKNOWN': 'Tuntematon',
    'NOT_APPLICABLE': 'Soveltumaton',
    'GYNANDROMORPH': 'Gynandromorfi',
    'MULTIPLE': 'Eri sukupuolia',
    'CONFLICTING': 'Ristiriitainen'
}

collection_qualities = {
    'PROFESSIONAL':'Ammattiaineistot / asiantuntijoiden laadunvarmistama',
    'HOBBYIST':'Asiantuntevat harrastajat / asiantuntijoiden laadunvarmistama',
    'AMATEUR':'Kansalaishavaintoja / ei laadunvarmistusta'
}

def compute_individual_count(individual_count_col):
    """
    Determine whether the column gets a value 'paikalla' or 'poissa'.
    Keeps None or NaN values as they are.

    Parameters:
    individual_count_col (pd.Series): Column containing individual counts.

    Returns:
    pd.Series: Series with 'paikalla', 'poissa', or original NaN/None based on the individual count.
    """
    return np.where(individual_count_col.isna(), individual_count_col, 
                    np.where(individual_count_col > 0, 'paikalla', 'poissa'))

def compute_collection_id(collection_id_col, collection_names):
    """
    Computes collection names from collection ids

    Parameters:
    collection_id_col (Pandas.Series): Column to contain collection ids
    collection_names (Dict): Dictionary derived from a json containing collection IDs and corresponding names

    Returns:
    ids (Pandas.Series): Corresponding collection names
    """
    # Get only the IDs without URLs (e.g. 'http://tun.fi/HR.3553' to 'HR.3553')
    ids = pd.Series(collection_id_col.str.split('/').str[-1])

    # Map values
    ids = ids.map(collection_names, na_action='ignore')
    return ids

def map_values(cell):
    """
    Function to map the values if more than 1 value in a cell

    Parameters:
    cell (Dataframe item): Cell with multiple values to map
    values_dict (Dictionary): Mapping dictionary

    Returns:
    (str) Mapped values as a string
    """
    values = cell.split(', ')
    mapped_values = [regulatory_statuses.get(value, value) for value in values]
    return ', '.join(mapped_values)

def compute_areas(gdf_with_geom_and_ids, municipal_geojson):
    """
    Computes the municipalities and provinces for each row in the GeoDataFrame.

    Parameters:
    gdf_with_geom_and_ids (geopandas.GeoDataFrame): GeoDataFrame with geometry and IDs.
    municipal_geojson (str): Path to the GeoJSON file containing municipal geometries and corresponding ELY areas and provinces.

    Returns:
    pandas.Series: Series with municipalities for each row, separated by ',' if there are multiple areas.
    pandas.Series: Series with ely areas for each row, separated by ',' if there are multiple areas.
    """
    # Read the GeoJSON data
    municipal_gdf = gpd.read_file(municipal_geojson)
    gdf_with_geom_and_ids = gdf_with_geom_and_ids.copy()

    # Ensure both GeoDataFrames use the same coordinate reference system (CRS)
    if gdf_with_geom_and_ids.crs != municipal_gdf.crs:
        municipal_gdf = municipal_gdf.to_crs(gdf_with_geom_and_ids.crs)

    # Perform spatial join to find which areas each row is within
    joined_gdf = gpd.sjoin(gdf_with_geom_and_ids, municipal_gdf, how="left", predicate="intersects")

    # Group by the original indices and aggregate the area names
    municipalities = joined_gdf.groupby(joined_gdf.index)['Municipal_Name'].agg(lambda x: ', '.join(x.dropna().unique()))
    elys = joined_gdf.groupby(joined_gdf.index)['ELY_Area_Name'].agg(lambda x: ', '.join(x.dropna().unique()))

    # Ensure the resulting Series aligns with the original GeoDataFrame's indices
    municipalities = municipalities.reindex(gdf_with_geom_and_ids.index, fill_value='')
    elys = elys.reindex(gdf_with_geom_and_ids.index, fill_value='')

    return municipalities, elys

def get_title_name_from_table_name(table_name):
    """
    Converts table names back to the title names. E.g. 'sompion_lappi_polygons' -> 'Sompion Lappi'

    Parameters: 
    table_name (str): A PostGIS table name

    Returns: 
    cleaned_valuea (str): A cleaned version of a PostGIS table name
    """
    # Define a dictionary to map table names to cleaned values
    table_mapping = {
        "sompion_lappi": "Sompion Lappi",
        "satakunta": "Satakunta",
        "pohjois_savo": "Pohjois-Savo",
        "pera_pohjanmaa": "Perä-Pohjanmaa",
        "laatokan_karjala": "Laatokan Karjala",
        "kittilan_lappi": "Kittilän Lappi",
        "keski_pohjanmaa": "Keski-Pohjanmaa",
        "kainuu": "Kainuu",
        "etela_hame": "Etelä-Häme",
        "enontekion_lappi": "Enontekiön Lappi",
        "ahvenanmaa": "Ahvenanmaa",
        "etela_savo": "Etelä-Savo",
        "etela_karjala": "Etelä-Karjala",
        "varsinais_suomi": "Varsinais-Suomi",
        "pohjois_hame": "Pohjois-Häme",
        "koillismaa": "Koillismaa",
        "uusimaa": "Uusimaa",
        "oulun_pohjanmaa": "Oulun Pohjanmaa",
        "inarin_lappi": "Inarin Lappi",
        "etela_pohjanmaa": "Etelä-Pohjanmaa",
        "pohjois_karjala": "Pohjois-Karjala"
    }

    # Remove the data type (e.g., points, polygons, lines)
    base_name = table_name.rsplit('_', 1)[0]
    
    # Look up the cleaned value in the dictionary
    cleaned_value = table_mapping.get(base_name, "Unknown table name")
    
    return cleaned_value

def compute_all(gdf, collection_names, municipal_geojson_path):
    '''
    Computes or translates variables that can not be directly accessed from the source API
   
    Parameters:
    gdf (geopandas.GeoDataFrame): GeoDataFrame containing occurrences.
    collection_names (dict): The dictionary containing all collection IDs and their long names
    municipal_geojson_path (str): Path to the GeoJSON file containing municipal geometries.

    Returns:
    gdf (geopandas.GeoDataFrame): GeoDataFrame containing occurrences and computed columns.
    '''
    # Create a dictionary to store all new values
    all_cols = {}

    # Direct mappings:
    if 'unit.atlasClass' in gdf.columns:
        all_cols['unit.atlasClass'] = gdf['unit.atlasClass'].map(atlas_classes, na_action='ignore')

    if 'unit.atlasCode' in gdf.columns:
        all_cols['unit.atlasCode'] = gdf['unit.atlasCode'].map(atlas_codes, na_action='ignore')

    if 'unit.linkings.originalTaxon.primaryHabitat.habitat' in gdf.columns:
        all_cols['unit.linkings.originalTaxon.primaryHabitat.habitat'] = gdf['unit.linkings.originalTaxon.primaryHabitat.habitat'].map(habitat_dict)
   
    if 'unit.linkings.originalTaxon.latestRedListStatusFinland.status' in gdf.columns:
        all_cols['unit.linkings.originalTaxon.latestRedListStatusFinland.status'] = gdf['unit.linkings.originalTaxon.latestRedListStatusFinland.status'].map(red_list_statuses, na_action='ignore') 
    
    if 'unit.linkings.taxon.threatenedStatus' in gdf.columns:
        all_cols['unit.linkings.taxon.threatenedStatus'] = gdf['unit.linkings.taxon.threatenedStatus'].map(threatened_statuses, na_action='ignore')
    
    if 'unit.recordBasis' in gdf.columns:
        all_cols['unit.recordBasis'] = gdf['unit.recordBasis'].map(record_basis, na_action='ignore')
    
    if 'unit.interpretations.recordQuality' in gdf.columns:
        all_cols['unit.interpretations.recordQuality'] = gdf['unit.interpretations.recordQuality'].map(record_qualities, na_action='ignore')
    
    if 'document.secureReasons' in gdf.columns:
        all_cols['document.secureReasons'] = gdf['document.secureReasons'].map(reasons, na_action='ignore')
    
    if 'unit.sex' in gdf.columns:
        all_cols['unit.sex'] = gdf['unit.sex'].map(sexes, na_action='ignore')
    
    if 'unit.abundanceUnit' in gdf.columns:
        all_cols['unit.abundanceUnit'] = gdf['unit.abundanceUnit'].map(abundance_units, na_action='ignore')
    
    if 'document.linkings.collectionQuality' in gdf.columns:
        all_cols['document.linkings.collectionQuality'] = gdf['document.linkings.collectionQuality'].map(collection_qualities, na_action='ignore')

    # Remove endings from biogeographical regions
    if 'gathering.interpretations.biogeographicalProvinceDisplayname' in gdf.columns:
        all_cols['gathering.interpretations.biogeographicalProvinceDisplayname'] = gdf['gathering.interpretations.biogeographicalProvinceDisplayname'].str.split(r"\s*\(").str[0]

    # Mappings with multiple value in a cell:
    all_cols['unit.linkings.originalTaxon.administrativeStatuses'] = gdf['unit.linkings.originalTaxon.administrativeStatuses'].apply(map_values)

    # Computed values from different source
    all_cols['compute_from_individual_count'] = compute_individual_count(gdf['unit.interpretations.individualCount']) 
    all_cols['compute_from_collection_id'] = compute_collection_id(gdf['document.collectionId'], collection_names) 

    municipal_col, vastuualue_col = compute_areas(gdf[['unit.unitId', 'geometry']], municipal_geojson_path)
    all_cols['computed_municipality'] = municipal_col.astype('str')
    all_cols['computed_ely_area'] = vastuualue_col.astype('str')

    # Create a dataframe to join
    computed_cols_df = pd.DataFrame(all_cols, dtype="str")

    # Drop duplicate columns
    columns_to_drop = []
    for column in computed_cols_df.columns:
        if column in gdf.columns:
            columns_to_drop.append(column)
    gdf.drop(columns_to_drop, axis=1, inplace=True)

    # Concatenate computed columns to gdf
    gdf = pd.concat([gdf, computed_cols_df], axis=1)

    return gdf