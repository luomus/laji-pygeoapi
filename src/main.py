import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
import pyogrio, psycopg2, geoalchemy2, os
import process_data, edit_config, load_data, edit_configmaps, edit_db
from psycopg2 import sql

# Set options for pandas and geopandas
pd.options.mode.copy_on_write = True
gpd.options.io_engine = "pyogrio" # Faster way to read data


# URLs and file paths
load_dotenv()
API_access_token =  os.getenv('ACCESS_TOKEN')
taxon_id_url= f'https://api.laji.fi/v0/taxa/MX.37600/species?onlyFinnish=true&selectedFields=id,vernacularName,scientificName,informalTaxonGroups&lang=multi&page=1&pageSize=1000&sortOrder=taxonomic&access_token={API_access_token}'
taxon_name_url = f'https://api.laji.fi/v0/informal-taxon-groups?pageSize=1000&access_token={API_access_token}'
occurrence_url = f'https://api.laji.fi/v0/warehouse/query/unit/list?administrativeStatusId=MX.finlex160_1997_appendix4_2021,MX.finlex160_1997_appendix4_specialInterest_2021,MX.finlex160_1997_appendix2a,MX.finlex160_1997_appendix2b,MX.finlex160_1997_appendix3a,MX.finlex160_1997_appendix3b,MX.finlex160_1997_appendix3c,MX.finlex160_1997_largeBirdsOfPrey,MX.habitatsDirectiveAnnexII,MX.habitatsDirectiveAnnexIV,MX.birdsDirectiveStatusAppendix1,MX.birdsDirectiveStatusMigratoryBirds&redListStatusId=MX.iucnCR,MX.iucnEN,MX.iucnVU,MX.iucnNT&countryId=ML.206&time=1990-01-01/&aggregateBy=gathering.conversions.wgs84Grid05.lat,gathering.conversions.wgs84Grid1.lon&onlyCount=false&individualCountMin=0&coordinateAccuracyMax=1000&page=1&pageSize=10000&taxonAdminFiltersOperator=OR&collectionAndRecordQuality=PROFESSIONAL:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL,UNCERTAIN;HOBBYIST:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL;AMATEUR:EXPERT_VERIFIED,COMMUNITY_VERIFIED;&geoJSON=true&featureType=ORIGINAL_FEATURE&access_token={API_access_token}'
template_resource = r'template_resource.txt'
pygeoapi_config = r'pygeoapi-config.yml'
lookup_table = r'lookup_table_columns.csv'
# taxon_id_file = r'test_data/taxon-export.tsv' # For local testing

if os.getenv('RUNNING_IN_OPENSHIFT') == "True":
    pygeoapi_config_out = r'pygeoapi-config_out.yml'
else:
    pygeoapi_config_out = r'pygeoapi-config.yml'



def main():
    """
    Main function to load, process data, insert it into the database, and prepare the API configuration.
    """
    print("Start")

    tot_rows = 0 
    no_family_name = gpd.GeoDataFrame()
    multiprocessing = os.getenv('MULTIPROCESSING')
    pages = os.getenv('PAGES')
    engine = edit_db.connect_to_db()

    # Clear config file and database to make space for new data sets. 
    edit_config.clear_collections_from_config(pygeoapi_config, pygeoapi_config_out)
    edit_db.drop_all_tables(engine)

    # Get taxon data
    taxon_df = load_data.get_taxon_data(taxon_id_url, taxon_name_url, pages='all')
    #taxon_df = pd.read_csv('taxon-export.csv') # For local testing


    table_names = []

    # Determine the number of pages to process 
    if pages.lower() == "all":
        pages = load_data.get_last_page(occurrence_url)
    pages = int(pages)

    # Load and process data in batches of 10 pages. Store to the database
    for startpage in range(1, pages+1, 10):
        if startpage < pages-9:
            endpage = startpage + 9
        else:
            endpage = pages

        # Get 10 pages of occurrence data
        gdf = load_data.get_occurrence_data(occurrence_url, multiprocessing=multiprocessing, startpage=startpage, pages=endpage) 

        # Merge taxonomy information with the occurrence data
        gdf = process_data.merge_taxonomy_data(gdf, taxon_df)

        # Translate column names to comply with the Darwin Core standard
        gdf = process_data.column_names_to_dwc(gdf, lookup_table)

        # Extract entries without family names
        no_family_name = gdf[gdf['InformalGroupName'].isnull()]

        print("Looping over all species classes...")

        # Process each unique species group (e.g. Birds, Mammals etc.)
        for group_name in gdf['InformalGroupName'].unique():
            sub_gdf = gdf[gdf['InformalGroupName'] == group_name]

            # Get cleaned table name
            table_name = process_data.clean_table_name(group_name)
            table_names.append(table_name)

            # Skip nans and unclassified
            if table_name != 'unclassified' and table_name != 'nan':

                # Create evenDateTimeDisplay column and local ID
                sub_gdf['eventDateTimeDisplay'] = process_data.convert_dates(sub_gdf).astype(str)
                sub_gdf['localID'] = sub_gdf.index
                n_rows = len(sub_gdf)
                tot_rows += n_rows

                # Add data to the database
                sub_gdf.rename_geometry("geom")
                sub_gdf.to_postgis(table_name, engine, if_exists='append', schema='public', index=False)
                
                print(f"In total {n_rows} rows of {table_name} inserted to the database")


    # Update pygeoapi configuration
    for table_name in table_names:
        if table_name != 'unclassified' and table_name != 'nan':
            bbox = edit_db.get_table_bbox(engine, table_name)
            min_date, max_date = edit_db.get_table_dates(engine, table_name)

            # Create parameters dictionary to fill the template for pygeoapi config file
            template_params = {
                "<placeholder_table_name>": table_name,
                "<placeholder_bbox>": str(bbox),
                "<placeholder_min_date>": min_date,
                "<placeholder_max_date>": max_date,
                "<placeholder_postgres_host>": os.getenv('POSTGRES_HOST'),
                "<placeholder_postgres_password>": os.getenv('POSTGRES_PASSWORD'),
                "<placeholder_postgres_user>": os.getenv('POSTGRES_USER'),
                "<placeholder_db_name>": os.getenv('POSTGRES_DB')
            }
            # Add database information into the config file
            edit_config.add_to_pygeoapi_config(template_resource, template_params, pygeoapi_config_out)

    print(f"\nIn total {tot_rows} rows of data inserted successfully into the PostGIS database and pygeoapi config file.")
    print(f"Warning: in total {len(no_family_name)} species without scientific family name were discarded")

    # And finally replace configmap in openshift with the local config file only when the script is running in kubernetes / openshift
    if os.getenv('RUNNING_IN_OPENSHIFT') == "True":
        edit_configmaps.update_configmap(pygeoapi_config) 
        
    print("API is ready to use. ")

if __name__ == '__main__':
    main()