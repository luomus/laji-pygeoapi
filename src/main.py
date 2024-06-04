import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pyogrio, psycopg2, geoalchemy2, os
import process_data, edit_config, load_data, edit_configmaps, edit_db

# Set options for pandas and geopandas
pd.options.mode.copy_on_write = True
gpd.options.io_engine = "pyogrio" # Faster way to read data

# URLs and file paths
occurrence_url = r'https://laji.fi/api/warehouse/query/unit/list?administrativeStatusId=MX.finlex160_1997_appendix4_2021,MX.finlex160_1997_appendix4_specialInterest_2021,MX.finlex160_1997_appendix2a,MX.finlex160_1997_appendix2b,MX.finlex160_1997_appendix3a,MX.finlex160_1997_appendix3b,MX.finlex160_1997_appendix3c,MX.finlex160_1997_largeBirdsOfPrey,MX.habitatsDirectiveAnnexII,MX.habitatsDirectiveAnnexIV,MX.birdsDirectiveStatusAppendix1,MX.birdsDirectiveStatusMigratoryBirds&redListStatusId=MX.iucnCR,MX.iucnEN,MX.iucnVU,MX.iucnNT&countryId=ML.206&time=1990-01-01/&aggregateBy=gathering.conversions.wgs84Grid05.lat,gathering.conversions.wgs84Grid1.lon&onlyCount=false&individualCountMin=0&coordinateAccuracyMax=1000&page=1&pageSize=10000&taxonAdminFiltersOperator=OR&collectionAndRecordQuality=PROFESSIONAL:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL,UNCERTAIN;HOBBYIST:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL;AMATEUR:EXPERT_VERIFIED,COMMUNITY_VERIFIED;&geoJSON=true&featureType=ORIGINAL_FEATURE'
taxon_id_url= r'https://laji.fi/api/taxa/MX.37600/species?onlyFinnish=true&selectedFields=id,vernacularName,scientificName,informalTaxonGroups&lang=multi&page=1&pageSize=1000&sortOrder=taxonomic'
taxon_name_url = r'https://laji.fi/api/informal-taxon-groups?pageSize=1000'
template_resource = r'template_resource.txt'
pygeoapi_config = r'pygeoapi-config.yml'
lookup_table = r'lookup_table_columns.csv'
# taxon_id_file = r'test_data/taxon-export.tsv' # For local testing


# Load environment variables from .env file
load_dotenv()
db_params = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('POSTGRES_HOST'),
    'port': '5432'
}

def main():
    """
    Main function to do everything. Loads and process data, insert it to database and prepare API configuration.
    """
    # Get the data sets
    pages = os.getenv('PAGES')
    multiprocessing = os.getenv('MULTIPROCESSING')
    gdf = load_data.get_occurrence_data(occurrence_url, multiprocessing=multiprocessing, pages=pages) 
    taxon_df = load_data.get_taxon_data(taxon_id_url, taxon_name_url, pages='all') 
    #taxon_df = pd.read_csv('taxon-export.csv') # For local testing

    # Merge taxonomy information to the occurrence data
    gdf = process_data.merge_taxonomy_data(gdf, taxon_df)

    # Translate column names to comply with the darwin core standard
    gdf = process_data.column_names_to_dwc(gdf, lookup_table)

    # Connect to the PostGIS database
    print("Creating database connection...")
    engine = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")
    with engine.connect() as connection:
        connection.execute(text('CREATE EXTENSION IF NOT EXISTS postgis;'))
        connection.commit()


    # Clear config file and database to make space for new data sets. 
    edit_config.clear_collections_from_config(pygeoapi_config)
    edit_db.drop_all_tables(engine)

    tot_rows = 0 
    no_family_name = gdf[gdf['InformalGroupName'].isnull()]

    # Iterate over unique values of "InformalGroupName" attribute
    print("Looping over all species classes...")
    for group_name in gdf['InformalGroupName'].unique():
        
        # Filter the GeoDataFrame based on species group
        sub_gdf = gdf[gdf['InformalGroupName'] == group_name]

        # Get cleaned table name
        table_name = process_data.clean_table_name(group_name)

        if table_name != 'unclassified' and table_name != 'nan':
            bbox = process_data.get_bbox(sub_gdf)
            min_date, max_date, dates = process_data.get_min_and_max_dates(sub_gdf)
            sub_gdf['eventDateTimeDisplay'] = dates.astype(str)
            sub_gdf['localID'] = sub_gdf.index
            n_rows = len(sub_gdf)
            tot_rows += n_rows

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
            edit_config.add_to_pygeoapi_config(template_resource, template_params, pygeoapi_config)

            # Add data to the database
            sub_gdf.to_postgis(table_name, engine, if_exists='replace', schema='public', index=False)

            print(f"In total {n_rows} rows of {table_name} inserted to the database and pygeoapi config file")


    print(f"\nIn total {tot_rows} rows of data inserted successfully into the PostGIS database and pygeoapi config file.")
    print(f"Warning: in total {len(no_family_name)} species without scientific family name were discarded")

    # And finally replace configmap in openshift with the local config file only when the script is running in kubernetes / openshift
    if os.getenv('RUNNING_IN_OPENSHIFT') == "True":
        edit_configmaps.update_configmap(pygeoapi_config) 
        
    print("API is ready to use. ")

if __name__ == '__main__':
    main()