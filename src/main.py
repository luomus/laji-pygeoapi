import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
import os
import process_data, edit_config, load_data, edit_configmaps, edit_db, compute_variables
#import numpy as np

# Set options for pandas and geopandas
pd.options.mode.copy_on_write = True
gpd.options.io_engine = "pyogrio" # Faster way to read data

# URLs and file paths
load_dotenv()
access_token = os.getenv('ACCESS_TOKEN')
taxon_name_url = f'https://api.laji.fi/v0/informal-taxon-groups?pageSize=1000&access_token={access_token}'
template_resource = r'template_resource.txt'
pygeoapi_config = r'pygeoapi-config.yml'
lookup_table = r'lookup_table_columns.csv'

# Create an URL for Virva filtered occurrence data
base_url = "https://api.laji.fi/v0/warehouse/query/unit/list?"
selected_fields = "unit.linkings.taxon.threatenedStatus,unit.linkings.originalTaxon.administrativeStatuses,unit.linkings.taxon.taxonomicOrder,unit.linkings.originalTaxon.latestRedListStatusFinland.status,gathering.municipality,gathering.displayDateTime,gathering.interpretations.biogeographicalProvinceDisplayname,gathering.interpretations.coordinateAccuracy,unit.abundanceUnit,unit.atlasCode,unit.atlasClass,gathering.locality,unit.unitId,unit.linkings.taxon.scientificName,unit.interpretations.individualCount,unit.interpretations.recordQuality,unit.abundanceString,gathering.eventDate.begin,gathering.eventDate.end,gathering.gatheringId,document.collectionId,unit.breedingSite,unit.det,unit.lifeStage,unit.linkings.taxon.id,unit.notes,unit.recordBasis,unit.sex,unit.taxonVerbatim,document.documentId,document.notes,document.secureReasons,gathering.conversions.eurefWKT,gathering.notes,gathering.team,unit.keywords,unit.linkings.originalTaxon,unit.linkings.taxon.nameSwedish,unit.linkings.taxon.nameEnglish,document.linkings.collectionQuality,unit.linkings.taxon.sensitive,unit.abundanceUnit,gathering.conversions.eurefCenterPoint.lat,gathering.conversions.eurefCenterPoint.lon,document.dataSource,document.siteStatus,document.siteType,gathering.stateLand"
administrative_status_ids = "MX.finlex160_1997_appendix4_2021,MX.finlex160_1997_appendix4_specialInterest_2021,MX.finlex160_1997_appendix2a,MX.finlex160_1997_appendix2b,MX.finlex160_1997_appendix3a,MX.finlex160_1997_appendix3b,MX.finlex160_1997_appendix3c,MX.finlex160_1997_largeBirdsOfPrey,MX.habitatsDirectiveAnnexII,MX.habitatsDirectiveAnnexIV,MX.birdsDirectiveStatusAppendix1,MX.birdsDirectiveStatusMigratoryBirds"
red_list_status_ids = "MX.iucnCR,MX.iucnEN,MX.iucnVU,MX.iucnNT"
country_id = "ML.206"
time_range = "1990-01-01/"
aggregate_by = "gathering.conversions.wgs84Grid05.lat,gathering.conversions.wgs84Grid1.lon"
only_count = "false"
individual_count_min = "0"
coordinate_accuracy_max = "1000"
page = "1"
page_size = "1000"
taxon_admin_filters_operator = "OR"
collection_and_record_quality = "PROFESSIONAL:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL,UNCERTAIN;HOBBYIST:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL;AMATEUR:EXPERT_VERIFIED,COMMUNITY_VERIFIED;"
geo_json = "true"
feature_type = "ORIGINAL_FEATURE"
occurrence_url = f"{base_url}selected={selected_fields}&administrativeStatusId={administrative_status_ids}&redListStatusId={red_list_status_ids}&countryId={country_id}&time={time_range}&aggregateBy={aggregate_by}&onlyCount={only_count}&individualCountMin={individual_count_min}&coordinateAccuracyMax={coordinate_accuracy_max}&page={page}&pageSize={page_size}&taxonAdminFiltersOperator={taxon_admin_filters_operator}&collectionAndRecordQuality={collection_and_record_quality}&geoJSON={geo_json}&featureType={feature_type}&access_token={access_token}"

# Pygeoapi output file
if os.getenv('RUNNING_IN_OPENSHIFT') == True or os.getenv('RUNNING_IN_OPENSHIFT') == "True":
    pygeoapi_config_out = r'pygeoapi-config_out.yml'
    print("Starting in Openshift / Kubernetes...")
else:
    pygeoapi_config_out = r'pygeoapi-config.yml'
    print("Starting in Docker...")

def main():
    """
    Main function to load, process data, insert it into the database, and prepare the API configuration.
    """
    #print(f"URL: n\ {occurrence_url}")

    tot_rows = 0
    multiprocessing = os.getenv('MULTIPROCESSING')
    pages = os.getenv('PAGES')
    engine = edit_db.connect_to_db()
    table_names = []
    occurrences_without_group_count = 0
    merged_occurrences_count = 0
    edited_features_count = 0
    failed_features_count = 0

    # Clear config file and database to make space for new data sets. 
    edit_config.clear_collections_from_config(pygeoapi_config, pygeoapi_config_out)
    edit_db.drop_all_tables(engine)

    # Get taxon data
    taxon_df = load_data.get_taxon_data(taxon_name_url, pages='all')

    # Determine the number of pages to process 
    if pages.lower() == "all":
        pages = load_data.get_last_page(occurrence_url)
    pages = int(pages)
    
    print(f"Retrieving {pages} pages of occurrence data from the API...")

    # Load and process data in batches. Store to the database
    batch_size=5
    for startpage in range(1, pages+1, batch_size):
        if startpage < pages-batch_size-1:
            endpage = startpage + batch_size-1
        else:
            endpage = pages
        
        # Get 10 pages of occurrence data
        gdf = load_data.get_occurrence_data(occurrence_url, multiprocessing=multiprocessing, startpage=startpage, pages=endpage) 

        print("Prosessing data...")

        # Merge taxonomy information with the occurrence data
        gdf = process_data.merge_taxonomy_data(gdf, taxon_df)

        # Combine similar columns (e.g. 'column[0]' and 'column[1]' to 'column')
        gdf = process_data.combine_similar_columns(gdf)

        # Compute variables that can not be directly accessed from the source API
        gdf = compute_variables.compute_variables(gdf)

        # Remove some columns
        gdf = process_data.translate_column_names(gdf, lookup_table, style='virva')

        # Fix invalid geometries
        gdf['geometry'], edited_features = process_data.validate_geometry(gdf['geometry'])
        edited_features_count += edited_features

        # Convert GeometryCollections to MultiPolygons if they exist
        gdf['geometry'] = gdf['geometry'].apply(process_data.convert_geometry_collection_to_multipolygon)

        # Change column types
        gdf['Keruu_aloitus_pvm'] = gdf['Keruu_aloitus_pvm'].astype('str')
        gdf['Keruu_lopetus_pvm'] = gdf['Keruu_lopetus_pvm'].astype('str')
        gdf['Sensitiivinen_laji'] = gdf['Sensitiivinen_laji'].astype('bool')

        if 'Aineistolahde' not in gdf.columns:
            gdf['Aineistolahde'] = None

        # Extract entries without family names and drop them
        occurrences_without_group_count += len(gdf[gdf['elioryhma'].isnull()])
        gdf = gdf[~gdf['elioryhma'].isnull()]

        # Merge duplicates
        gdf, amount_of_merged_occurrences = process_data.merge_duplicates(gdf)
        merged_occurrences_count += amount_of_merged_occurrences

        # Process each unique species group (e.g. Birds, Mammals etc.)
        for group_name in gdf['elioryhma'].unique():
            sub_gdf = gdf[gdf['elioryhma'] == group_name]

            # Get cleaned table name
            table_name = process_data.clean_table_name(group_name)

            # Skip nans and unclassified
            if isinstance(table_name, str) and table_name != 'unclassified' and table_name != 'nan':
                
                # Create local ID
                sub_gdf['Paikallinen_tunniste'] = sub_gdf.index

                # Add to PostGIS database
                try:
                    sub_gdf.to_postgis(table_name, engine, if_exists='append', schema='public', index=False)
                    if table_name not in table_names:
                         table_names.append(table_name)
                except Exception as e:
                    print(f"Error occurred: {e}")
                    failed_features_count += len(sub_gdf) 
            del sub_gdf
        del gdf
    del taxon_df


    # Update pygeoapi configuration
    for table_name in table_names:
        bbox = edit_db.get_table_bbox(engine, table_name)
        min_date, max_date = edit_db.get_table_dates(engine, table_name)
        amount_of_occurrences = edit_db.get_amount_of_occurrences(engine, table_name)
        tot_rows += amount_of_occurrences

        # Create parameters dictionary to fill the template for pygeoapi config file
        template_params = {
            "<placeholder_table_name>": table_name,
            "<placeholder_amount_of_occurrences>": str(amount_of_occurrences),
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
        print(f"In total {amount_of_occurrences} occurrences of {table_name} inserted to the database")

    print(f"In total {tot_rows} rows of data inserted successfully into the PostGIS database and pygeoapi config file.")
    print(f"In total {merged_occurrences_count} duplicate occurrences were merged")
    print(f"In total {edited_features_count} invalid geometries were fixed")
    print(f"Warning: in total {occurrences_without_group_count} species without scientific family name were discarded")
    print(f"Warning: in total {failed_features_count} features failed to add to the database")

    # And finally replace configmap in openshift with the local config file only when the script is running in kubernetes / openshift
    if os.getenv('RUNNING_IN_OPENSHIFT') == True or os.getenv('RUNNING_IN_OPENSHIFT') == "True":
        edit_configmaps.update_configmap(pygeoapi_config_out) 
        
    print("API is ready to use. ")

if __name__ == '__main__':
    main()