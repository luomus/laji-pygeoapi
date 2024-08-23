import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
import os
import process_data, edit_config, load_data, edit_configmaps, compute_variables, edit_db, edit_metadata
#import numpy as np

def main():
    """
    Main function to load, process data, insert it into the database, and prepare the API configuration.
    """

    # Set options
    pd.options.mode.copy_on_write = True

    # URLs and file paths
    load_dotenv()
    access_token = os.getenv('ACCESS_TOKEN')
    taxon_name_url = f'https://api.laji.fi/v0/informal-taxon-groups?lang=fi&pageSize=1000&access_token={access_token}'
    template_resource = r'template_resource.txt'
    pygeoapi_config = r'pygeoapi-config.yml'
    municipal_geojson_path = r'municipalities_and_elys.geojson'
    lookup_table = r'lookup_table_columns.csv'

    # Create an URL for Virva filtered occurrence data
    base_url = "https://api.laji.fi/v0/warehouse/query/unit/list?"
    selected_fields = "unit.facts,gathering.facts,document.facts,unit.linkings.taxon.threatenedStatus,unit.linkings.originalTaxon.administrativeStatuses,unit.linkings.taxon.taxonomicOrder,unit.linkings.originalTaxon.latestRedListStatusFinland.status,gathering.displayDateTime,gathering.interpretations.biogeographicalProvinceDisplayname,gathering.interpretations.coordinateAccuracy,unit.abundanceUnit,unit.atlasCode,unit.atlasClass,gathering.locality,unit.unitId,unit.linkings.taxon.scientificName,unit.interpretations.individualCount,unit.interpretations.recordQuality,unit.abundanceString,gathering.eventDate.begin,gathering.eventDate.end,gathering.gatheringId,document.collectionId,unit.breedingSite,unit.det,unit.lifeStage,unit.linkings.taxon.id,unit.notes,unit.recordBasis,unit.sex,unit.taxonVerbatim,document.documentId,document.notes,document.secureReasons,gathering.conversions.eurefWKT,gathering.notes,gathering.team,unit.keywords,unit.linkings.originalTaxon,unit.linkings.taxon.nameFinnish,unit.linkings.taxon.nameSwedish,unit.linkings.taxon.nameEnglish,document.linkings.collectionQuality,unit.linkings.taxon.sensitive,unit.abundanceUnit,gathering.conversions.eurefCenterPoint.lat,gathering.conversions.eurefCenterPoint.lon,document.dataSource,document.siteStatus,document.siteType,gathering.stateLand"
    #administrative_status_ids = "MX.finlex160_1997_appendix4_2021,MX.finlex160_1997_appendix4_specialInterest_2021,MX.finlex160_1997_appendix2a,MX.finlex160_1997_appendix2b,MX.finlex160_1997_appendix3a,MX.finlex160_1997_appendix3b,MX.finlex160_1997_appendix3c,MX.finlex160_1997_largeBirdsOfPrey,MX.habitatsDirectiveAnnexII,MX.habitatsDirectiveAnnexIV,MX.birdsDirectiveStatusAppendix1,MX.birdsDirectiveStatusMigratoryBirds"
    #red_list_status_ids = "MX.iucnCR,MX.iucnEN,MX.iucnVU,MX.iucnNT"
    country_id = "ML.206"
    time_range = "1990-01-01/" 
    only_count = "false"
    individual_count_min = "0"
    coordinate_accuracy_max = "1000"
    page = "1"
    page_size = "10000"
    taxon_admin_filters_operator = "OR"
    collection_and_record_quality = "PROFESSIONAL:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL,UNCERTAIN;HOBBYIST:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL;AMATEUR:EXPERT_VERIFIED,COMMUNITY_VERIFIED;"
    geo_json = "true"
    feature_type = "ORIGINAL_FEATURE"
    occurrence_url = f"{base_url}selected={selected_fields}&countryId={country_id}&time={time_range}&onlyCount={only_count}&individualCountMin={individual_count_min}&coordinateAccuracyMax={coordinate_accuracy_max}&page={page}&pageSize={page_size}&taxonAdminFiltersOperator={taxon_admin_filters_operator}&collectionAndRecordQuality={collection_and_record_quality}&geoJSON={geo_json}&featureType={feature_type}&access_token={access_token}"
    # redListStatusId={red_list_status_ids}&administrativeStatusId={administrative_status_ids}&
    
    # Pygeoapi output file
    if os.getenv('RUNNING_IN_OPENSHIFT') == True or os.getenv('RUNNING_IN_OPENSHIFT') == "True":
        pygeoapi_config_out = r'pygeoapi-config_out.yml'
        metadata_db_path = 'tmp-catalogue.tinydb'
        db_path_in_config = 'metadata_db.tinydb'
        print("Starting in Openshift / Kubernetes...")
    else:
        pygeoapi_config_out = r'pygeoapi-config.yml'
        metadata_db_path = 'catalogue.tinydb'
        db_path_in_config = metadata_db_path
        print("Starting in Docker...")
        #print(f"URL: n\ {occurrence_url}")

    tot_rows = 0
    table_no = 0
    multiprocessing = os.getenv('MULTIPROCESSING')
    pages = os.getenv('PAGES')
    table_names = []
    occurrences_without_group_count = 0
    merged_occurrences_count = 0
    failed_features_count = 0
    edited_features_count = 0
    duplicates_count_by_id = 0
    processed_occurrences = 0
    last_iteration = False

    # Clear config file and metadata database to make space for new data sets. 
    last_update = edit_config.clear_collections_from_config(pygeoapi_config, pygeoapi_config_out).group()
    edit_metadata.empty_metadata_db(metadata_db_path)

    # Get other data sets
    print("Loading taxon and collection data...")
    taxon_df = load_data.get_taxon_data(taxon_name_url)
    collection_names = load_data.get_collection_names(f"https://api.laji.fi/v0/collections?selected=id&lang=fi&pageSize=1500&langFallback=true&access_token={access_token}")

    
    # Determine the number of pages to retrieve based on the 'PAGES' environment variable
    pages_env = os.getenv('PAGES').lower()

    if pages_env == 'latest' and last_update:
        # If 'latest' is selected and there is a last update, retrieve only the latest occurrences
        occurrence_url = f"{occurrence_url}&loadedSameOrAfter={last_update}"
        pages = load_data.get_last_page(occurrence_url)
        print(f"Starting to retrieve {pages} pages of occurrence data loaded after the last update: {last_update}...")
        
    elif pages_env == 'all':
        # If 'all' is selected, drop all db tables and retrieve all pages of occurrence data
        pages = load_data.get_last_page(occurrence_url)
        edit_db.drop_all_tables()
        print(f"Starting to retrieve {pages} pages of occurrence data for an empty database...")
        
    elif pages_env == '0':
        # If '0' is selected, drop all tables without downloading any new data
        edit_db.drop_all_tables()
        print(f"Emptying the database without downloading data...")
        pages = 0
        
    else:
        # If a specific number of pages is provided, attempt to parse it and download that many pages
        try:
            pages = int(os.getenv('PAGES'))
            edit_db.drop_all_tables()
            print(f"Starting to retrieve {pages} pages of occurrence data for an empty database...")
        except ValueError:
            # Handle invalid input for the 'PAGES' environment variable
            print("ERROR: The environment variable 'PAGES' is not valid. Choose 'latest', 'all', '0', or specify the number of pages you want to download (e.g., 10).")
            raise Exception("Invalid 'PAGES' environment variable value.")


    number_of_occurrences_before_updating = edit_db.get_amount_of_all_occurrences()

    # Load and process data in batches. Store to the database
    batch_size = 5
    for startpage in range(1, pages+1, batch_size):
        if startpage <= pages-batch_size-1:
            endpage = startpage + batch_size-1
        else:
            endpage = pages
            last_iteration = True
        
        # Get occurrence data
        print(f"Retrieving pages {startpage} to {endpage}...")
        gdf = load_data.get_occurrence_data(occurrence_url, startpage=startpage, endpage=endpage, multiprocessing=multiprocessing) 
        processed_occurrences += len(gdf)

        print("Prosessing data...")
        gdf = process_data.merge_taxonomy_data(gdf, taxon_df)
        gdf = process_data.get_facts(gdf)
        gdf = process_data.combine_similar_columns(gdf)
        gdf = compute_variables.compute_all(gdf, collection_names, municipal_geojson_path)
        gdf = process_data.translate_column_names(gdf, lookup_table, style='virva')
        gdf = process_data.convert_geometry_collection_to_multipolygon(gdf)
        gdf, amount_of_merged_occurrences = process_data.merge_duplicates(gdf, lookup_table)
        merged_occurrences_count += amount_of_merged_occurrences

        print("Inserting data to the database...")
        table_names, failed_features_count, occurrences_without_group_count = edit_db.to_db(gdf, table_names, failed_features_count, occurrences_without_group_count, last_iteration)

        del gdf
    del taxon_df, collection_names

    print(f"Number of occurrences before updating: {number_of_occurrences_before_updating}")
    print("Updating PostGIS indexes, geometries and pygeoapi configuration...")
    for table_name in table_names:
        edited_features_count += edit_db.validate_geometries_postgis(table_name)
        duplicates_count_by_id += edit_db.remove_duplicates_by_id(table_name)
        bbox = edit_db.get_table_bbox(table_name)
        min_date, max_date = edit_db.get_table_dates(table_name)
        no_of_occurrences = edit_db.get_amount_of_occurrences(table_name)
        quality_dict = edit_db.get_quality_frequency(table_name)
        tot_rows += no_of_occurrences
        table_no += 1

        # Create parameters dictionary to fill the template for pygeoapi config file
        template_params = {
            "<placeholder_table_name>": table_name,
            "<placeholder_amount_of_occurrences>": str(no_of_occurrences),
            "<placeholder_bbox>": str(bbox),
            "<placeholder_min_date>": min_date,
            "<placeholder_max_date>": max_date,
            "<placeholder_postgres_host>": os.getenv('POSTGRES_HOST'),
            "<placeholder_postgres_password>": os.getenv('POSTGRES_PASSWORD'),
            "<placeholder_postgres_user>": os.getenv('POSTGRES_USER'),
            "<placeholder_db_name>": os.getenv('POSTGRES_DB')
        }

        metadata_dict = {
            "bbox": bbox,
            "dataset_name": table_name,
            "no_of_occurrences": no_of_occurrences,
            "min_date": min_date,
            "max_date": max_date,
            "table_no": table_no,
            "quality_dict": quality_dict
        }

        edit_config.add_to_pygeoapi_config(template_resource, template_params, pygeoapi_config_out)
        edit_metadata.create_metadata(metadata_dict, metadata_db_path)
        edit_db.update_indexes(table_name)

    number_of_occurrences_after_updating = edit_db.get_amount_of_all_occurrences()
    print(f"Number of occurrences after updating: {number_of_occurrences_after_updating}")
    print(f"So, in total, {processed_occurrences} occurrences were processed:")
    print(f" -> {number_of_occurrences_after_updating - number_of_occurrences_before_updating} of them were added to the database:")
    print(f" -> {edited_features_count} of them had invalid geometries that were fixed")
    print(f" -> {occurrences_without_group_count} of them were discarced because they were not part of any ELY center area")
    print(f" -> {merged_occurrences_count} of them were merged as duplicates")
    print(f" -> {duplicates_count_by_id} of them were not inserted as they were already in the database")
    print(f" -> {failed_features_count} of them failed to add to the database")

    # Add metadata info to config file
    edit_config.add_metadata_to_config(pygeoapi_config_out, db_path_in_config)

    # And finally replace configmap in openshift with the local config file only when the script is running in kubernetes / openshift
    if os.getenv('RUNNING_IN_OPENSHIFT') == True or os.getenv('RUNNING_IN_OPENSHIFT') == "True":
        edit_configmaps.update_and_restart(pygeoapi_config_out, metadata_db_path) 

    print("API is ready to use. ")

if __name__ == '__main__':
    main()