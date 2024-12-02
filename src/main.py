import pandas as pd
from dotenv import load_dotenv
import os
import process_data, edit_config, load_data, edit_configmaps, compute_variables, edit_db, edit_metadata

def main():
    """
    Main function to load, process data, insert it into the database, and prepare the API configuration.
    """

    # Set options
    pd.options.mode.copy_on_write = True

    # Environmental variables
    load_dotenv()
    access_token = os.getenv('ACCESS_TOKEN')
    laji_api_url = os.getenv('LAJI_API_URL')
    pages_env = os.getenv('PAGES').lower()
    multiprocessing = os.getenv('MULTIPROCESSING')

    # URLs and file paths
    taxon_name_url = f'{laji_api_url}informal-taxon-groups?lang=fi&pageSize=1000&access_token={access_token}'
    collection_names_url = f"{laji_api_url}collections?selected=id&lang=fi&pageSize=1500&langFallback=true&access_token={access_token}"
    ranges1_url = f'{laji_api_url}/metadata/ranges?lang=fi&asLookupObject=true&access_token={access_token}'
    ranges2_url = f'{laji_api_url}/warehouse/enumeration-labels?access_token={access_token}'
    template_resource = r'template_resource.txt'
    pygeoapi_config = r'pygeoapi-config.yml'
    municipal_geojson_path = r'municipalities.geojson'
    lookup_table = r'lookup_table_columns.csv'

    # If the code is running in openshift/kubernetes, set paths differently
    if os.getenv('RUNNING_IN_OPENSHIFT') == "True":
        pygeoapi_config_out = r'pygeoapi-config_out.yml'
        metadata_db_path = 'tmp-catalogue.tinydb'
        db_path_in_config = 'metadata_db.tinydb'
        print("Starting in Openshift / Kubernetes...")
    else:
        pygeoapi_config_out = r'pygeoapi-config.yml'
        metadata_db_path = 'catalogue.tinydb'
        db_path_in_config = metadata_db_path
        print("Starting in Docker...")

    # Define counters
    failed_features_count = 0
    edited_features_count = 0
    duplicates_count_by_id = 0
    processed_occurrences = 0
    pages = None
    
    # Clear config file and metadata database to make space for new data sets. 
    last_update = edit_config.clear_collections_from_config(pygeoapi_config, pygeoapi_config_out).group()
    edit_metadata.empty_metadata_db(metadata_db_path)

    if pages_env == '0':
        edit_db.drop_all_tables()
    else:

        # Get other data sets
        print("Loading taxon data, collection data and value ranges...")
        taxon_df = load_data.get_taxon_data(taxon_name_url)
        collection_names = load_data.get_collection_names(collection_names_url)
        ranges1 = load_data.get_value_ranges(ranges1_url)
        ranges2 = load_data.get_enumerations(ranges2_url)
        all_value_ranges = ranges1 | ranges2 # Concenate dictionaries

        # Create an URL for occurrence data
        base_url = f"{laji_api_url}warehouse/query/unit/list?"
        selected_fields = "unit.facts,gathering.facts,document.facts,unit.linkings.taxon.threatenedStatus,unit.linkings.originalTaxon.administrativeStatuses,unit.linkings.taxon.taxonomicOrder,unit.linkings.originalTaxon.latestRedListStatusFinland.status,gathering.displayDateTime,gathering.interpretations.biogeographicalProvinceDisplayname,gathering.interpretations.coordinateAccuracy,unit.abundanceUnit,unit.atlasCode,unit.atlasClass,gathering.locality,unit.unitId,unit.linkings.taxon.scientificName,unit.interpretations.individualCount,unit.interpretations.recordQuality,unit.abundanceString,gathering.eventDate.begin,gathering.eventDate.end,gathering.gatheringId,document.collectionId,unit.breedingSite,unit.det,unit.lifeStage,unit.linkings.taxon.id,unit.notes,unit.recordBasis,unit.sex,unit.taxonVerbatim,document.documentId,document.notes,document.secureReasons,gathering.conversions.eurefWKT,gathering.notes,gathering.team,unit.keywords,unit.linkings.originalTaxon,unit.linkings.taxon.nameFinnish,unit.linkings.taxon.nameSwedish,unit.linkings.taxon.nameEnglish,document.linkings.collectionQuality,unit.linkings.taxon.sensitive,unit.abundanceUnit,gathering.conversions.eurefCenterPoint.lat,gathering.conversions.eurefCenterPoint.lon,document.dataSource,document.siteStatus,document.siteType,gathering.stateLand"
        administrative_status_ids = "MX.finlex160_1997_appendix4_2021,MX.finlex160_1997_appendix4_specialInterest_2021,MX.finlex160_1997_appendix2a,MX.finlex160_1997_appendix2b,MX.finlex160_1997_appendix3a,MX.finlex160_1997_appendix3b,MX.finlex160_1997_appendix3c,MX.finlex160_1997_largeBirdsOfPrey,MX.habitatsDirectiveAnnexII,MX.habitatsDirectiveAnnexIV,MX.birdsDirectiveStatusAppendix1,MX.birdsDirectiveStatusMigratoryBirds"
        red_list_status_ids = "MX.iucnCR,MX.iucnEN,MX.iucnVU,MX.iucnNT"
        country_id = "ML.206"
        time_range = "1990-01-01/" 
        coordinate_accuracy_max = "1000"
        page = "1"
        page_size = "10000"
        taxon_admin_filters_operator = "OR"
        collection_and_record_quality = "PROFESSIONAL:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL,UNCERTAIN;HOBBYIST:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL;AMATEUR:EXPERT_VERIFIED,COMMUNITY_VERIFIED;"
        geo_json = "true"
        feature_type = "ORIGINAL_FEATURE"
        biogeographical_province_ids = ["ML.251","ML.252","ML.253","ML.254","ML.255","ML.256","ML.257","ML.258","ML.259","ML.260","ML.261","ML.262","ML.263","ML.264","ML.265","ML.266","ML.267","ML.268","ML.269","ML.270","ML.271"]
        occurrence_url = f"{base_url}selected={selected_fields}&countryId={country_id}&time={time_range}&redListStatusId={red_list_status_ids}&administrativeStatusId={administrative_status_ids}&coordinateAccuracyMax={coordinate_accuracy_max}&page={page}&pageSize={page_size}&taxonAdminFiltersOperator={taxon_admin_filters_operator}&collectionAndRecordQuality={collection_and_record_quality}&geoJSON={geo_json}&featureType={feature_type}&access_token={access_token}"
    
        # Check if 'PAGES' is set to 'latest' and 'last_update' is available
        if pages_env == 'latest' and last_update:
            occurrence_url = f"{occurrence_url}&loadedSameOrAfter={last_update}" # Add the 'loadedSameOrAfter' parameter to the occurrence URL for filtering recent data
        elif pages_env != 'all': # Else if the 'PAGES' is a specific number, parse it 
            try:
                pages = int(pages_env)
            except ValueError:
                raise Exception("Invalid 'PAGES' environment variable value. Choose 'latest', 'all', '0', or specify the number of pages you want to download (e.g., 10).")

        # Check if using private queries
        if os.getenv('TARGET') == 'virva':
            occurrence_url = occurrence_url.replace('/query/', '/private-query/')
            access_email = os.getenv('ACCESS_EMAIL')
            occurrence_url = f"{occurrence_url}&personEmail={access_email}" 

        # Loop over each biogeographical province ID to load and process its occurrence data
        for idx, id in enumerate(biogeographical_province_ids):

            # Add biogeographical provices to the query
            if 'biogeographicalProvinceId' not in occurrence_url:
                occurrence_url += f"&biogeographicalProvinceId={id}"
            else:
                occurrence_url = occurrence_url.replace(f'biogeographicalProvinceId={biogeographical_province_ids[idx-1]}', f'biogeographicalProvinceId={id}')
            
            table_base_name = compute_variables.get_biogeographical_region_from_id(id)
            table_names = [
                f'{table_base_name}_points',
                f'{table_base_name}_lines',
                f'{table_base_name}_polygons',
                ]

            # If the number of pages to load is not set, calculate the last available page
            if pages_env in ['all', 'latest']:
                pages = load_data.get_last_page(occurrence_url)

            if pages_env != 'latest':
                edit_db.drop_table(table_names)

            # Process data in batches
            batch_size = 5
            for startpage in range(1, pages+1, batch_size):

                endpage = min(startpage + batch_size - 1, pages)

                print(f"Loading pages {startpage}-{endpage} ({pages} in total) of occurrences from {table_base_name}")
                gdf = load_data.get_occurrence_data(occurrence_url, startpage=startpage, endpage=endpage, multiprocessing=multiprocessing) 

                if gdf.empty:
                    print(f"No occurrences found from {table_base_name}, skipping.")
                    table_names = None
                    continue

                processed_occurrences += len(gdf)

                print("Preparing data...")
                gdf = process_data.merge_taxonomy_data(gdf, taxon_df)
                gdf = process_data.process_facts(gdf)
                gdf = process_data.combine_similar_columns(gdf)
                gdf = compute_variables.compute_all(gdf, all_value_ranges, collection_names, municipal_geojson_path)
                gdf = process_data.translate_column_names(gdf, lookup_table, style='virva')
                gdf, edited_features = process_data.validate_geometry(gdf)
                edited_features_count += edited_features

                print("Inserting data to the DB...")
                failed_features_count += edit_db.to_db(gdf, table_names)


            print(f"Processing tables in the DB...")
            duplicates_count_by_id += edit_db.remove_duplicates(table_names)
            #edit_db.update_indexes(table_names, multiprocessing)

        print("Creating metadata...")
        edit_metadata.create_metadata(template_resource, metadata_db_path, pygeoapi_config_out)
    
        # Print statistics
        number_of_occurrences_after_updating = edit_db.get_amount_of_all_occurrences()
        print(f"So, in total, {processed_occurrences} occurrences were processed:")
        print(f" -> {edited_features_count} of them had invalid geometries that were fixed")
        print(f" -> {failed_features_count} of them failed to add to the database")
        print(f" -> {duplicates_count_by_id} duplicates were removed from the database")
        print(f"The database has now {number_of_occurrences_after_updating} occurrences")

        # Add metadata info to config file
        edit_config.add_metadata_to_config(pygeoapi_config_out, db_path_in_config)

    # Replace configmap in openshift with the local config file only when the script is running in kubernetes / openshift
    if os.getenv('RUNNING_IN_OPENSHIFT') == "True":
        edit_configmaps.update_and_restart(pygeoapi_config_out, metadata_db_path) 

    print("API is ready to use. ")

if __name__ == '__main__':
    main()