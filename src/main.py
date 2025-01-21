import pandas as pd
from dotenv import load_dotenv
import os
import process_data, edit_config, load_data, edit_configmaps, compute_variables, edit_db, edit_metadata

def setup_environment():
    """
    Load environmental variables and configure paths.
    """
    load_dotenv()
    access_token = os.getenv('ACCESS_TOKEN')
    laji_api_url = os.getenv('LAJI_API_URL', 'https://api.laji.fi/v0/')
    pages_env = os.getenv('PAGES', 'all').lower()
    access_email = os.getenv('ACCESS_EMAIL')
    multiprocessing = os.getenv('MULTIPROCESSING', 'True')
    target = os.getenv('TARGET', 'default')

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
    
    return {
        "access_token": access_token,
        "access_email": access_email,
        "laji_api_url": laji_api_url,
        "target": target,
        "pages_env": pages_env,
        "multiprocessing": multiprocessing,
        "pygeoapi_config_out": pygeoapi_config_out,
        "metadata_db_path": metadata_db_path,
        "db_path_in_config": db_path_in_config
    }

def load_and_process_data(occurrence_url, table_base_name, pages, config, all_value_ranges, taxon_df, collection_names, municipal_geojson_path, lookup_table, drop_tables=False):
    """
    Load and process data in batches from the given URL.
    """
    processed_occurrences = 0
    failed_features_count = 0
    edited_features_count = 0
    duplicates_count_by_id = 0
    table_names = [f'{table_base_name}_points', f'{table_base_name}_lines', f'{table_base_name}_polygons']

    if drop_tables:
        edit_db.drop_table(table_names)

    batch_size = 5
    for startpage in range(1, pages + 1, batch_size):
        endpage = min(startpage + batch_size - 1, pages)
        print(f"Loading {table_base_name} observations. Pages {startpage}-{endpage} ({pages} in total)")
        
        gdf, failed_features = load_data.get_occurrence_data(occurrence_url, startpage=startpage, endpage=endpage, multiprocessing=config["multiprocessing"])
        failed_features_count += failed_features

        if gdf.empty:
            print(f"No occurrences found from {table_base_name}, skipping.")
            continue

        print(f"Processing {len(gdf)} observations...")
        processed_occurrences += len(gdf)
        
        gdf = process_data.merge_taxonomy_data(gdf, taxon_df)
        gdf = process_data.process_facts(gdf)
        gdf = process_data.combine_similar_columns(gdf)
        gdf = compute_variables.compute_all(gdf, all_value_ranges, collection_names, municipal_geojson_path)
        gdf = process_data.translate_column_names(gdf, lookup_table, style='virva')
        gdf, edited_features = process_data.validate_geometry(gdf)
        
        edited_features_count += edited_features
        failed_features_count += edit_db.to_db(gdf, table_names)
    
    duplicates_count_by_id += edit_db.remove_duplicates(table_names)
    
    return processed_occurrences, failed_features_count, edited_features_count, duplicates_count_by_id


def main():
    """
    Main function to load, process data, insert it into the database, and prepare the API configuration.
    """

    # Set options
    pd.options.mode.copy_on_write = True
    config = setup_environment()

    processed_occurrences = 0
    failed_features_count = 0
    edited_features_count = 0
    duplicates_count_by_id = 0
    drop_tables = False
    
    last_update = edit_db.get_and_update_last_update()

    if config['pages_env'] == '0':
        edit_db.drop_all_tables()
    else:

        # Load essential data
        taxon_df = load_data.get_taxon_data(f"{config['laji_api_url']}informal-taxon-groups?lang=fi&pageSize=1000&access_token={config['access_token']}")
        collection_names = load_data.get_collection_names(f"{config['laji_api_url']}collections?selected=id&lang=fi&pageSize=1500&langFallback=true&access_token={config['access_token']}")
        ranges1 = load_data.get_value_ranges(f"{config['laji_api_url']}/metadata/ranges?lang=fi&asLookupObject=true&access_token={config['access_token']}")
        ranges2 = load_data.get_enumerations(f"{config['laji_api_url']}/warehouse/enumeration-labels?access_token={config['access_token']}")
        all_value_ranges = ranges1 | ranges2

        # Construct API URL for api.laji.fi
        base_url = f"{config['laji_api_url']}warehouse/query/unit/list?"
        if config['pages_env'] == "latest" and last_update:
            base_url = f"{base_url}loadedSameOrAfter={last_update}&"
        elif config['pages_env'] == "all":
            drop_tables = True
        if config['target'] == 'virva':
            base_url = base_url.replace('/query/', '/private-query/')
            base_url = f"{base_url}personEmail={config['access_email']}&"

        selected_fields = "document.loadDate,unit.facts,gathering.facts,document.facts,unit.linkings.taxon.threatenedStatus,unit.linkings.originalTaxon.administrativeStatuses,unit.linkings.taxon.taxonomicOrder,unit.linkings.originalTaxon.latestRedListStatusFinland.status,gathering.displayDateTime,gathering.interpretations.biogeographicalProvinceDisplayname,gathering.interpretations.coordinateAccuracy,unit.abundanceUnit,unit.atlasCode,unit.atlasClass,gathering.locality,unit.unitId,unit.linkings.taxon.scientificName,unit.interpretations.individualCount,unit.interpretations.recordQuality,unit.abundanceString,gathering.eventDate.begin,gathering.eventDate.end,gathering.gatheringId,document.collectionId,unit.breedingSite,unit.det,unit.lifeStage,unit.linkings.taxon.id,unit.notes,unit.recordBasis,unit.sex,unit.taxonVerbatim,document.documentId,document.notes,document.secureReasons,gathering.conversions.eurefWKT,gathering.notes,gathering.team,unit.keywords,unit.linkings.originalTaxon,unit.linkings.taxon.nameFinnish,unit.linkings.taxon.nameSwedish,unit.linkings.taxon.nameEnglish,document.linkings.collectionQuality,unit.linkings.taxon.sensitive,unit.abundanceUnit,gathering.conversions.eurefCenterPoint.lat,gathering.conversions.eurefCenterPoint.lon,document.dataSource,document.siteStatus,document.siteType,gathering.stateLand"
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

        print("Processing species data from each biogeographical region...")
        biogeographical_province_ids = ["ML.251","ML.252","ML.253","ML.254","ML.255","ML.256","ML.257","ML.258","ML.259","ML.260","ML.261","ML.262","ML.263","ML.264","ML.265","ML.266","ML.267","ML.268","ML.269","ML.270","ML.271"]
        for province_id in biogeographical_province_ids:
            table_base_name = compute_variables.get_biogeographical_region_from_id(province_id)
            occurrence_url = f"{base_url}selected={selected_fields}&countryId={country_id}&time={time_range}&redListStatusId={red_list_status_ids}&administrativeStatusId={administrative_status_ids}&coordinateAccuracyMax={coordinate_accuracy_max}&page={page}&pageSize={page_size}&taxonAdminFiltersOperator={taxon_admin_filters_operator}&collectionAndRecordQuality={collection_and_record_quality}&geoJSON={geo_json}&featureType={feature_type}&biogeographicalProvinceId={province_id}&access_token={config['access_token']}"
            pages = load_data.get_last_page(occurrence_url) if config["pages_env"] in ["all", "latest"] else int(config["pages_env"])
            results = load_and_process_data(occurrence_url, table_base_name, pages, config, all_value_ranges, taxon_df, collection_names, 'municipalities.geojson', 'lookup_table_columns.csv', drop_tables)

            processed_occurrences += results[0]
            failed_features_count += results[1]
            edited_features_count += results[2]
            duplicates_count_by_id += results[3]

        if os.getenv("INVASIVE_SPECIES", "True").lower() == "true":
            print("Processing invasive species data...")
            occurrence_url = f"{base_url}selected={selected_fields}&countryId={country_id}&time={time_range}&invasive=True&page={page}&pageSize={page_size}&geoJSON={geo_json}&featureType={feature_type}&access_token={config['access_token']}"
            pages = load_data.get_last_page(occurrence_url) if config["pages_env"] in ["all", "latest"] else int(config["pages_env"])
            results = load_and_process_data(occurrence_url, 'invasive_species', pages, config, all_value_ranges, taxon_df, collection_names, 'municipalities.geojson', 'lookup_table_columns.csv', drop_tables)

            processed_occurrences += results[0]
            failed_features_count += results[1]
            edited_features_count += results[2]
            duplicates_count_by_id += results[3]

        print("Processing completed.")

    # Create metadata for the processed data
    print("Creating metadata...")
    edit_metadata.create_metadata("template_resource.txt", config["metadata_db_path"], config["pygeoapi_config_out"])

    # Generate statistics for reporting
    total_occurrences = edit_db.get_amount_of_all_occurrences()

    # Update the PyGeoAPI configuration with metadata info
    print("Updating PyGeoAPI configuration with metadata...")
    edit_config.add_metadata_to_config(config["pygeoapi_config_out"], config["db_path_in_config"])

    # If running in Openshift/Kubernetes, replace the config map and restart
    if os.getenv('RUNNING_IN_OPENSHIFT') == "True":
        print("Updating configmap and restarting the service...")
        edit_configmaps.update_and_restart(config["pygeoapi_config_out"], config["metadata_db_path"])
    
    print("\n--- Summary Report ---")
    print(f" -> Total processed occurrences: {processed_occurrences}")
    print(f" -> Fixed geometries: {edited_features_count}")
    print(f" -> Failed insertions: {failed_features_count} (estimated)")
    print(f" -> Duplicates removed: {duplicates_count_by_id}")
    print(f" -> Final occurrences in database after processing: {total_occurrences}")

    print("\nAPI is ready to use. All tasks completed successfully.")

if __name__ == '__main__':
    main()