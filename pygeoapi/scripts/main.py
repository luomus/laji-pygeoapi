import pandas as pd
import geopandas as gpd
from dotenv import load_dotenv
import os
import logging
from scripts import load_data, process_data, edit_config, edit_configmaps, compute_variables, edit_db, edit_metadata, send_error_emails
import sys
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

load_dotenv()

logging_level = os.getenv('LOGGING_LEVEL', 'INFO')

logging.basicConfig(
    level=logging_level,
    stream=sys.stdout,
    format='%(asctime)s %(levelname)s %(message)s'
)

maintenance_executor = ThreadPoolExecutor(max_workers=1)
maintenance_futures = []  # (future -> returns (duplicates_removed, merged_features))

def _parse_bool(val, default=False):
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() == "true"

def setup_environment():
    """Read environment variables once and normalize types (especially booleans)."""
    access_token = os.getenv('ACCESS_TOKEN')
    laji_api_url = os.getenv('LAJI_API_URL')
    pages_env = os.getenv('PAGES', 'all').lower()
    access_email = os.getenv('ACCESS_EMAIL')
    multiprocessing = _parse_bool(os.getenv('MULTIPROCESSING'), True)
    target = os.getenv('TARGET')
    batch_size = int(os.getenv('BATCH_SIZE', 5))
    run_in_openshift = _parse_bool(os.getenv('RUNNING_IN_OPENSHIFT'), False)
    invasive_species = _parse_bool(os.getenv('INVASIVE_SPECIES'), True)
    biogeographical_province_ids = os.getenv('BIOGEOGRAPHICAL_PROVINCES')
    if biogeographical_province_ids:
        biogeographical_province_ids = biogeographical_province_ids.split(',')

    # Paths depend on platform
    if run_in_openshift:
        pygeoapi_config_out = r'pygeoapi-config_out.yml'
        metadata_db_path = 'tmp-catalogue.tinydb'
        db_path_in_config = 'metadata_db.tinydb'
        logger.info("Starting in Openshift / Kubernetes...")
    else:
        pygeoapi_config_out = r'pygeoapi-config.yml'
        metadata_db_path = 'catalogue.tinydb'
        db_path_in_config = metadata_db_path
        logger.info("Starting in Docker...")
    
    return {
        "access_token": access_token,
        "access_email": access_email,
        "laji_api_url": laji_api_url,
        "target": target,
        "pages_env": pages_env,
        "multiprocessing": multiprocessing,
        "pygeoapi_config_out": pygeoapi_config_out,
        "metadata_db_path": metadata_db_path,
        "db_path_in_config": db_path_in_config,
        "batch_size": batch_size,
        "run_in_openshift": run_in_openshift,
        "invasive_species": invasive_species,
        "biogeographical_province_ids": biogeographical_province_ids
    }

def load_and_process_data(occurrence_url, params, headers, table_base_name, pages, config, all_value_ranges, taxon_df, collection_names, municipality_ely_mappings, municipality_elinvoima_mappings, lookup_df, drop_tables=False):
    """
    Load and process data in batches from the given URL.
    """
    processed_occurrences = 0
    failed_features_count = 0
    edited_features_count = 0
    duplicates_count_by_id = 0
    converted_collections = 0
    merged_features_count = 0
    table_names = [f'{table_base_name}_points', f'{table_base_name}_lines', f'{table_base_name}_polygons']

    if drop_tables:
        edit_db.drop_table(table_names)

    gdf = None
    batch_size = config["batch_size"]

    for startpage in range(1, pages + 1, batch_size):
        endpage = min(startpage + batch_size - 1, pages)
        logger.info(f"Loading {table_base_name} observations. Pages {startpage}-{endpage} ({pages} in total)")
        
        gdf, failed_features = load_data.get_occurrence_data(occurrence_url, params, headers, startpage=startpage, endpage=endpage, multiprocessing=config["multiprocessing"])
        failed_features_count += failed_features

        if gdf.empty:
            logger.warning(f"No occurrences found from {table_base_name}, skipping.")
            continue

        logger.info(f"Processing {len(gdf)} observations...")
        processed_occurrences += len(gdf)
        
        gdf = process_data.merge_taxonomy_data(gdf, taxon_df)
        gdf = process_data.combine_similar_columns(gdf)
        gdf = compute_variables.compute_all(gdf, all_value_ranges, collection_names, municipality_ely_mappings, municipality_elinvoima_mappings)
        gdf = process_data.translate_column_names(gdf, lookup_df, style='virva')
        gdf, converted = process_data.convert_geometry_collection_to_multipolygon(gdf)
        gdf, edited = process_data.validate_geometry(gdf)
        failed_features_count += edit_db.to_db(gdf, table_names)
        edited_features_count += edited
        converted_collections += converted

    if gdf is not None and not gdf.empty:
        # Schedule maintenance work in background so next dataset can start downloading.
        def maintenance_job(tnames, lookup):
            try:
                d = edit_db.remove_duplicates(tnames)
                m = edit_db.merge_similar_observations(tnames, lookup)
                edit_db.update_indexes(tnames, use_multiprocessing=True)
                return d, m
            except Exception as e:
                logger.error(f"Maintenance job failed for {tnames}: {e}")
                return 0, 0
        maintenance_futures.append(maintenance_executor.submit(maintenance_job, table_names, lookup_df))
        logger.debug(f"Scheduled async maintenance for tables {table_names}")

    return processed_occurrences, failed_features_count, edited_features_count, duplicates_count_by_id, converted_collections, merged_features_count

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
    converted_collections = 0
    merged_features_count = 0
    drop_tables = False
    
    last_update = edit_db.get_and_update_last_update()
    edit_config.clear_collections_from_config('pygeoapi-config.yml', config["pygeoapi_config_out"])

    if config['pages_env'] == '0':
        edit_db.drop_all_tables()
    else:

        municipality_ely_mappings, _, lookup_df, taxon_df, collection_names, all_value_ranges, municipality_elinvoima_mappings = load_data.load_or_update_cache(config)

        # Construct API URL for api.laji.fi
        base_url = f"{config['laji_api_url']}warehouse/query/unit/list"
        if config['target'] == 'virva':
            base_url = base_url.replace('/query/', '/private-query/')

        logger.info("Processing species data from each biogeographical region...")
        headers = load_data._get_api_headers(config['access_token'])
        
        # Build common parameters
        common_params = {
            'selected': ",".join([field for field in lookup_df['selected'].dropna().to_list() if field]),
            'countryId': "ML.206",
            'time': "1990-01-01/",
            'redListStatusId': "MX.iucnCR,MX.iucnEN,MX.iucnVU,MX.iucnNT",
            'administrativeStatusId': "MX.finlex160_1997_appendix4_2021,MX.finlex160_1997_appendix4_specialInterest_2021,MX.finlex160_1997_appendix2a,MX.finlex160_1997_appendix2b,MX.finlex160_1997_appendix3a,MX.finlex160_1997_appendix3b,MX.finlex160_1997_appendix3c,MX.finlex160_1997_largeBirdsOfPrey,MX.habitatsDirectiveAnnexII,MX.habitatsDirectiveAnnexIV,MX.birdsDirectiveStatusAppendix1,MX.birdsDirectiveStatusMigratoryBirds",
            'coordinateAccuracyMax': "1000",
            'page': "1",
            'pageSize': "10000",
            'taxonAdminFiltersOperator': "OR",
            'collectionAndRecordQuality': "PROFESSIONAL:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL,UNCERTAIN;HOBBYIST:EXPERT_VERIFIED,COMMUNITY_VERIFIED,NEUTRAL;AMATEUR:EXPERT_VERIFIED,COMMUNITY_VERIFIED;",
            'geoJSON': "true",
            'featureType': "ORIGINAL_FEATURE",
            'individualCountMin': "0"
        }
        
        # Add conditional parameters
        if config['pages_env'] == "latest" and last_update:
            common_params['loadedSameOrAfter'] = last_update
        elif config['pages_env'] == "all":
            drop_tables = True
            
        if config['target'] == 'virva':
            common_params['personEmail'] = config['access_email']
        
        for province_id in config["biogeographical_province_ids"]:
            table_base_name = compute_variables.get_biogeographical_region_from_id(province_id)
            
            params = common_params.copy()
            params['biogeographicalProvinceId'] = province_id
            
            pages = load_data.get_pages(config["pages_env"], base_url, params, headers, int(params['pageSize']))
            results = load_and_process_data(base_url, params, headers, table_base_name, pages, config, all_value_ranges, taxon_df, collection_names, municipality_ely_mappings, municipality_elinvoima_mappings, lookup_df, drop_tables)

            processed_occurrences += results[0]
            failed_features_count += results[1]
            edited_features_count += results[2]
            duplicates_count_by_id += results[3]
            converted_collections += results[4]
            merged_features_count += results[5]

        if config["invasive_species"]:
            logger.info("Processing invasive species data...")
            
            params = common_params.copy()
            params['invasive'] = 'true'
            # Remove parameters not needed for invasive species
            params.pop('redListStatusId', None)
            params.pop('administrativeStatusId', None)
            params.pop('coordinateAccuracyMax', None)
            params.pop('taxonAdminFiltersOperator', None)
            params.pop('collectionAndRecordQuality', None)
            
            pages = load_data.get_pages(config["pages_env"], base_url, params, headers, int(params['pageSize']))
            results = load_and_process_data(base_url, params, headers, 'invasive_species', pages, config, all_value_ranges, taxon_df, collection_names, municipality_ely_mappings, municipality_elinvoima_mappings, lookup_df, drop_tables)

            processed_occurrences += results[0]
            failed_features_count += results[1]
            edited_features_count += results[2]
            duplicates_count_by_id += results[3]
            converted_collections += results[4]
            merged_features_count += results[5]

        logger.info("Processing completed.")

    # Wait for any async maintenance still running and aggregate their results
    if maintenance_futures:
        logger.info("Waiting for background maintenance tasks to finish...")
        for fut in maintenance_futures:
            try:
                d, m = fut.result()
                duplicates_count_by_id += d
                merged_features_count += m
            except Exception as e:
                logger.error(f"A maintenance task failed during final aggregation: {e}")
        maintenance_executor.shutdown(wait=True)

    # Create metadata for the processed data
    logger.info("Creating metadata...")
    edit_metadata.create_metadata("scripts/resources/template_resource.txt", config["metadata_db_path"], config["pygeoapi_config_out"])

    # Generate statistics for reporting
    total_occurrences = edit_db.get_amount_of_all_occurrences()

    # Update the PyGeoAPI configuration with metadata info
    logger.info("Updating PyGeoAPI configuration with metadata...")
    edit_config.add_resources_to_config(config["pygeoapi_config_out"], config["db_path_in_config"])

    # If running in Openshift/Kubernetes, replace the config map and restart
    if config['run_in_openshift']:
        logger.info("Updating configmap and restarting the service...")
        edit_configmaps.update_and_restart(config["pygeoapi_config_out"], config["metadata_db_path"])
    
    logger.info("\n--- Summary Report ---")
    logger.info(f" -> Total processed occurrences: {processed_occurrences}")
    logger.info(f" -> Fixed geometries: {edited_features_count}")
    logger.info(f" -> Failed insertions: {failed_features_count} (estimated)")
    logger.info(f" -> Duplicates removed: {duplicates_count_by_id}")
    logger.info(f" -> Converted geometry collections: {converted_collections}")
    logger.info(f" -> Merged features in PostGIS: {merged_features_count}")
    logger.info(f" -> Final occurrences in database after processing: {total_occurrences}")

    logger.info("\nAPI is ready to use. All tasks completed successfully.")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        if _parse_bool(os.getenv('SEND_ERROR_EMAILS', 'true'), True):
            send_error_emails.send_error_email(e, "data download script crashed")
        raise
