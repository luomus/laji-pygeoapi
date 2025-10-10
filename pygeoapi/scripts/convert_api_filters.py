import logging
import requests
from scripts.compute_variables import id_mapping
import re
from pyproj import Transformer
from difflib import get_close_matches
from scripts.load_data import get_filter_values

logger = logging.getLogger(__name__)

def convert_filters(lookup_df, all_value_ranges, municipals_ids, params, properties, config):
    """
    Converts filters from virva Finnish language scheme to be suitable for querying api.laji.fi data warehouse endpoint.
    For example, "Aineiston_tunniste=http://tun.fi/HR.95" is converted to "collectionId=HR.95" that can be used to query warehouse endpoint. 
    """
    access_token = config.get('access_token')
    base_url = config.get('laji_api_url')
    for name, value in properties:
        logger.info(f"Name: {name}, value: {value}")
        name = translate_filter_names(lookup_df, name)
        value = remove_id_prefix(value)
        if name in ['lifeStage', 'sex', 'recordQuality', 'collectionQuality', 'secureReason', 'recordBasis']:
            value = map_value(value, name, access_token, base_url)
        elif name in ['redListStatusId', 'administrativeStatusId', 'atlasClass', 'atlasCode', 'primaryHabitat']:
            value = map_value_ranges(all_value_ranges, value)
        elif name == 'biogeographicalProvinceId':
            value = map_biogeographical_provinces(value)
        elif name == 'finnishMunicipalityId':
            value = map_municipality(municipals_ids, value)
        elif name == 'time':
            value = convert_time(value)
        elif name == 'onlyNonStateLands':
            if value.lower() == 'true': # Swap because filter is negative
                value = 'False'
            else:
                value = 'True'
        logger.info(f"Converter name: {name}, value: {value}")
        params[name] = value
    return params

def translate_filter_names(lookup_df, name):
    """
    Map filter names from virva to api.laji.fi warehouse filters
    """
    if name in lookup_df['virva'].values:
        logger.info("Found exact match")
        return lookup_df.loc[lookup_df['virva'] == name, 'finbif_api_query'].values[0]
    
    # Check for similar names and log a hint
    close_matches = get_close_matches(name, lookup_df['virva'].values, n=1, cutoff=0.8)
    if close_matches:
        raise ValueError(f"Unknown filter '{name}'. Did you mean '{close_matches[0]}'?")
    else:
        logger.warning(f"Unknown filter '{name}'. Assuming it works in api.laji.fi..")

    return name


def remove_id_prefix(value):
    """
    Remove 'http://xyz.fi/' patterns from the filter values
    """
    if isinstance(value, str):
        value = re.sub(r'http://[^/]+\.fi/', '', value)
    return value


def map_value_ranges(all_value_ranges, value): 
    """
    Map filter values to api.laji.fi query parameters 
    """
    logger.debug(f"Mapping value ranges for value: {value}")
    values = [v.strip() for v in value.split(',')]
    mapped_values = []
    for val in values:
        val_cleaned = val.replace(' ', '')
        for k, v_ in all_value_ranges.items():
            v_cleaned = v_.replace(' ', '')
            if val_cleaned.casefold() == v_cleaned.casefold():
                mapped_values.append(k)
                break
        else:
            mapped_values.append(val)
    return ','.join(mapped_values)


def map_biogeographical_provinces(value):
    """
    Map biogeographical province values to api.laji.fi query parameters 
    """
    values = [v.strip() for v in value.split(',')]
    mapped_values = []
    for val in values:
        val_cleaned = re.sub(r'\([^)]*\)', '', val).replace(' ', '')
        for k, v_ in id_mapping.items():
            if val_cleaned.casefold() == v_.casefold():
                mapped_values.append(k)
                break
        else:
            mapped_values.append(val)
    return ','.join([v for v in mapped_values if v is not None])

def map_value(value, filter_name, access_token, base_url):
    """
    Map filter values to api.laji.fi query parameters 
    """
    mappings = get_filter_values(filter_name, access_token, base_url)
    
    case_insensitive_mappings = {k.replace(' ', '').casefold(): v for k, v in mappings.items()}

    values = [v.strip() for v in value.split(',')]
    mapped_values = [case_insensitive_mappings.get(val.replace(' ', '').casefold(), val) for val in values]
    return ','.join([v for v in mapped_values if v is not None])


def map_municipality(municipals_ids, value):
    """
    Map municipalities to api.laji.fi query ids 
    """
    values = [v.strip() for v in value.split(',')]
    mapped_values = [municipals_ids.get(val, val) for val in values]
    return ','.join([v for v in mapped_values if v is not None])


def convert_time(value):
    """
    Convert time values so that they can be used to filter api.laji.fi
    """
    if isinstance(value, str):
        value = re.sub(r'\[\s*\d{1,2}:\d{2}\s*\]', '', value) # Remove time values in square brackets (e.g. [9:41])
        value = re.sub(r'\s+', '', value) # Remove all whitespace
        values = [v.strip() for v in value.split(',')] # Handle multiple values (OR search)
        converted_values = []
        for v in values:
            if '/' in v: # Handle relative days (e.g. -7/0)
                start, end = v.split('/')
                start = start.strip()
                end = end.strip()
                if start.lstrip('-').isdigit() and end.lstrip('-').isdigit(): # If both are integers, treat as relative days
                    converted_values.append(f"{start}/{end}")
                else:
                    converted_values.append(f"{start}/{end}")
            else:
                converted_values.append(v) # Single date or year/month
        return ','.join(converted_values)
    return value

def process_bbox(bbox):
    """Return bbox as WKT POLYGON in EUREF-TM35FIN (EPSG:3067)."""
    logger.info(f"Processing bbox: {bbox}")

    ymin, xmin, ymax, xmax = bbox # Weird order due to the pygeoapi's crs handling?

    # Determine if bbox appears to be WGS84
    if -180 <= xmin <= 180 and -90 <= ymin <= 90 and -180 <= xmax <= 180 and -90 <= ymax <= 90:
        transformer = Transformer.from_crs("EPSG:4326", "EPSG:3067", always_xy=True)
        xmin, ymin = transformer.transform(xmin, ymin)
        xmax, ymax = transformer.transform(xmax, ymax)
    return (f"POLYGON(({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))")
    

# TODO: Handle other time/date values 
# TODO: Handle ids with # character