import logging
from pygeoapi.scripts.compute_variables import id_mapping
import re


logger = logging.getLogger(__name__)

def convert_filters(lookup_df, all_value_ranges, municipals_ids, params, properties):
    """
    Converts filters from virva Finnish language scheme to be suitable for querying api.laji.fi data warehouse endpoint.
    For example, "Aineiston_tunniste=http://tun.fi/HR.95" is converted to "collectionId=HR.95" that can be used to query warehouse endpoint. 
    """
    for name, value in properties:
        logging.info(f"Name: {name}, value: {value}")
        name = translate_filter_names(lookup_df, name)
        value = remove_tunfi_prefix(value)
        if name in ['breedingSite', 'recordBasis', 'secureReason', 'collectionQuality', 'redListStatusId', 'administrativeStatusId', 'atlasClass', 'atlasCode']:
            value = map_value_ranges(all_value_ranges, value)
        elif name == 'biogeographicalProvinceId':
            value = map_biogeographical_provinces(value)
        elif name == 'sex':
            value = map_sex(value)
        elif name == 'lifeStage':
            value = map_lifestage(value)
        elif name == 'finnishMunicipalityId':
            value = map_municipality(municipals_ids, value)
        elif name == 'time':
            value = convert_time(value)
        logging.info(f"Name: {name}, value: {value}")
        params[name] = value
    return params

def translate_filter_names(lookup_df, name):
    """
    Map filter names from virva to api.laji.fi warehouse filters
    """
    if name in lookup_df['virva'].values:
        return lookup_df.loc[lookup_df['virva'] == name, 'finbif_api_query'].values[0]
    return name


def remove_tunfi_prefix(value):
    """
    Remove 'http://tun.fi/' from the filter values
    """
    if isinstance(value, str) and 'http://tun.fi/' in value:
        return value.replace('http://tun.fi/', '')
    return value


def map_value_ranges(all_value_ranges, value):
    """
    Map filter values to api.laji.fi query parameters 
    For example, recordBasis value 'Havaittu' is mapped to 'HUMAN_OBSERVATION_UNSPECIFIED'
    """
    values = [v.strip() for v in value.split(',')]
    mapped_values = []
    for val in values:
        for k, v_ in all_value_ranges.items():
            if val.casefold() == v_.casefold():
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
        for k, v_ in id_mapping.items():
            if val.casefold() == v_.casefold():
                mapped_values.append(k)
                break
        else:
            mapped_values.append(val)
    return ','.join([v for v in mapped_values if v is not None])


def map_sex(value):
    """
    Map sex values to api.laji.fi query parameters 
    """
    sex_mappings = {
        'naaras': 'FEMALE',
        'koiras': 'MALE',
        'työläinen': 'WORKER',
        'tuntematon': 'UNKNOWN',
        'soveltumaton': 'NOTAPPLICABLE',
        'gynandromorfi': 'GYNANDROMORPH',
        'eri sukupuolia': 'MULTIPLE',
        'ristiriitainen': 'CONFLICTING'
    }
    values = [v.strip() for v in value.split(',')]
    mapped_values = [sex_mappings.get(val.casefold(), val) for val in values]
    return ','.join([v for v in mapped_values if v is not None])


def map_lifestage(value):
    """
    Map lifestage values to api.laji.fi query parameters 
    """
    lifestage_mappings = {
        'aikuinen': 'ADULT',
        'nuori': 'JUVENILE',
        'keskenkasvuinen': 'IMMATURE',
        'muna': 'EGG',
        'nuijapää': 'TADPOLE',
        'kotelo': 'PUPA',
        'nymfi': 'NYMPH',
        'subimago': 'SUBIMAGO',
        'toukka': 'LARVA',
        'kelo': 'SNAG',
        'alkio tai sikiö': 'EMBRYO',
        'esiaikuinen': 'SUBADULT',
        'sukukypsä': 'MATURE',
        'äkämä': 'GALL',
        'jäljet': 'MARKS',
        'triunguliini': 'TRIUNGULIN',
        'steriili': 'STERILE',
        'fertiili': 'FERTILE',
        'verso': 'SPROUT',
        'kuollut verso': 'DEAD_SPROUT',
        'nuppu': 'BUD',
        'kukka': 'FLOWER',
        'kuihtunut kukka': 'WITHERED_FLOWER',
        'siemen': 'SEED',
        'itiö': 'SEED',
        'hedelmä': 'SEED',
        'siemen / itiö / hedelmä': 'SEED',
        'mukula': 'SUBTERRANEAN',
        'sipuli': 'SUBTERRANEAN',
        'juuri': 'SUBTERRANEAN',
        'mukula / sipuli / juuri': 'SUBTERRANEAN'
    }
    values = [v.strip() for v in value.split(',')]
    mapped_values = [lifestage_mappings.get(val.casefold(), val) for val in values]
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

# TODO: Handle other time/date values 
# TODO: Handle ids with # character