import logging
import requests
from pygeoapi.provider.base import BaseProvider, ProviderQueryError 
from pandas import notna

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

from pygeoapi.scripts.process_features import process_json_features
from pygeoapi.scripts.main import setup_environment
from pygeoapi.scripts.load_data import load_or_update_cache
from pygeoapi.scripts.convert_api_filters import convert_filters, process_bbox

class LajiApiProvider(BaseProvider):
    """
    Custom api.laji.fi provider for pygeoapi. This provider fetches data from api.laji.fi and processes it into a GeoJSON feature collection following the same schema as other data in this API.
    """

    def __init__(self, provider_def):
        super().__init__(provider_def)
                
        config = setup_environment()
        self.api_url = config['laji_api_url'] + 'warehouse/query/unit/list'
        self.access_token = config.get('access_token', None)
        
        self.municipals_gdf, self.municipals_ids, self.lookup_df, self.taxon_df, self.collection_names, self.all_value_ranges = load_or_update_cache(config)
        

    def get_fields(self):
        fields = {}
        for _, row in self.lookup_df.iterrows():
            finbif_query = row.get('finbif_api_query')
            if notna(finbif_query):
                field_name = row.get('virva')
                field_type = row.get('type')
                if field_type == 'int':
                    field_type = 'integer'
                elif field_type == 'str':
                    field_type = 'string'
                elif field_type == 'bool':
                    field_type = 'boolean'

                if field_name and field_type:
                    fields[field_name] = {"type": field_type}
                else:
                    logger.debug(f'Skipping row with missing field_name or field_type: {row}')
                            
        logger.debug(f'Retrieved {len(fields)} fields from lookup dataframe')
        return fields
    
    @property
    def fields(self):
        return self.get_fields()

    def _build_request_params(self, offset, limit, bbox, properties):
        params = {
            'page': (offset // limit) + 1,
            'pageSize': limit,
            'crs': 'WGS84',
            'featureType': 'CENTER_POINT',
            'format': 'geojson',
            'access_token': self.access_token
        }
        
        # Handle bbox if provided
        if bbox and len(bbox) == 4:
            params['polygon'] = process_bbox(bbox)

        params = convert_filters(self.lookup_df, self.all_value_ranges, self.municipals_ids, params, properties)

        # Set selected fields
        params['selected'] = 'document.loadDate,unit.facts,gathering.facts,document.facts,unit.linkings.taxon.threatenedStatus,unit.linkings.originalTaxon.administrativeStatuses,unit.linkings.taxon.taxonomicOrder,unit.linkings.originalTaxon.latestRedListStatusFinland.status,gathering.displayDateTime,gathering.interpretations.biogeographicalProvinceDisplayname,gathering.interpretations.coordinateAccuracy,unit.abundanceUnit,unit.atlasCode,unit.atlasClass,gathering.locality,unit.unitId,unit.linkings.taxon.scientificName,unit.interpretations.individualCount,unit.interpretations.recordQuality,unit.abundanceString,gathering.eventDate.begin,gathering.eventDate.end,gathering.gatheringId,document.collectionId,unit.breedingSite,unit.det,unit.lifeStage,unit.linkings.taxon.id,unit.notes,unit.recordBasis,unit.sex,unit.taxonVerbatim,document.documentId,document.notes,document.secureReasons,gathering.conversions.eurefWKT,gathering.notes,gathering.team,unit.keywords,unit.linkings.originalTaxon,unit.linkings.taxon.nameFinnish,unit.linkings.taxon.nameSwedish,unit.linkings.taxon.nameEnglish,document.linkings.collectionQuality,unit.linkings.taxon.sensitive,unit.abundanceUnit,gathering.conversions.eurefCenterPoint.lat,gathering.conversions.eurefCenterPoint.lon,document.dataSource,document.siteStatus,document.siteType,gathering.stateLand'

        return params

    def _make_api_request(self, params):
        # Log the request URL for debugging (without sensitive tokens)
        safe_params = {k: v if k != 'access_token' else '***' for k, v in params.items()}
        logger.info(f'Making API request with params: {safe_params}')
        
        response = requests.get(self.api_url, params=params)
        response.raise_for_status()
        
        data = response.json()
        
        # Check for API-specific error responses
        if 'error' in data:
            error_msg = data.get('error', {}).get('message', 'Unknown API error')
            raise ProviderQueryError(f'API Error: {error_msg}')
        
        MAX_ITEMS = 1_000_000
        if data.get('total', 0) > MAX_ITEMS and params.get('page', 0) > 1:
            logger.warning(f"API response contains more than {MAX_ITEMS:,} items. Please use query filters to limit results.")
            raise ProviderQueryError(f"Too many items in response: {data.get('total', 0)}")
        else:
            logger.debug(f'API request successful, received {data.get("pageSize", 0)} total results')
            return data
            

    def query(self, offset=0, limit=1000, resulttype='results', bbox=[], datetime_=None, properties=[], sortby=[], select_properties=[], skip_geometry=False, **kwargs):
        logger.debug(f"Query called with offset={offset}, limit={limit}, resulttype={resulttype}")
        
        params = self._build_request_params(offset, limit, bbox, properties)
            
        # Make API request
        data = self._make_api_request(params)

        if resulttype == 'hits':
            return {
                'type': 'FeatureCollection',
                'features': [],
                'numberMatched': data.get('total', 0)
            }

        features = process_json_features(self, data)
        logger.debug(f'Processed {len(features)} features')

        data['features'] = features
        data['numberReturned'] = len(features)
        return data #TODO: Separate different geometry types and return geopackage maybe?

    def get(self, identifier, **kwargs):
        """
        Get single unit from the api.laji.fi by unitId.

        :param identifier: unitId ('unit.unitId' field in api.laji.fi, e.g. 'http://tun.fi/12345')

        :returns: `dict` of single record
        """

        # Decode identifier to restore '#'
        decoded_identifier = 'http://tun.fi/' + str(identifier).replace('_', '#')

        # Fetch by id
        params = {
            'crs': 'WGS84',
            'featureType': 'ORIGINAL_FEATURE',
            'format': 'geojson',
            'access_token': self.access_token,
            'unitId': decoded_identifier
        }

        # Set selected fields
        params['selected'] = 'document.loadDate,unit.facts,gathering.facts,document.facts,unit.linkings.taxon.threatenedStatus,unit.linkings.originalTaxon.administrativeStatuses,unit.linkings.taxon.taxonomicOrder,unit.linkings.originalTaxon.latestRedListStatusFinland.status,gathering.displayDateTime,gathering.interpretations.biogeographicalProvinceDisplayname,gathering.interpretations.coordinateAccuracy,unit.abundanceUnit,unit.atlasCode,unit.atlasClass,gathering.locality,unit.unitId,unit.linkings.taxon.scientificName,unit.interpretations.individualCount,unit.interpretations.recordQuality,unit.abundanceString,gathering.eventDate.begin,gathering.eventDate.end,gathering.gatheringId,document.collectionId,unit.breedingSite,unit.det,unit.lifeStage,unit.linkings.taxon.id,unit.notes,unit.recordBasis,unit.sex,unit.taxonVerbatim,document.documentId,document.notes,document.secureReasons,gathering.conversions.eurefWKT,gathering.notes,gathering.team,unit.keywords,unit.linkings.originalTaxon,unit.linkings.taxon.nameFinnish,unit.linkings.taxon.nameSwedish,unit.linkings.taxon.nameEnglish,document.linkings.collectionQuality,unit.linkings.taxon.sensitive,unit.abundanceUnit,gathering.conversions.eurefCenterPoint.lat,gathering.conversions.eurefCenterPoint.lon,document.dataSource,document.siteStatus,document.siteType,gathering.stateLand'

        # Make API request
        data = self._make_api_request(params)

        # Process the features in the response
        features = process_json_features(self, data)

        return features[0]
    
    def get_schema(self, schema_type=None):       
        # Build properties schema from self.fields
        properties = {}
        for field, info in self.fields.items():
            try:
                # Map to JSON Schema types
                field_type = info.get('type', 'string')
                if field_type == 'number':
                    json_type = 'number'
                elif field_type == 'integer':
                    json_type = 'integer'
                elif field_type == 'boolean':
                    json_type = 'boolean'
                else:
                    json_type = 'string'
                properties[field] = {"type": json_type}
            except Exception as e:
                logger.warning(f'Error processing field {field}: {e}')
                properties[field] = {"type": "string"}  # fallback

        schema = {
            "$schema": "http://json-schema.org/draft/2020-12/schema#",
            "title": "Feature properties", 
            "type": "object",
            "properties": properties
        }

        logger.info(f"Generated schema with {len(properties)} properties")
        return ("application/schema+json", schema)
            

    def __repr__(self) -> str:
        return f'<LajiApiProvider> {self.api_url}'