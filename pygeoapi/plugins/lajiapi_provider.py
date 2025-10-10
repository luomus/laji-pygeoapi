import logging
import requests
from pygeoapi.provider.base import BaseProvider, ProviderQueryError
from pandas import notna
from scripts.process_features import process_json_features
from scripts.main import setup_environment
from scripts.load_data import load_or_update_cache, _get_api_headers
from scripts.convert_api_filters import convert_filters, process_bbox

logger = logging.getLogger(__name__)

class LajiApiProvider(BaseProvider):
    """Custom api.laji.fi provider for pygeoapi."""

    include_extra_query_parameters = True

    def __init__(self, provider_def):
        super().__init__(provider_def)
        self.config = setup_environment()
        self.include_extra_query_parameters = True
        self.api_url = self.config['laji_api_url'] + 'warehouse/query/unit/list'
        self.access_token = self.config.get('access_token')
        self.municipals_gdf, self.municipals_ids, self.lookup_df, self.taxon_df, \
            self.collection_names, self.all_value_ranges = load_or_update_cache(self.config)
        self._cached_fields = None
        self.selected_fields = ",".join([field for field in self.lookup_df['selected'].dropna().to_list() if field])

    def get_fields(self):
        if self._cached_fields is not None:
            return self._cached_fields
        fields = {}
        for _, row in self.lookup_df.iterrows():
            finbif_query = row.get('finbif_api_query')
            description = row.get('description')
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
                    fields[field_name] = {"type": field_type, "title": description}
                else:
                    logger.debug('Skipping row with missing field_name or field_type: %s', row)
        logger.debug('Retrieved %d fields from lookup dataframe', len(fields))
        self._cached_fields = fields
        return self._cached_fields

    @property
    def fields(self):
        return self.get_fields()

    def _build_request_params(self, offset, limit, bbox, properties):
        # Basic parameter validation
        try:
            limit_int = int(limit)
            offset_int = int(offset)
        except (TypeError, ValueError):
            raise ProviderQueryError(self._error_message('invalid-parameter', 'offset/limit must be integers', hint='Use numeric offset and limit query parameters.'))
        if limit_int <= 0 or limit_int > 10000:
            raise ProviderQueryError(self._error_message('invalid-parameter', f'limit {limit_int} outside allowed range 1-10000', hint='Reduce limit to <= 10000.'))
        if offset_int < 0:
            raise ProviderQueryError(self._error_message('invalid-parameter', f'offset {offset_int} must be >= 0'))

        params = {
            'page': (offset // limit) + 1,
            'pageSize': limit,
            'crs': 'WGS84',
            'featureType': 'CENTER_POINT',
            'format': 'geojson'
        }
        if bbox and len(bbox) == 4:
            params['polygon'] = process_bbox(bbox)
        params = convert_filters(self.lookup_df, self.all_value_ranges, self.municipals_ids, params, properties, self.config)
        params['selected'] = self.selected_fields

        return params

    def _make_api_request(self, params):
        headers = _get_api_headers(self.access_token)
        try:
            response = requests.get(self.api_url, params=params, headers=headers, timeout=300)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.Timeout:
            raise ProviderQueryError(self._error_message('timeout', 'Upstream request timed out after 300 seconds', hint='Try narrowing filters or reducing limit.'))
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else None
            raise ProviderQueryError(self._error_message('upstream-http-error', f'Upstream HTTP error {status}', hint='Check filter validity or try again later.'))
        except requests.exceptions.RequestException as e:
            raise ProviderQueryError(self._error_message('network-error', f'Network error contacting upstream: {e}'))

        if isinstance(data, dict) and 'error' in data:
            error_msg = data.get('error', {}).get('message', 'Unknown API error')
            raise ProviderQueryError(self._error_message('upstream-error', error_msg))
        MAX_ITEMS = 1_000_000
        if data.get('total', 0) > MAX_ITEMS and params.get('page', 0) > 1:
            logger.error('API response contains more than %s items.', MAX_ITEMS)
            raise ProviderQueryError(self._error_message('too-many-results', f"Too many items in response: {data.get('total', 0)}", hint='Refine with datetime, bbox, taxon, collection, or other filters.'))
        logger.debug('API request successful, received %s results', data.get('pageSize', 0))
        return data

    def query(self, offset=0, limit=1000, resulttype='results', bbox=None, datetime_=None, properties=None, sortby=None, select_properties=None, skip_geometry=False, **kwargs):
        logger.debug('Query called offset=%s limit=%s resulttype=%s', offset, limit, resulttype)
        if bbox is None:
            bbox = []
        if properties is None:
            properties = []
        params = self._build_request_params(offset, limit, bbox, properties)
        try:
            data = self._make_api_request(params)
            if resulttype == 'hits':
                return {'type': 'FeatureCollection', 'features': [], 'numberMatched': data.get('total', 0)}
            features = process_json_features(self, data)
            logger.debug('Processed %d features', len(features))
            data['features'] = features
            data['numberReturned'] = len(features)
            return data
        except ProviderQueryError as e:
            # Re-raise so pygeoapi error handling captures message
            raise
        except Exception as e:
            raise ProviderQueryError(self._error_message('internal-error', f'Unexpected provider error: {e}'))

    def get(self, identifier, **kwargs):
        """
        Get single unit from the api.laji.fi by unitId.

        :param identifier: unitId ('unit.unitId' field in api.laji.fi, e.g. 'http://tun.fi/12345')

        :returns: `dict` of single record
        """

        # Decode identifier to restore '#'
        decoded_identifier = str(identifier).replace('_', '#')
        logging.debug('Get called for identifier: %s', decoded_identifier)
        params = {
            'crs': 'WGS84',
            'featureType': 'ORIGINAL_FEATURE',
            'format': 'geojson',
            'unitId': decoded_identifier
        }
        params['selected'] = self.selected_fields
        try:
            data = self._make_api_request(params)
            features = process_json_features(self, data)
            return features[0]
        except ProviderQueryError:
            raise
        except Exception as e:
            raise ProviderQueryError(self._error_message('internal-error', f'Unexpected provider error: {e}'))

    def get_schema(self, schema_type=None):
        properties = {}
        for field, info in self.fields.items():
            try:
                field_type = info.get('type', 'string')
                if field_type not in {'number', 'integer', 'boolean', 'string'}:
                    field_type = 'string'
                properties[field] = {"type": field_type}
            except Exception as e:
                logger.warning('Error processing field %s: %s', field, e)
                properties[field] = {"type": "string"}
        schema = {
            "$schema": "http://json-schema.org/draft/2020-12/schema#",
            "title": "Feature properties",
            "type": "object",
            "properties": properties
        }
        logger.debug('Generated schema with %d properties', len(properties))
        return ("application/schema+json", schema)

    def __repr__(self) -> str:
        return f'<LajiApiProvider> {self.api_url}'
    
    def get_metadata(self):
        logger.info("Get_metadata called!")
        return {"title": 'metadata', "description": 'description'}

    # ----------------- helpers -----------------
    def _error_message(self, code, message, hint=None):
        err = {
            'type': f'urn:pygeoapi:lajiapi:{code}',
            'title': code.replace('-', ' ').title(),
            'detail': message
        }
        if hint:
            err['hint'] = hint
        return err