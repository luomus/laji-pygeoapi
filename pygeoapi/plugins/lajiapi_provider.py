import logging
import requests
from pygeoapi.provider.base import BaseProvider  # type: ignore
from pandas import notna

logger = logging.getLogger(__name__)

from pygeoapi.scripts.process_features import process_json_features
from pygeoapi.scripts.main import setup_environment
from pygeoapi.scripts.load_data import load_or_update_cache
from pygeoapi.scripts.convert_api_filters import convert_filters

class LajiApiProvider(BaseProvider):
    """
    Custom api.laji.fi provider for pygeoapi. This provider fetches data from api.laji.fi and processes it into a GeoJSON feature collection following the same schema as other data in this API.
    """

    def __init__(self, provider_def):
        super().__init__(provider_def)
        config = setup_environment()
        self.api_url = config['laji_api_url'] + 'warehouse/query/unit/list'
        self.access_token = config.get('access_token', 'missing_from_config?')
        self.municipals_gdf, self.municipals_ids, self.lookup_df, self.taxon_df, self.collection_names, self.all_value_ranges = load_or_update_cache(config)

    def get_fields(self):
        """
        Get fields and field types from the CSV for pygeoapi.
        """
        fields = {}
        for _, row in self.lookup_df.iterrows():
            finbif_query = row['finbif_api_query']
            if notna(finbif_query):
                field_name = row['virva']
                field_type = row['type']
                fields[field_name] = {"type": field_type}
        return fields

    @property
    def fields(self):
        """
        Return the fields of the provider.
        This is used to describe the schema of the data.
        """
        logging.info("field called")
        return self.get_fields()

    def query(self, offset=0, limit=100, resulttype='results', bbox=[], datetime_=None, properties=[], sortby=[], select_properties=[], skip_geometry=False, **kwargs):
        """
        Query api.laji.fi response and get the results as a GeoJSON feature collection. Process the data.

        :param self: self
        :param offset: starting record to return (default 0)
        :param limit: number of records to return (default 100)
        :param resulttype: return results or hit limit (default results)
        :param bbox: bounding box [minx,miny,maxx,maxy]
        :param datetime_: temporal (datestamp or extent)
        :param properties: list of tuples (name, value)
        :param sortby: list of dicts (property, order)
        :param select_properties: list of property names
        :param skip_geometry: bool of whether to skip geometry (default False)
        :param kwargs: additional parameters

        :returns: dict of 0..n GeoJSON feature collection
        """
        logging.info("Query called")
        MAX_ITEMS = 1_000_000
        if offset >= MAX_ITEMS:
            raise ValueError(f"Fetching more than {MAX_ITEMS} 1 million observations is not allowed. Please use query filters.") 

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
            params['bbox'] = bbox

        params = convert_filters(self.lookup_df, self.all_value_ranges, self.municipals_ids, params, properties)

        # Set selected fields
        params['selected'] = 'document.loadDate,unit.facts,gathering.facts,document.facts,unit.linkings.taxon.threatenedStatus,unit.linkings.originalTaxon.administrativeStatuses,unit.linkings.taxon.taxonomicOrder,unit.linkings.originalTaxon.latestRedListStatusFinland.status,gathering.displayDateTime,gathering.interpretations.biogeographicalProvinceDisplayname,gathering.interpretations.coordinateAccuracy,unit.abundanceUnit,unit.atlasCode,unit.atlasClass,gathering.locality,unit.unitId,unit.linkings.taxon.scientificName,unit.interpretations.individualCount,unit.interpretations.recordQuality,unit.abundanceString,gathering.eventDate.begin,gathering.eventDate.end,gathering.gatheringId,document.collectionId,unit.breedingSite,unit.det,unit.lifeStage,unit.linkings.taxon.id,unit.notes,unit.recordBasis,unit.sex,unit.taxonVerbatim,document.documentId,document.notes,document.secureReasons,gathering.conversions.eurefWKT,gathering.notes,gathering.team,unit.keywords,unit.linkings.originalTaxon,unit.linkings.taxon.nameFinnish,unit.linkings.taxon.nameSwedish,unit.linkings.taxon.nameEnglish,document.linkings.collectionQuality,unit.linkings.taxon.sensitive,unit.abundanceUnit,gathering.conversions.eurefCenterPoint.lat,gathering.conversions.eurefCenterPoint.lon,document.dataSource,document.siteStatus,document.siteType,gathering.stateLand'

        # Get the data from the API
        response = requests.get(self.api_url, params=params)
        response.raise_for_status()
        data = response.json()

        if resulttype == 'hits':
            return {
                'type': 'FeatureCollection',
                'features': [],
                'numberMatched': data.get('total')
            }

        # Process the features in the response
        features = process_json_features(self, data)

        data['features'] = features
        return data

    def get(self, identifier, **kwargs):
        """
        Get single unit from the api.laji.fi by unitId.

        :param identifier: unitId ('unit.unitId' field in api.laji.fi, e.g. 'http://tun.fi/12345')

        :returns: `dict` of single record
        """

        # Decode identifier to restore '#'
        decoded_identifier = 'http://tun.fi/' + identifier.replace('_', '#')

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

        # Get the data from the API
        response = requests.get(self.api_url, params=params)
        response.raise_for_status()
        data = response.json()

        # Process the features in the response
        features = process_json_features(self, data)

        return features[0]
    
    def get_schema(self, schema_type=None):
        # Example: return a simple schema
        return ('application/geo+json', {'$ref': 'https://geojson.org/schema/Feature.json'})