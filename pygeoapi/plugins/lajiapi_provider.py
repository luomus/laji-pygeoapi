import logging
import json
import requests
from pygeoapi.provider.base import BaseProvider  # type: ignore

logger = logging.getLogger(__name__)

from .process_features import process_json_features
from main import setup_environment, load_files

class LajiApiProvider(BaseProvider):
    """
    Custom api.laji.fi provider for pygeoapi. This provider fetches data from api.laji.fi and processes it into a GeoJSON feature collection following the same schema as other data in this API.
    """

    def __init__(self, provider_def):
        super().__init__(provider_def)
        self.api_url = provider_def['url']
        config = setup_environment()
        self.access_token = config.get('access_token', 'missing_from_config?')
        self.municipals_gdf, self.lookup_df, self.taxon_df, self.collection_names, self.all_value_ranges = load_files(config)

    def get_fields(self):
        """
        Get fields and field types from the JSON for pygeoapi.
        """
        # Read the JSON file
        with open("plugins/fields.json", "r", encoding="utf-8") as f:
            fields = json.load(f)
        return fields

    @property
    def fields(self):
        """
        Return the fields of the provider.
        This is used to describe the schema of the data.
        """
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
        params = {
            'page': (offset // limit) + 1,
            'pageSize': limit,
            'crs': 'WGS84',
            'featureType': 'ORIGINAL_FEATURE',
            'format': 'geojson',
            'access_token': self.access_token
        }

        # Handle bbox if provided
        if bbox and len(bbox) == 4:
            params['bbox'] = bbox


        # Add any additional filters from properties
        for name, value in properties:
            params[name] = value

        # Set selected fields
        params['selected'] = 'document.loadDate,unit.facts,gathering.facts,document.facts,unit.linkings.taxon.threatenedStatus,unit.linkings.originalTaxon.administrativeStatuses,unit.linkings.taxon.taxonomicOrder,unit.linkings.originalTaxon.latestRedListStatusFinland.status,gathering.displayDateTime,gathering.interpretations.biogeographicalProvinceDisplayname,gathering.interpretations.coordinateAccuracy,unit.abundanceUnit,unit.atlasCode,unit.atlasClass,gathering.locality,unit.unitId,unit.linkings.taxon.scientificName,unit.interpretations.individualCount,unit.interpretations.recordQuality,unit.abundanceString,gathering.eventDate.begin,gathering.eventDate.end,gathering.gatheringId,document.collectionId,unit.breedingSite,unit.det,unit.lifeStage,unit.linkings.taxon.id,unit.notes,unit.recordBasis,unit.sex,unit.taxonVerbatim,document.documentId,document.notes,document.secureReasons,gathering.conversions.eurefWKT,gathering.notes,gathering.team,unit.keywords,unit.linkings.originalTaxon,unit.linkings.taxon.nameFinnish,unit.linkings.taxon.nameSwedish,unit.linkings.taxon.nameEnglish,document.linkings.collectionQuality,unit.linkings.taxon.sensitive,unit.abundanceUnit,gathering.conversions.eurefCenterPoint.lat,gathering.conversions.eurefCenterPoint.lon,document.dataSource,document.siteStatus,document.siteType,gathering.stateLand'

        # Get the data from the API
        response = requests.get(self.api_url, params=params)
        response.raise_for_status()
        data = response.json()

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
        decoded_identifier = 'http://tun.fi/' + identifier.replace('~', '#')

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
        crs = params.get('crs', 'EPSG:4326')
        features = process_json_features(self, data, crs)

        return features[0]
    
    def get_schema(self, schema_type=None):
        # Example: return a simple schema
        return ('application/geo+json', {'$ref': 'https://geojson.org/schema/Feature.json'})
    
    
