import geopandas as gpd
from sqlalchemy import create_engine, text
from sqlalchemy_utils import database_exists, create_database
import re

print("start")
# Path to the GeoJSON file
gpkd = r'scripts\HBF_86582_geo.gpkg'
template_resource = r'scripts\template_resource.txt'
pygeoapi_config = r'pygeoapi-config.yml'

# Database connection parameters
db_params = {
    'dbname': 'my_geospatial_db',
    'user': 'postgres',
    'password': 'admin123',
    'host': 'localhost',
    'port': '5433'
}

# Read file
gdf = gpd.read_file(gpkd, rows=10000)
crs = gdf.crs
gdf = gdf[['record_id', 'reported_name', 'common_name_finnish', 'scientific_name_interpreted', 'informal_groups_finnish', 'informal_groups_english', 'geometry']]

# Split column so species can be classified based on main group e.g. "Birds"
gdf['class'] = gdf['informal_groups_english'].str.split(';', expand=True)[0]
print(gdf.head())

# Connect to the PostGIS database

engine = create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")
print("Connection created")

# Create a new database if not exist
if not database_exists(engine.url):
    create_database(engine.url)

# Add Postgis plugin if not added
with engine.connect() as connection:
    connection.execute(text('CREATE EXTENSION IF NOT EXISTS postgis;'))
    connection.commit()


def clean_table_name(group_value):
    ''' Function to clean and return a table name '''
    
    # If name not exists
    if group_value is None or group_value == '':
        return 'unclassified'
    
    # Remove non-alphanumeric characters and white spaces
    cleaned_value = re.sub(r'[^\w\s]', '', group_value).replace(' ', '_')   

    # Shorten the name
    if len(cleaned_value) > 40:
        cleaned_value = cleaned_value[:40]

    return f'{cleaned_value}'

def get_bbox(sub_gdf):
    minx, miny, maxx, maxy = sub_gdf.geometry.total_bounds
    return [minx, miny, maxx, maxy]

def add_to_pygeoapi_config(template_resource, template_params, pygeoapi_config):
    '''This function adds data sets to the pygeoapi config file'''

    # Read the template file
    with open(template_resource, "r") as file:
        template = file.read()

    # Replace placeholders with real values
    for key, value in template_params.items():
        template = template.replace(key, value)
   
    # Append filled tempalte to the config file
    with open(pygeoapi_config, "a") as file:
        file.write(template)
        print(f"Table {template_params['<placeholder_table_name>']} added to the pygeoapi config file")

# Iterate over unique values of the "class" attribute
for group_value in gdf['class'].unique():
    
    # Filter the GeoDataFrame based on species
    sub_gdf = gdf[gdf['class'] == group_value]
    
    # Get cleaned table name, bounding box (bbox) and number of rows (n_rows)
    table_name = clean_table_name(group_value)
    bbox = get_bbox(sub_gdf)
    n_rows = len(sub_gdf)

    template_params = {
        "<placeholder_table_name>": table_name,
        "<placeholder_bbox>": str(bbox)
    }

    add_to_pygeoapi_config(template_resource, template_params, pygeoapi_config)

    # Create a PostGIS table 
    sub_gdf.to_postgis(table_name, engine, if_exists='replace', schema='public', index=False)
    print(f"In total {n_rows} rows of {table_name} inserted to the database")


print("All data inserted successfully into the PostGIS database and pygeoapi config file.")
print("API is ready to use.")