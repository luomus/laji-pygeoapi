

# Pygeoapi for laji.fi occurrence data
Pygeoapi server installation with PostGIS database for laji.fi occurrence data.

# Docker Installation (Local)
To install and run the Pygeoapi instance locally, follow these steps:

### 1. Clone the directory
```
git clone https://github.com/luomus/laji-pygeoapi.git
```

### 2. Go to the directory
```
cd laji-pygeoapi
```

### 3. Create .env file only for local use, for example:
```
POSTGRES_DB=my_geospatial_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=admin123
POSTGRES_HOST=postgres
PAGES=latest
BRANCH=dev
MULTIPROCESSING=False
RUNNING_IN_OPENSHIFT=False
LAJI_API_URL=https://apitest.laji.fi/v0/
ACCESS_TOKEN=loremipsum12456789
INTERNAL_POSTGRES_DB=my_internal_db
INTERNAL_POSTGRES_USER=pygeoapi
INTERNAL_POSTGRES_PASSWORD=admin456
INTERNAL_POSTGRES_HOST=postgres
ACCESS_EMAIL=your@email.com
TARGET=default
```
Where
| Variable name | Definition | Default value |
|--|--|--|
| POSTGRES_DB| The database name|my_geospatial_db|
| POSTGRES_USER| The database user|postgres |
| POSTGRES_PASSWORD| The password associated with the default user | *not set*|
| POSTGRES_HOST| The host running the database| postgres |
| PAGES| Integer to download a specific number of pages. *"0"* to empty the database. *"all"* to add all data (this takes a lot of time), *"latest"* to add only the latest data after the last update | latest |
| MULTIPROCESSING| Enables (*"True"*) or disables (*"False"*) multiprocessing when downloading data and calculating indexes| False |
| RUNNING_IN_OPENSHIFT| *"True"* when Pygeoapi is running in an OpenShift / Kubernetes environment. *"False"* when locally in Docker.| False |
| ACCESS_TOKEN| API Access token needed for using the source APIs. See instruction: https://api.laji.fi/explorer/ | loremipsum12456789 |
| INTERNAL_POSTGRES_DB| Name for the internal database | my_internal_db |
| INTERNAL_POSTGRES_USER| Username for the internal database | pygeoapi |
| INTERNAL_POSTGRES_PASSWORD| Password for the internal database | admin456 |
| INTERNAL_POSTGRES_HOST| Hostname for the internal database | postgres |
| TARGET| Either "default" or "virva" | default |

### 4. Init the database:
In the root directory, run:
```
./scripts/init-db.sh
```
*Note*: If you get "Permissions denied" error, you may have to change the permissions of the file `init-database.sh`.
On Windows, you may need to provide full paths and ensure that Docker Desktop is open.

### 5. Run docker command:
```
docker compose up --build
```

6. That's it! Your Pygeoapi instance with the PostGIS  database should now be up and running. You can stop it by pressing CTRL + C. 

## Usage
Once the Docker container is running, you can access the Pygeoapi service through your web browser at [http://localhost:5000](http://localhost:5000).

## Configuration
The configuration file for Pygeoapi is named `pygeoapi-config.yml`. You can modify these files in a text editor to customize the behavior of the Pygeoapi service or connect to a different PostGIS database tables. 

Note: the config file is generated mostly automatically by the Python scripts located in the `src` directory. 

# Openshift Installation
See https://github.com/luomus/laji-pygeoapi/blob/dev/openshift/README.md
