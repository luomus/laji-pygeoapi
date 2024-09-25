

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
```
Where
| Variable name | Definition | Default value |
|--|--|--|
| POSTGRES_DB| The database name|my_geospatial_db|
| POSTGRES_USER| The database user|postgres |
| POSTGRES_PASSWORD| The password associated with the default user | *not set*|
| POSTGRES_HOST| The host running the database| postgres |
| PAGES| Integer to download a specific number of pages. *"0"* to empty the database. *"all"* to add all data (this takes a lot of time), *"latest"* to add only the latest data after the last update | latest |
| BRANCH| The GitHub branch|dev|
| MULTIPROCESSING| Enables (*"True"*) or disables (*"False"*) multiprocessing when downloading data from the source APIs| False               |
| RUNNING_IN_OPENSHIFT| *"True"* when Pygeoapi is running in an OpenShift / Kubernetes environment. *"False"* when locally in a Docker.| False |
| ACCESS_TOKEN| API Access token needed for using the source APIs. See instruction: https://api.laji.fi/explorer/ | loremipsum12456789 |

### 4. Init the database:
```
./scripts/init-db.sh
./scripts/flask-command.sh db upgrade
```

### 5. Run docker command:
```
docker compose up --build
```

6. That's it! Your Pygeoapi instance with the PostGIS  database should now be up and running.

## Usage
Once the Docker container is up and running, you can access the Pygeoapi service through your web browser. By default, the service is available at [http://localhost:5000](http://localhost:5000).

## Configuration
The configuration file for Pygeoapi is named `pygeoapi-config.yml`. You can modify these files in a text editor to customize the behavior of the Pygeoapi service or connect to a different PostGIS database tables.

To edit the data or database, see Python files in the `src` folder. 

# Openshift Installation
See https://github.com/luomus/laji-pygeoapi/blob/dev/openshift/README.md
