# laji_pygeoapi
Pygeoapi server installation with PostGIS database for laji.fi occurrence data.

# Installation with Docker
To install and run the Pygeoapi instance locally, follow these steps:

1. Clone the directory
```
git clone https://github.com/luomus/laji-pygeoapi.git
```

2. Go to the directory
```
cd laji-pygeoapi
```

3. Run docker command:
```
docker compose up --build
```
4. That's it! Your Pygeoapi instance with the PostGIS test database should now be up and running.

# Usage

Once the Docker container is up and running, you can access the Pygeoapi service through your web browser. By default, the service is available at [http://localhost:5000](http://localhost:5000).

# Configuration

The configuration file for Pygeoapi is named `pygeoapi-config.yml`. You can modify these files in a text editor to customize the behavior of the Pygeoapi service or connect to a different PostGIS database tables.

To edit the data or database, see Python files in the `scripts` folder. 
