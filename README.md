# laji_pygeoapi
Pygeoapi installation with PostGIS database for laji.fi occurrence data

# Docker Deploy
1. Clone the directory
```
git clone https://github.com/luomus/laji_pygeoapi.git
```

3. Go to directory
```
cd laji_pygeoapi
```

2. Run docker command:
```
docker compose up postgres
```
	
4. Run python script (requires Python and geopandas, sqlalchemy_utils and sqlalchemy libraries installed):
```
python scripts/create_datadump.py
```

4. Press ctrl+c to in cmd to stop container

5. Run docker command:
```
docker compose up
```

7. Go to the http://localhost:5001/

8. If you want to modify the API, open ```pygeoapi-config.yml``` to edit 

