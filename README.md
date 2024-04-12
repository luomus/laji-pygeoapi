# laji_pygeoapi
Pygeoapi installation with PostGIS test database for laji.fi occurrence data

# Docker Deploy
1. Clone the directory
```
git clone https://github.com/luomus/laji_pygeoapi.git
```

2. Go to the directory
```
cd laji_pygeoapi
```

3. Run docker command:
```
docker compose up --build
```

4. Run python script when postgres container is up:
```
python scripts/create_datadump_from_gpck.py
```

5. Press CTRL + to stop the docker and compose it up again
```
docker compose up
```

4. Go to the http://localhost:5000/ and see the data

5. If you want to modify the API, open ```pygeoapi-config.yml``` to edit and then run ```docker compose up``` again

