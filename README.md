# laji_pygeoapi
Pygeoapi installation with PostGIS test database for laji.fi occurrence data

# Docker Deploy
1. Clone the directory
```
git clone https://github.com/luomus/laji_pygeoapi.git
```

3. Go to the directory
```
cd laji_pygeoapi
```

2. Run docker command:
```
docker compose up --build
```

7. Go to the http://localhost:5000/

8. If you want to modify the API, open ```pygeoapi-config.yml``` to edit and then run ```docker compose up``` again

