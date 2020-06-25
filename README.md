# geo-flask

API to interact with NCBI GEO and analytic database



## Build Contianer

docker build -t ngeo .

## Run services

docker run -p 80:5000 -d ngeo
docker run -p 8080:5000 -d ngeo