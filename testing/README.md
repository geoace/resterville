# VSCode debugging

## The following will setup a postgres instance in a docker container for use with testing

### Prep:
1. Download your GPC process credential json file to env\credentials.json
2. copy /env/.env-template tp /env/.env
3. Update .env with GOOGLE_APPLICATION_CREDENTIALS set to your credential file
4. Run the following in a terminal window:
This assumes you cloned the repo to C:\GitHub\resterville
```shell
cd testing
mkdir data
docker-compose up -d
cd ../
docker build -t resterville .
docker run -d -p 8080:8080 --env-file ./env/.env -e PG_CONNECTION="dbname='mygisdb' user='postgres' host='192.168.1.8' port='5432' password='mysecretpassword'" -v C:\GitHub\resterville\env\credentials.json:/app/secrets/bucket-credentials resterville
```
5. Launch the Python Debugger: Flask launch template in vscode debug
6. Place your break points
7. Open your browser and go to: http://localhost:5000/agol2pg?api_key=API4me&service=mygisdb_postgres&url=https://services9.arcgis.com/IUhP9plEzDTayUVC/arcgis/rest/services/Hydrant/FeatureServer/2&table=utl_water_hydrant&schema=public&geometry_name=geom&batch=2000&save_attachments=true&bucket=coz-assets&loglevel=debug&oid=id
