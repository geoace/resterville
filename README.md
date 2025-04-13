
# About RESTerville
RESTerville is a cloud-native data automation software which has been developed specifically for geospatial workflows. Development and licensing of the initial offering is currently wrapping up. Once complete, this brand new toolkit will be made public and open source forever. Our committment to you is that all features, once tested vetted by the developer community, will be made available to everyone -- without any hidden costs or premium versions. Open source software is the foundation of what we've built in RESTerville. As such, we want to share what has helped us so that it may in turn help you!

Once we complete feature testing, we'll make the repository publicly accessible and create training videos. They'll be free for you to use for as long as you need it. It's our gift to you.

# GETTING STARTED FROM SOURCE
#### Clone the repository
`git clone https://github.com/geoace/resterville.git`

#### Create a copy of the .env-template file and modify it according to your needs
`cp .env-template .env`

`nano .env`

#### Add bucket credentials as needed

#### Build the image
`docker build -t resterville .`

#### Run the container, adding in postgresql connection strings (shown here as environment variables) and credentials (shown here as volumes) as needed
```shell
docker run -d -p 8080:8080 --env-file ./env/.env \
-e PG_CONNECTION="dbname='my_db' user='viewer' host='localhost' port='5432' password='super!secret';dbname='my_db2' user='superuser_1' host='localhost' port='5432' password='superb!secret'" \
-v /home/linux_user/resterville/env/credentials.json:/app/env/credentials.json \
resterville
```

# GETTING STARTED FROM THE CLOUD (GCP example; Video tutorial coming as funding allows)
## SET UP INSTRUCTIONS
Create a Project...

#### Bucket
1. Go to cloud storage
2. Name, region preference, etc. (I generally just add a name like "agol_backups" and keep defaults)
3. Do NOT enforce public access prevention (gives option to share things publicly as desired)

#### Set up Bucket Administrator Service Account
4. Go to IAM & Admin
5. Service Accounts > Create Service Account
6. Service Account Name: bucket-admin
7. Assign roles to bucket-admin service account: Storage Admin, Storage Folder Admin, Storage Object Admin, Storage Object Viewer
8. Click on service account > keys > add key > json (downloads to your computer)

#### Load key into GCP secret manager
9. If needed, add API
10. Create secret (give it a name)
11. Upload recently-created json file (see step 5 above)
12. Create Secret

#### Artifact Registry
13. Activate cloud shell and run the following commands (change region and project name as appropriate)
```shell
docker pull geoace/resterville:latest
docker tag geoace/resterville:latest REGION-docker.pkg.dev/PROJECT_NAME/gcf-artifacts/resterville:latest
docker push REGION-docker.pkg.dev/PROJECT_NAME/gcf-artifacts/resterville
```
#### Cloud Run
14. Create Service
15. Select "Artifact Registry" radio button
16. Press the "Select" button and navigate to your newly-pushed container in the docker registry
17. Fill out region, authentication, allocation, etc. NOTE: We recommend at least 4 GB memory and 2 CPUs (scale up if moving very large data volumes)
18. Variables should be filled out according to what tools you plan on using. 
    - For ArcGIS workflows, ensure ARCGIS_CLIENT_ID, ARCGIS_PORTAL_URL, ARCGIS_CLIENT_SECRET, ARCGIS_PASSWORD, and ARCGIS_USER are set (if ARCGIS_PORTAL_URL is left blank, it will default to arcgis.com). 
    - If using postgresql, then fill out PG_CONNECTION information (see documentation for format). - - If backing up to a GCP Bucket, then ensure GOOGLE_APPLICATION_CREDENTIALS are mounted to app/secrets (see photos). 
    - When using the BUCKET variable on setup, all Bucket-related workflows will default to the provided bucket when the optional parameter is not provided. 
    - API-KEY will be the passcode required to request workflows from the server, so keep it safe! We recommend storing all sensitive information in secret manager. 
19. Deploy and visit the GCP-provided HTTPS URL. If you see the RESTerville logo, you're good to go!

# Function Usage
See API Documentation at https://resterville.org/docs.php#

There are jupyter notebook "body builder" help documents here which help construct POST bodies: https://github.com/geoace/RESTerville_help

# LICENSING

This project is licensed under the GNU General Public License v3.0. See the LICENSE file for more information.

### Third-Party Licenses

- **ArcGIS API for Python**: This project uses the ArcGIS API for Python, which is licensed under the Apache License 2.0. See https://github.com/Esri/arcgis-python-api/blob/master/LICENSE for more details.
- **Google Cloud Components**: This project incorporates work licensed under the Creative Commons Attribution 4.0 International License. See https://creativecommons.org/licenses/by/4.0/ for more details.

### Attribution

- **Google Cloud Components**: This project uses code or data provided by Google, available at https://cloud.google.com/python/docs/reference.






