# Nasdaq Riga
Extract live corporate bond data from Nasdaq Riga website and save to Big Query.

## Installation
- Selenium probably needs Chrome driver installed separately. Check its installation instructions.
- Also need a project on Google Cloud Platform.
- Add environment variables from `.env.exmaple` or install and use `python-dotenv`.
```
pip install -r requirements.txt
```

## Run
### Locally
GCP authenetication from command line, then just run script.
```
gcloud auth login
```
To skip GCP stuff and work with DataFrame locally, just comment out `to_gbq`.
### GCP
First setup repository authentication.
Setup instructions are in GCP's Artifact Registry.

Then, build and push to artifact registry:

```
docker buildx build --platform linux/amd64 . -t [LOCAL_IMAGE_NAME]:[TAG]
docker tag [LOCAL_IMAGE_NAME]:[TAG] [REGION]-docker.pkg.dev/[PROJECT_ID]/[REPOSITORY_NAME]/[IMAGE_NAME]:[TAG]
docker push [REGION]-docker.pkg.dev/[PROJECT_ID]/[REPOSITORY_NAME]/[IMAGE_NAME]:[TAG]
```

Finally, setup a Cloud Run job and optionally schedule it with Cloud Scheduler.