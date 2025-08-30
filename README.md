# Nasdaq Riga
Extract live corporate bond data from Nasdaq Riga website and save to Big Query.

## Installation
- Selenium probably needs Chrome driver installed separately. Check its installation instructions.
- Also need a project on Google Cloud Platform.
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