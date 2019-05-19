To run data_ingestion.py, do:

1. Install dependencies: pip install -r requirements.txt
2. Set your `GOOGLE_APPLICATION_CREDENTIALS`
3. `bq mk funding`
4. `usage: data_ingestion.py [-h] [--input INPUT] [--output OUTPUT] --project
                         PROJECT --jobname JOBNAME --staging_location
                         STAGING_LOCATION --temp_location TEMP_LOCATION
                         [--runner RUNNER]`
