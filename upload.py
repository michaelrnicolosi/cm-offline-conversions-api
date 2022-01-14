from googleapiclient import discovery
from google.oauth2 import service_account
from google.cloud import secretmanager
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import json
import time


def upload_conversions(request):

    secret_client = secretmanager.SecretManagerServiceClient()
    secret_name = f"projects/<PROJECT-CODE>/secrets/offline-conversions/versions/1"
    secret_response = secret_client.access_secret_version(request={"name": secret_name})
    payload = secret_response.payload.data.decode("UTF-8")
    json_payload = json.loads(payload)

    credentials = service_account.Credentials.from_service_account_info(json_payload)

    API_NAME = 'dfareporting'
    API_VERSION = 'v3.4'
    API_SCOPES = ['https://www.googleapis.com/auth/dfareporting',
                'https://www.googleapis.com/auth/dfatrafficking',
                'https://www.googleapis.com/auth/ddmconversions']

    
    scoped_credentials = credentials.with_scopes(API_SCOPES)
    scoped_credentials

    service = discovery.build(API_NAME, API_VERSION, credentials=credentials)


    # BigQuery client
    client = bigquery.Client()

    # Query the BigQuery table
    query = """
        SELECT
            RACKUI,
            lead_score,
            prediction_date
        FROM `extreme-cable-318218.cm_offline_api.lead_scores`
    """

    # Query results in a Pandas data frame
    df = client.query(query).to_dataframe()
    for row in df.iterrows():
        print(row['RACKUI'])


    # Profile ID is found in CM when the service account is added as a user profile
    profile_id = '7018732'

    conversions = []

    for index, row in df.iterrows():

        prediction_timestamp_microseconds = int(time.time() * 1000000)

        # Construct the conversion
        conversion = {
            'matchId': str(row['RACKUI']) + "5",
            'floodlightActivityId': '11728112',
            'floodlightConfigurationId': '4361691',
            'ordinal': prediction_timestamp_microseconds,
            'quantity': 1,
            'value': str(row['lead_score']),
            'timestampMicros': prediction_timestamp_microseconds,
            'customVariables': [
                {
                    'type': 'u9',
                    'value': str(row['lead_score'])
                }
            ]
        }

        conversions.append(conversion)

    # Batch insert the conversion to CM
    request_body = {
        'conversions': conversions
    }
    request = service.conversions().batchinsert(profileId=profile_id,
                                                body=request_body)
    response = request.execute()
    print(response)




