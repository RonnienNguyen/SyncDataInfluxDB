import os
from influxdb_client import InfluxDBClient
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# InfluxDB settings
INFLUXDB_HOST = "your_influxdb_host"
INFLUXDB_TOKEN = "your_influxdb_token"
INFLUXDB_ORG = "your_influxdb_org"
INFLUXDB_BUCKET = "your_influxdb_bucket"
INFLUXDB_QUERY = "your_influxdb_query"

# Google Drive settings
# Get Folder Driver to this
GDRIVE_CREDENTIALS_FILE = 'path_to_your_credentials_file.json'
GDRIVE_FOLDER_ID = 'your_folder_id'

def query_influxdb():
    client = InfluxDBClient(url=INFLUXDB_HOST, token=INFLUXDB_TOKEN)
    query_api = client.query_api()
    tables = query_api.query(INFLUXDB_QUERY, org=INFLUXDB_ORG, bucket=INFLUXDB_BUCKET)
    return tables

def upload_to_google_drive(file_path):
    creds = service_account.Credentials.from_service_account_file(GDRIVE_CREDENTIALS_FILE, ['https://www.googleapis.com/auth/drive'])
    drive_service = build('drive', 'v3', credentials=creds)

    file_metadata = {
        'name': os.path.basename(file_path),
        'parents': [GDRIVE_FOLDER_ID]
    }
    media = MediaFileUpload(file_path)

    file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()

    return file.get('id')

if __name__ == '__main__':
    result = query_influxdb()
    
    for table in result:
        for record in table.records:
            # Process and save data as a file (e.g., CSV) from InfluxDB
            # Replace with your data processing logic

            # Upload the file to Google Drive
            uploaded_file_id = upload_to_google_drive(file_path)
            print(f'Uploaded file with ID: {uploaded_file_id}')
