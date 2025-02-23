import os
import time
import io
import requests
import pdfplumber
import yaml

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Load config file
with open("config.yml", "r") as ymlfile:
    cfg = yaml.safe_load(ymlfile)

SPREADSHEET_ID = cfg["spreadsheet_id"]
SHEET_NAME = cfg["sheet_name"]
TABLE_RANGE = f"{SHEET_NAME}!A1"

def get_credentials():
    creds = None

    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    return creds

def get_sheet_service(creds):
    try:
        service = build("sheets", "v4", credentials=creds)
        return service.spreadsheets()
    except HttpError as err:
        print(f"❌ Google Sheets API Error: {err}")
        exit(1)

def download_pdf(url):
    response = requests.get(url)
    if response.status_code == 200:
        return io.BytesIO(response.content)
    else:
        raise Exception(f"Failed to download PDF: {response.status_code}")

def extract_tables_from_pdf(pdf_stream):
    data = []
    with pdfplumber.open(pdf_stream) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    data.append(row)
    return data

def upload_to_google_sheets(data, sheet):
    body = {
        'value_input_option': 'USER_ENTERED',
        'data': [
            {
                'majorDimension': 'ROWS',
                'range': TABLE_RANGE,
                'values': data,
            }
        ]
    }
    response = sheet.values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()
    print("✅ Data successfully uploaded!")

def main():
    pdf_url = cfg["pdf_url"]
    creds = get_credentials()
    sheet = get_sheet_service(creds)
    
    try:
        pdf_stream = download_pdf(pdf_url)
        table_data = extract_tables_from_pdf(pdf_stream)
        
        if table_data:
            upload_to_google_sheets(table_data, sheet)
        else:
            print("⚠️ No tables found in the PDF.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
