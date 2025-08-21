import os
import pickle
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import gspread
from google.oauth2.credentials import Credentials

# Define your required scopes
SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/spreadsheets'
]

def get_authenticated_services(credentials_path='/Users/samaguiar/Desktop/CRTL F WORKFLOW/client_secret_1009938022523-gurqd6lo7akldfc117970lbsiv0ei9ta.apps.googleusercontent.com.json', token_path='/Users/samaguiar/Desktop/CRTL F WORKFLOW/token.pkl'):
    """
    Returns:
        drive_service, sheets_service
    Usage:
        drive_service, sheets_service = get_authenticated_services()
    """
    creds = None

    # Load token if available
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)

    # Refresh or create new token if needed
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)

    # Initialize APIs
    drive_service = build('drive', 'v3', credentials=creds)
    sheets_service = build('sheets', 'v4', credentials=creds)
    return drive_service, sheets_service


def get_gspread_client(sheets_service):
    """
    Returns a gspread client authenticated using existing Google Sheets API credentials
    """
    creds = sheets_service._http.credentials  # Extract credentials from Sheets API service
    client = gspread.authorize(creds)
    return client