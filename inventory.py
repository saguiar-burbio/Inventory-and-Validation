##==== Imports ======##
import pandas as pd
from auth_google_setup import *
from google_helper_functions import *
import re
from datetime import datetime, timedelta
import time

##==== Changable information ======##
credentials_path = ""

FOLDERS_TO_SKIP = [
    'History Folder',
    'BD AUTO 13 ERROR BATCH (SA)', 'BD AUTO 12 ERROR BATCH (SA)',
    'Make Searchable 100', 'Fix Batch', 'MAKE SEARCHABLE 100 TEMP NS', 'Problem docs'
]

# currently goes to SBD only
SPREADSHEET_ID = '1woR_0mq8GIR6PeCLQwA71Klp19kqQOyZ_t7RTuNkdF0'
RANGE_NAME = 'Library Inventory!F2:H'
TYPE_OF_DOCUMENT = "School Board Document"
BP_LIBRARY_ID = '1SDBaPrUcbetlgkupSUmxMVEEjjgDFDIw' #SBD Library ID

RUN_ALL = False #if true, will run entire library; if false, will only re-run last 6 months for speed


##==== Main Functions ====##
def run_all():
    total_start = time.time()
    print("\n# ============================== #\n")
    print("Authenticating Google Drive...")
    # drive_service, sheets_service = get_authenticated_services(credentials_path)
    drive_service, sheets_service = get_authenticated_services()
    print("Authentication Complete.")
    print("\n# ============================== #\n")

    print("\n# ============================== #\n")
    print(f"\n- Collecting all folders from {TYPE_OF_DOCUMENT} Library...")

    start_time = time.time()
    # Step 1: Get all folders
    folders_dict = get_folder_names(BP_LIBRARY_ID, drive_service, FOLDERS_TO_SKIP) 
    # print('Folders: ', folders_dict) 
    end_time = time.time()
    print(f"  -- Time Taken: {round(end_time - start_time, 2)} seconds")

    print(f"\n- Collecting all files from {TYPE_OF_DOCUMENT} Library...")

    start_time = time.time()
    # Step 2: Get all files at once
    files = list_all_files_recursive(BP_LIBRARY_ID, drive_service, FOLDERS_TO_SKIP) 
    #print('Files: ', files)
    end_time = time.time()
    print(f"  -- Time Taken: {round(end_time - start_time,2)/60} minutes")

    # Step 3: Merge files with folder names
    all_files = merge_files_with_folders(files, folders_dict)
    print('- Files collected. Sorting by meeting date...')
    start_time = time.time()
    # Step 4: Convert to DataFrame and sort by meeting date
    df = pd.DataFrame(all_files, columns=['File Name','Folder Name','File Link'])
    df['Meeting Date'] = pd.to_datetime(
        df['File Name'].str.extract(r'(\d{2}-\d{2}-\d{2})')[0],
        format='%m-%d-%y', errors='coerce'
    )
    df['Meeting Date'].fillna(pd.Timestamp('1970-01-01'), inplace=True)
    df.sort_values('Meeting Date', ascending=False, inplace=True)
    print('- Files sorted by Meeting Date. Adding to spreadsheet...')

    # Step 5: Update Google Sheet in one batch
    values = df[['File Name','Folder Name','File Link']].values.tolist()
    end_time = time.time()
    print(f"  -- Time Taken: {round(end_time - start_time,2)} seconds")

    sheets_service.spreadsheets().values().clear(
        spreadsheetId = SPREADSHEET_ID,
        range=RANGE_NAME
    ).execute()

    sheets_service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_NAME,
        valueInputOption='RAW',
        body={'values': values}
    ).execute()

    print(f"- {len(values)} rows updated in the sheet!")
    total_end = time.time()
    print(f"  -- Total Time Taken for Inventory: {round(total_end - total_start,2)} minutes")
    print("\n# ============================== #\n")

def quick_run():
    total_start = time.time()
    print("\n# ============================== #\n")
    print("Authenticating Google Drive...")
    drive_service, sheets_service = get_authenticated_services()
    print("Authentication Complete.")
    print("\n# ============================== #\n")

    print(f"\n# - Collecting all files from {TYPE_OF_DOCUMENT} Library...")

    # Step 0: Get today's month and last 6 months
    today = datetime.today()
    last_six_months = [(today - pd.DateOffset(months=i)).month for i in range(6)]

    print(f"- Collecting all folders from {TYPE_OF_DOCUMENT} Library...")

    # Step 1: Get all folders
    folders_dict = get_folder_names(BP_LIBRARY_ID, drive_service, FOLDERS_TO_SKIP)

    print(f"- Getting Files in last 6 months...")
    # Step 2: Filter folders by last 6 months
    recent_folders = {fid: fname for fid, fname in folders_dict.items()
                      if re.search(r'(\d{1,2})', fname) and int(re.search(r'(\d{1,2})', fname).group(1)) in last_six_months}

    # Step 3: Get older files from the sheet (folders older than 6 months)
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_NAME
    ).execute()
    old_values = result.get('values', [])
    old_df = pd.DataFrame(old_values, columns=['File Name','Folder Name','File Link']) if old_values else pd.DataFrame(columns=['File Name','Folder Name','File Link'])

    # Step 4: Bulk fetch all files under the library
    all_files = []
    page_token = None
    query = f"'{BP_LIBRARY_ID}' in parents and trashed=false"
    while True:
        response = drive_service.files().list(
            q=query,
            fields="nextPageToken, files(id, name, parents)",
            pageSize=1000,
            pageToken=page_token
        ).execute()
        all_files.extend(response.get('files', []))
        page_token = response.get('nextPageToken')
        if not page_token:
            break

    # Step 5: Keep only files in recent folders
    recent_files = [f for f in all_files if f.get('parents', [None])[0] in recent_folders]

    # Step 6: Merge recent files with folder names
    recent_df = pd.DataFrame([
        [f['name'], recent_folders[f['parents'][0]], f"https://drive.google.com/file/d/{f['id']}/view?usp=drivesdk"]
        for f in recent_files
    ], columns=['File Name','Folder Name','File Link'])

    # Step 7: Combine old + recent
    combined_df = pd.concat([old_df, recent_df], ignore_index=True)

    # Optional: sort by meeting date
    combined_df['Meeting Date'] = pd.to_datetime(
        combined_df['File Name'].str.extract(r'(\d{2}-\d{2}-\d{2})')[0],
        format='%m-%d-%y', errors='coerce'
    )
    combined_df['Meeting Date'].fillna(pd.Timestamp('1970-01-01'), inplace=True)
    combined_df.sort_values('Meeting Date', ascending=False, inplace=True)

    # Step 8: Update Google Sheet in one batch
    values = combined_df[['File Name','Folder Name','File Link']].values.tolist()
    sheets_service.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_NAME
    ).execute()
    sheets_service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_NAME,
        valueInputOption='RAW',
        body={'values': values}
    ).execute()

    print(f"- {len(values)} rows updated in the sheet!")
    total_end = time.time()
    print(f"  -- Total Time Taken for Inventory: {round(total_end - total_start,2)/60} minutes")
    print("\n# ============================== #\n")



def main():
    print("\n# ============= Starting Program ============ #\n")
    if RUN_ALL:
        run_all()
    else:
        quick_run()
    print("\n# ============= Program Complete ============ #\n")
    
## Run code ##
if __name__ == "__main__":
    main()