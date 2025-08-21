def get_folder_names(root_folder_id, drive_service, FOLDERS_TO_SKIP):
    folders = {}
    page_token = None
    query = f"'{root_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    while True:
        response = drive_service.files().list(
            q=query,
            fields="nextPageToken, files(id, name)",
            pageSize=1000,
            pageToken=page_token
        ).execute()
        for f in response.get('files', []):
            if f['name'] not in FOLDERS_TO_SKIP:
                folders[f['id']] = f['name']
        page_token = response.get('nextPageToken')
        if not page_token:
            break
    return folders

def list_all_files(root_folder_id, drive_service):
    files = []
    page_token = None
    query = f"'{root_folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder' and trashed=false"
    while True:
        response = drive_service.files().list(
            q=query,
            fields="nextPageToken, files(id, name, parents)",
            pageSize=1000,
            pageToken=page_token
        ).execute()
        files.extend(response.get('files', []))
        page_token = response.get('nextPageToken')
        if not page_token:
            break
    return files

def merge_files_with_folders(files, folders_dict):
    all_files = []
    for f in files:
        folder_name = folders_dict.get(f['parents'][0], 'Unknown')
        file_link = f"https://drive.google.com/file/d/{f['id']}/view?usp=drivesdk"
        all_files.append([f['name'], folder_name, file_link])
    return all_files

def list_all_files_recursive(parent_id, drive_service, FOLDERS_TO_SKIP):
    all_files = []

    # Get subfolders
    page_token = None
    folder_query = f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    while True:
        response = drive_service.files().list(
            q=folder_query,
            fields="nextPageToken, files(id, name)",
            pageSize=1000,
            pageToken=page_token
        ).execute()

        for folder in response.get('files', []):
            folder_name = folder['name']
            folder_id = folder['id']
            if folder_name not in FOLDERS_TO_SKIP:
                # Recursive call to get files inside this subfolder
                all_files.extend(list_all_files_recursive(folder_id, drive_service, FOLDERS_TO_SKIP))
        page_token = response.get('nextPageToken')
        if not page_token:
            break

    # Get files directly under this folder
    file_query = f"'{parent_id}' in parents and mimeType != 'application/vnd.google-apps.folder' and trashed=false"
    page_token = None
    while True:
        response = drive_service.files().list(
            q=file_query,
            fields="nextPageToken, files(id, name, parents)",
            pageSize=1000,
            pageToken=page_token
        ).execute()
        all_files.extend(response.get('files', []))
        page_token = response.get('nextPageToken')
        if not page_token:
            break

    return all_files

