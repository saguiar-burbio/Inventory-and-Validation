import pandas as pd
import numpy as np
from PyPDF2 import PdfReader
import pdfplumber
import os
from fuzzywuzzy import fuzz, process
import fitz  # PyMuPDF
from PIL import Image
import pytesseract
import re
from datetime import datetime, timedelta
import time
from tqdm import tqdm
import subprocess

def is_valid_filename(filename):
    pattern = r"^(\d{6,7})_([A-Za-z0-9 .\-()&#']+)_BOE-(AGENDA-SP|AGENDA-WS|AGENDA-REG|AGENDA|SP|WS|REG|COM|EXE|FIN|PUB|PACK|FC)\d*_(\d{2}-\d{2}-\d{2,4})\.pdf$"
    return bool(re.match(pattern, filename))

# Mapping of acronyms to full descriptions
acronym_mapping = {
    "SD": "School District",
    "TWP HSD": "Township High School District",
    "UD": "Unit District",
    "CHSD": "Community High School District",
    "CCSD": "Consolidated Community School District",
    "ISD": "Independent School District",
    "USD": "Unified School District",
    "K-12": "Kindergarten to 12th Grade",
    "CUSD": "Consolidated Unified School District",
    "CISD": "Consolidated Independent School District",
    "PUB SCH": "Public Schools",
    "CO PBLC SCHS": "County Public Schools",
    "PBLC SCHS": "Public Schools",
    "SCH": "School",
    "CORP": "Corporation",
    "CO": "County",
    "COMM": "Community",
    "ELEM": "Elementary",
    "COM": "Community",
    "DIST": "District",
}

import re
from datetime import datetime

def normalize_date(date_str):
    if not date_str:
        return None
    
    # Remove weekday names
    date_str = re.sub(r'^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)[a-z]*,?\s*', '', date_str, flags=re.IGNORECASE)
    
    # Remove ordinal suffixes
    date_str = re.sub(r'(\d{1,2})(st|nd|rd|th)', r'\1', date_str, flags=re.IGNORECASE)
    
    # Remove extra text like "Minutes", "Joint", etc.
    date_str = re.sub(r'\b(Minutes|Joint|P\.M\.|A\.M\.)\b', '', date_str, flags=re.IGNORECASE)
    
    # Remove 'day of' phrases
    date_str = re.sub(r'(\d{1,2})\s+day\s+of\s+', r'\1 ', date_str, flags=re.IGNORECASE)
    
    # Replace dots with slashes for numeric dates like 6.30.25
    date_str = date_str.replace('.', '/')
    
    # Remove commas
    date_str = date_str.replace(',', '')
    
    date_formats = [
        "%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y", "%m-%d-%y",  # Numeric
        "%B %d %Y", "%b %d %Y"  # Month name
    ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    
    return None

def expand_acronyms(district_name):
    # Replace acronyms with full descriptions using regex
    pattern = r'\b(' + '|'.join(re.escape(k) for k in acronym_mapping.keys()) + r')\b'
    return re.sub(pattern, lambda m: acronym_mapping[m.group()], district_name, flags=re.IGNORECASE)

def extract_text_from_images(page):
    image = page.get_pixmap()
    img = Image.frombytes("RGB", [image.width, image.height], image.samples)
    return pytesseract.image_to_string(img) or ""

import re

def extract_dates_from_text(text):
    ## NOT IN USE
    # Regex pattern to detect various date formats, including "2nd day of May 2024"
    date_pattern = re.compile(
        r'\b('
        r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}'  # 05/12/2024 or 5-12-24
        r'|'
        r'\d{4}-\d{2}-\d{2}'  # 2024-05-12
        r'|'
        r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+\d{4}'  # May 12, 2024
        r'|'
        r'\d{1,2}(?:st|nd|rd|th)\s+day\s+of\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}'  # 2nd day of May 2024
        r')\b',
        re.IGNORECASE
    )

    return date_pattern.findall(text)


def get_drive_link_from_path(file_path, service, root_folder_id='root'):
    """
    Convert local path like /content/drive/MyDrive/Folder/Subfolder/File.pdf
    to API path and return the Google Drive shareable link.

    root_folder_id: starting folder ID (default 'root' means My Drive root)
    """
    # Remove local Colab mount prefix if present
    prefix = "/content/drive/MyDrive/"
    if file_path.startswith(prefix):
        file_path = file_path[len(prefix):]  # strip off the prefix

    # Split the path into folders and filename
    parts = file_path.strip("/").split("/")
    filename = parts[-1]
    folders = parts[:-1]

    parent_id = root_folder_id  # start here (your known folder ID)

    # Loop through each folder in the path to find its folder ID
    for folder in folders:
        query = (
            f"name='{folder}' and "
            f"mimeType='application/vnd.google-apps.folder' and "
            f"'{parent_id}' in parents and trashed=false"
        )
        response = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)'
        ).execute()

        items = response.get('files', [])
        if not items:
            return f"❌ Folder not found: {folder}"
        parent_id = items[0]['id']  # go down one level

    # Now find the file in the final folder
    query = (
        f"name='{filename}' and "
        f"'{parent_id}' in parents and trashed=false"
    )
    response = service.files().list(
        q=query,
        spaces='drive',
        fields='files(id, name)'
    ).execute()

    items = response.get('files', [])
    if not items:
        return f"❌ File not found: {filename} in {'/'.join(folders)}"

    file_id = items[0]['id']
    return f"https://drive.google.com/file/d/{file_id}/view"


import os
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def get_link_from_filepath(file_path, drive_service):
    file_name = os.path.basename(file_path)

    try:
        # Search for file by name
        query = f"name = '{file_name}' and trashed = false"
        response = drive_service.files().list(q=query, fields="files(id, name)").execute()
        files = response.get('files', [])

        if not files:
            return f"❌ File not found: {file_name}"

        file_id = files[0]['id']
        return f"https://drive.google.com/file/d/{file_id}/view"

    except HttpError as error:
        return f"❌ An error occurred: {error}"

def fuzzy_match_token(text, district_name):
  token_set_ratio = fuzz.token_set_ratio(district_name, text)

  district_tokens = set(district_name.upper().split())
  text_tokens = set(text.upper().split())
  matched_tokens = district_tokens.intersection(text_tokens)
  matched_text = " | ".join(matched_tokens) if matched_tokens else "No match"

  return token_set_ratio, matched_text

# Step 1: Recursively list all folders
def list_all_folders_in_drive(parent_folder_id, drive_service):
    folders = []
    stack = [parent_folder_id]

    while stack:
        current_folder_id = stack.pop()
        folders.append(current_folder_id)

        response = drive_service.files().list(
            q=f"'{current_folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false",
            fields="files(id, name)"
        ).execute()

        subfolders = response.get('files', [])
        for folder in subfolders:
            stack.append(folder['id'])

    return folders

# Step 2: Find all file matches and return folder names + links
def find_file_and_get_folder_info(file_name, parent_folder_id, drive_service):
    folder_ids = list_all_folders_in_drive(parent_folder_id, drive_service)
    matched_files = []

    for folder_id in folder_ids:
        response = drive_service.files().list(
            q=f"'{folder_id}' in parents and name = '{file_name}' and trashed = false",
            fields="files(id, name, parents)"
        ).execute()

        matched_files.extend(response.get('files', []))

    if not matched_files:
        return {"folders": "File not found", "links": []}

    folder_names = set()
    file_links = []

    for file in matched_files:
        file_id = file['id']
        file_links.append(f'https://drive.google.com/file/d/{file_id}/view')

        for p_id in file.get('parents', []):
            folder_info = drive_service.files().get(
                fileId=p_id,
                fields='name'
            ).execute()
            folder_names.add(folder_info['name'])

    return {
        "folders": ", ".join(folder_names),
        "links": file_links
    }

def check_boe_type(text, doc_type):
    """
    Determines the suggested BOE document type based on keywords in the text
    and checks if it matches the provided doc_type.

    Args:
        text (str): Text of the first page of the document.
        doc_type (str): Provided document type (e.g., 'BOE-SP', 'BOE-REG').

    Returns:
        - doc_type_check: "Match" or "No Match"
        - suggested_type: "" if Match, otherwise the suggested BOE type
    """
    # Normalize inputs
    text = text.upper()[:150]
    doc_type = doc_type.strip().upper()

    # Determine suggested type
    if "SPECIAL" in text:
        suggested_type = "BOE-SP"
    elif any(x in text for x in ["EXECUTIVE", "EXEC", "EX"]):
        suggested_type = "BOE-EXE"
    elif any(x in text for x in ["COMMITTEE", "SITE COUNCIL"]):
        suggested_type = "BOE-COM"
    elif "FINANCE" in text:
        suggested_type = "BOE-FIN"
    elif any(x in text for x in ["WORK SESSION", "WORKSHOP", "DISCUSSION", "WORK-STUDY", "WORK SHOP"]):
        suggested_type = "BOE-WS"
    elif any(x in text for x in ["PUBLIC HEARING", "PUBLIC"]):
        suggested_type = "BOE-PUB"
    elif any(x in text for x in ["REGULAR", "REORGANIZATION", "ORGANIZATIONAL", "SCHOOL BOARD MEETING", "COMMITTEE OF THE WHOLE"]):
        suggested_type = "BOE-REG"
    else:
        suggested_type = "No keywords found."

    # Determine match
    if doc_type == suggested_type:
        return "Match", ""
    else:
        return "No Match", suggested_type


def find_district_name(text):
    patterns = [
        r"\b(.+?)\s+Independent School District\b",
        r"\b(.+?)\s+Unified School District\b",
        r"\b(.+?)\s+Community Unit School District\b",
        r"\b(.+?)\s+Union Free School District\b",
        r"\b(.+?)\s+Central School District\b",
        r"\b(.+?)\s+City School District\b",
        r"\b(.+?)\s+Parish School Board\b",
        r"\b(.+?)\s+County School District\b",
        r"\b(.+?)\s+School Administrative Unit\b",
        r"\b(.+?)\s+Regional School District\b",
        r"\b(.+?)\s+Public Schools\b",
        r"\b(.+?)\s+City Schools\b",
        r"\b(.+?)\s+Town Schools\b",
        r"\b(.+?)\s+School Department\b",
        r"\b(.+?)\s+Board of Education\b",
        r"\b(.+?)\s+Charter Schools?\b",
        r"\b(.+?)\s+Public Charter Schools?\b",
        r"\b(.+?)\s+ISD\b",
        r"\b(.+?)\s+R-\d+\b",
        r"\b(.+?)\s+CUSD\b",
        r"\b(.+?)\s+CCSD\b",
        r"\b(.+?)\s+USD\b",
        r"\b(.+?)\s+BOE\b",
        r"\b(.+?)\s+School System\b",
        r"\b(.+?)\s+School District\b",
        r"\b(.+?)\s+Board of School Trustees\b",
        # r"\b(.+?)\s+Schools",
        # r"\b(.+?)\s+School"

    ]
    text = text.upper()
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(0).strip()
    return "No district name found"

def is_pdf_fully_readable(filepath):
    try:
        with fitz.open(filepath) as doc:
            total_pages = len(doc)
            if total_pages < 2:
                print(f"⚠️ Only {total_pages} page(s): {filepath}")

            for page_num in range(total_pages):
                try:
                    _ = doc[page_num].get_text()
                except Exception as page_err:
                    print(f"❌ Error on page {page_num + 1} of {filepath}: {page_err}")
                    return False

        return True

    except Exception as e:
        print(f"❌ Failed to open {filepath}: {e}")
        return False


def check_with_pdfinfo(filepath):
    try:
        output = subprocess.check_output(['pdfinfo', filepath], stderr=subprocess.STDOUT, timeout=10)
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ pdfinfo error for {filepath}:\n{e.output.decode()}")
        return False
    except subprocess.TimeoutExpired:
        print(f"⏱️ pdfinfo timed out for {filepath}")
        return False

def extract_boarddocs_link(text, cleaned_district_df):
    boarddocs_pattern = re.compile(r'https://go\.boarddocs\.com/[^\s"]+')
    found_links = boarddocs_pattern.findall(text)
    if found_links:
        for link in found_links:
            cleaned_district_df_link = cleaned_district_df[cleaned_district_df['BoardDoc Link'].str.contains(link, na=False)]
            if not cleaned_district_df_link.empty:
                return cleaned_district_df_link["District Name"].values[0]
    return None



def match_boarddoc_link(text, district_name, cleaned_district_df, best_match):
    """
    Attempts to match BoardDocs links found in text against a district DataFrame,
    and updates best_match dictionary accordingly.

    Args:
        text (str): Text to search for BoardDocs links.
        district_name (str): Expected district name for comparison.
        cleaned_district_df (pd.DataFrame): DataFrame containing at least 'BoardDoc Link' and 'District Name' columns.
        best_match (dict): Dictionary to be updated with match results.

    Returns:
        None. Updates best_match dict in-place.
    """
    # print("Trying to match with BoardDoc Link...")

    text_lower = text.lower()
    # Normalize common BoardDocs mistakes
    cleaned_text = text_lower.replace('boarddocs.coml', 'boarddocs.com/')

    # Patterns
    full_link_pattern = re.compile(r'https://go\.boarddocs\.com/[^\s"<>]+')
    base_link_pattern = re.compile(r'(https://go\.boarddocs\.com/[^\s"<>]+\.nsf)')

    # Find all links
    found_links = full_link_pattern.findall(cleaned_text)
    # print(f"Found Links: {found_links}")

    # Precompute 'BoardDoc Link Base' in DataFrame for efficiency
    cleaned_district_df['BoardDoc Link Base'] = cleaned_district_df['BoardDoc Link'].apply(
        lambda x: base_link_pattern.match(str(x)).group(0).strip().lower() if base_link_pattern.match(str(x)) else ''
    )

    for link in found_links:
        base_match = base_link_pattern.match(link)
        if base_match:
            link_base = base_match.group(0).strip().lower()

            # Find matching rows in the DataFrame
            match_df = cleaned_district_df[
                cleaned_district_df['BoardDoc Link Base'].str.contains(link_base, case=False, na=False)
            ]

            if not match_df.empty:
                matched_district = match_df["District Name"].values[0]

                best_match["district_name"] = matched_district

                if matched_district != district_name:
                    best_match["exact_match"] = "District Name Mismatch via BoardDoc link"
                else:
                    best_match["exact_match"] = "Found via BoardDocs"
                # Matched, exit loop early
                break
            else:
                print(f"❌ No match found for {link}")
        else:
            print(f"❌ No base match found for {link}")
    else:
        # This runs only if no break occurred (no matches found)
        best_match["exact_match"] = "Not Found"