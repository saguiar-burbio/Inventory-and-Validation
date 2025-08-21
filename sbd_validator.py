##==== Imports ======##
import os
import re
import pickle
from datetime import datetime, timedelta
from multiprocessing.pool import ThreadPool
from multiprocessing import cpu_count
import pandas as pd
from tqdm import tqdm
from PIL import Image
import fitz  # PyMuPDF
import pytesseract
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from gspread_dataframe import set_with_dataframe


from auth_google_setup import *
from validator_functions import *
from google_helper_functions import *

##==== Changeable info ======##
credentials_path = '/Users/samaguiar/Desktop/CRTL F WORKFLOW/client_secret_1009938022523-gurqd6lo7akldfc117970lbsiv0ei9ta.apps.googleusercontent.com.json'

spreadsheet_name = "SBD Name and Date Validation v2"
tab_name = "CTRL F BD AUTO TEMP BATCH 2"
folder_path = "/content/drive/MyDrive/Board Packet Library/CTRL F BD AUTO TEMP BATCH 2"

district_acronyms = {
    "AACPS": "ANNE ARUNDEL COUNTY PUBLIC SCHOOLS",
    "FNSBSD": "FAIRBANKS NORTH STAR BOROUGH SCHOOL DISTRICT",
    "FNSB": "FAIRBANKS NORTH STAR BOROUGH SCHOOL DISTRICT",
    # Add more as needed
}

date_pattern = re.compile(
    r"""
    \b(
        # Numeric dates: 8/12/25 or 08-12-2025 or 6.30.25
        \d{1,2}[/-\.]\d{1,2}[/-\.]\d{2,4} |
        
        # Full ISO style: 2025-07-16
        \d{4}-\d{2}-\d{2} |
        
        # Month name with optional day suffix: July 16th, 2025 or Aug 06 2025
        (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4} |
        
        # Weekday prefix: Wednesday, July 16th, 2025
        (?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[a-z]*,?\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2}(?:st|nd|rd|th)?,?\s+\d{4} |
        
        # Day ordinal phrasing: 7th day of July 2025
        \d{1,2}(?:st|nd|rd|th)?\s+day\s+of\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}
    )\b
    """,
    re.IGNORECASE | re.VERBOSE
)

##==== Authentication ======##
drive_service, sheets_service = get_authenticated_services(credentials_path)

##==== Google Sheets Setup ======##
gc = get_gspread_client(sheets_service)

ss = gc.open(spreadsheet_name)

# Main sheet
try:
    sheet = ss.worksheet(tab_name)
except gspread.exceptions.WorksheetNotFound:
    sheet = ss.add_worksheet(title=tab_name, rows=1000, cols=20)

# Problem sheet
tab_name_problems = tab_name + " Problems"
try:
    sheet_problems = ss.worksheet(tab_name_problems)
except gspread.exceptions.WorksheetNotFound:
    sheet_problems = ss.add_worksheet(title=tab_name_problems, rows=1000, cols=20)

# Load district list for matching
district_list_sheet = gc.open("v2 Board Doc Dataset").worksheet("District List")
district_list = district_list_sheet.get_all_values()
district_df = pd.DataFrame(district_list[1:], columns=district_list[0])
cleaned_district_df = district_df[['NCES ID','District Name','State','Other Board Doc Link (not hosted on District Website)']]
cleaned_district_df.rename(columns={'Other Board Doc Link (not hosted on District Website)':'BoardDoc Link'}, inplace=True)

##==== Columns ======##
base_columns = [
    "File Name", "District Name", "File Size (MB)", "Suggested District Name",
    "Exact Match?", "Core Name", "Partial Match", "Expanded Name",
    "Expanded Match?", "Percent Confidence: Exact", "Percent Confidence: Expanded Name",
    "Matched Text", "Found Date: Page #", "Found Date", "Document Type Found",
    "Document Type: Keywords"
]

##==== File Processing Function ======##
def process_file(file):
    file_path = os.path.join(folder_path, file)
    problem_dict = None
    row_dict = None

    if not os.path.isfile(file_path) or not file.lower().endswith('.pdf'):
        problem_dict = {"File Name": file, "Link": get_link_from_filepath(file_path, drive_service), "Issue": "Not a PDF"}
        return None, problem_dict

    if not is_valid_filename(file):
        problem_dict = {"File Name": file, "Link": get_link_from_filepath(file_path, drive_service), "Issue": "Invalid filename"}
        return None, problem_dict

    try:
        file_size_mb = round(os.path.getsize(file_path) / (1024 * 1024), 2)
        if file_size_mb < 1: file_size_mb = 0.01
        if os.path.getsize(file_path) == 0:
            problem_dict = {"File Name": file, "Link": get_link_from_filepath(file_path, drive_service), "Issue": "Zero-byte file"}
            return None, problem_dict

        # Extract info
        district_name = file.split('_')[1]
        core_name = re.sub(r'\b(SCHOOL|DISTRICT|NO\.?|SD|TWP HSD|UD|COMMUNITY|CUSD|USD|COUNTY|PUBLIC|SCHOOLS|CITY|TOWNSHIP|ELEMENTARY|#\d+)\b', '', district_name, flags=re.IGNORECASE).strip().upper()
        core_name = re.sub(r'\b[IVXLCDM]+\b|\b\d+\b', '', core_name)  # Remove Roman/Arabic numerals
        core_name = core_name.replace('-', ' ').replace('/', ' ')

        expanded_name = expand_acronyms(district_name).upper()
        type_doc = file.split('_')[-2]
        date_file = file.split('_')[-1].split('.')[0]

        try:
            formatted_date = datetime.strptime(date_file, "%m-%d-%y").strftime("%Y-%m-%d")
            formatted_date_obj = datetime.strptime(formatted_date, "%Y-%m-%d")
        except ValueError:
            formatted_date_obj = None
            formatted_date = "Invalid date"

        if formatted_date_obj and formatted_date_obj > datetime.today() + timedelta(days=30):
            problem_dict = {"File Name": file, "Link": get_link_from_filepath(file_path, drive_service), "Issue": "Date over 30 days in future"}
            return None, problem_dict

        best_match = {
            "file": file,
            "district_name": district_name,
            "suggested_district_name": "Not Found",
            "exact_match": "Not Found",
            "core_name": core_name,
            "partial_match": "Not Found",
            "expanded_name": expanded_name,
            "expanded_match": "Not Found",
            "token_set_ratio": 0,
            "token_set_ratio_expand": 0,
            "page_num": "N/A",
            "matched_text": "No match",
            "found_date_page": "N/A",
            "found_date": "No date found",
            "doc_type_check": "Not Found",
            "doc_type_keywords": "N/A"
        }

        with fitz.open(file_path) as doc:
            for page_num, page in enumerate(doc, start=1):
                if page_num > 3:
                    break
                text = page.get_text().upper().replace("\n", " ").replace("  ", " ")

                # Check for BoardDoc link
                if page_num == 1:
                    match_boarddoc_link(text, district_name, cleaned_district_df, best_match)

                # Fuzzy matching
                token_set_ratio, matched_text = fuzzy_match_token(text, district_name)
                token_set_ratio_expand, matched_text_expand = fuzzy_match_token(text, expanded_name)

                match_1 = bool(re.search(r"\b" + re.escape(district_name.upper()) + r"\b", text))
                match_2 = bool(re.search(r"\b" + re.escape(core_name) + r"\b", text))
                match_3 = bool(re.search(r"\b" + re.escape(expanded_name) + r"\b", text))

                if token_set_ratio > best_match['token_set_ratio']:
                    best_match.update({
                        "exact_match": "Found!" if match_1 else "Not Found",
                        "partial_match": "Found!" if match_2 else "Not Found",
                        "expanded_match": "Found!" if match_3 else "Not Found",
                        "token_set_ratio": token_set_ratio,
                        "token_set_ratio_expand": token_set_ratio_expand,
                        "page_num": page_num,
                        "matched_text": matched_text
                    })

                # Check dates in document
                found_dates = date_pattern.findall(text)
                if found_dates:
                    normalized_dates = [normalize_date(d) for d in found_dates if normalize_date(d)]
                    matched_dates = " | ".join(normalized_dates) if normalized_dates else "No date found"
                    if formatted_date in matched_dates:
                        best_match["found_date"] = "Date Match!"
                        best_match["found_date_page"] = page_num

                # Acronym check
                for acronym, full_name in district_acronyms.items():
                    if re.search(r"\b" + re.escape(acronym) + r"\b", text):
                        best_match.update({
                            "district_name": full_name,
                            "exact_match": f"Found via Acronym ({acronym})",
                            "page_num": page_num
                        })
                        break

                # Document type
                doc_type_check, doc_type_keywords = check_boe_type(text, type_doc)
                best_match.update({"doc_type_check": doc_type_check, "doc_type_keywords": doc_type_keywords})

                # Possible district name
                if page_num == 1:
                    possible_district_name = find_district_name(text)
                    best_match["suggested_district_name"] = possible_district_name

                # OCR fallback if first page fails
                if page_num == 1 and best_match["exact_match"] == "Not Found":
                    try:
                        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
                        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                        gray = img.convert("L")
                        ocr_text = pytesseract.image_to_string(gray, config="--psm 6").upper()
                        ocr_token_set_ratio, matched_text_ocr = fuzzy_match_token(ocr_text, district_name)
                        ocr_match_1 = bool(re.search(r"\b" + re.escape(district_name.upper()) + r"\b", ocr_text))
                        ocr_match_2 = bool(re.search(r"\b" + re.escape(core_name) + r"\b", ocr_text))

                        if ocr_token_set_ratio > best_match['token_set_ratio']:
                            best_match.update({
                                "exact_match": "OCR Match" if ocr_match_1 else "Not Found",
                                "partial_match": "OCR Match" if ocr_match_2 else "Not Found",
                                "token_set_ratio": ocr_token_set_ratio,
                                "page_num": page_num,
                                "matched_text": matched_text_ocr
                            })

                        best_match["suggested_district_name"] = find_district_name(ocr_text)
                    except Exception:
                        pass

        row_dict = {
            "File Name": best_match["file"],
            "District Name": best_match["district_name"],
            "File Size (MB)": file_size_mb,
            "Suggested District Name": best_match["suggested_district_name"],
            "Exact Match?": best_match["exact_match"],
            "Core Name": best_match["core_name"],
            "Partial Match": best_match["partial_match"],
            "Expanded Name": best_match["expanded_name"],
            "Expanded Match?": best_match["expanded_match"],
            "Percent Confidence: Exact": best_match["token_set_ratio"],
            "Percent Confidence: Expanded Name": best_match["token_set_ratio_expand"],
            "Matched Text": best_match["matched_text"],
            "Found Date: Page #": best_match["found_date_page"],
            "Found Date": best_match["found_date"],
            "Document Type Found": best_match["doc_type_check"],
            "Document Type: Keywords": best_match["doc_type_keywords"]
        }

        return row_dict, None

    except Exception as e:
        problem_dict = {"File Name": file, "Link": get_link_from_filepath(file_path, drive_service), "Issue": str(e)}
        return None, problem_dict

##==== Main Execution ======##
if not os.path.exists(folder_path):
    print("Invalid folder path")
else:
    start_time = time.time()
    files = os.listdir(folder_path)
    validated_list = []
    problem_list = []

    pool = ThreadPool(cpu_count())
    results = list(tqdm(pool.imap(process_file, files), total=len(files)))
    pool.close()
    pool.join()

    for r in results:
        if r[0]:
            validated_list.append(r[0])
        if r[1]:
            problem_list.append(r[1])

    validated_docs = pd.DataFrame(validated_list, columns=base_columns)
    problem_documents = pd.DataFrame(problem_list)

    # Update sheets
    if not problem_documents.empty:
        set_with_dataframe(sheet_problems, problem_documents, include_index=False, include_column_header=True, resize=True)
    if not validated_docs.empty:
        set_with_dataframe(sheet, validated_docs, include_index=False, include_column_header=True, resize=True)

    end_time = time.time()
    print(f"Processing complete ✅\n Total Time: {round(end_time - start_time,2)/60} minutes")
   


