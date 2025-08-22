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
import io
from googleapiclient.http import MediaIoBaseDownload
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time
from multiprocessing import cpu_count
from concurrent.futures import ProcessPoolExecutor


from auth_google_setup import *
from validator_functions import *
from google_helper_functions import *

##==== Changeable info ======##
credentials_path = '/Users/samaguiar/Desktop/CRTL F WORKFLOW/client_secret_1009938022523-gurqd6lo7akldfc117970lbsiv0ei9ta.apps.googleusercontent.com.json'

spreadsheet_name = "SBD Name and Date Validation v2"
tab_name = "September 2025 A Batch 1.2 redo"
FOLDER_ID = '1kGrB2VrHMMCKatcf2IKdtJqgjmIb3dsC'

district_acronyms = {
    "AACPS": "ANNE ARUNDEL COUNTY PUBLIC SCHOOLS",
    "FNSBSD": "FAIRBANKS NORTH STAR BOROUGH SCHOOL DISTRICT",
    "FNSB": "FAIRBANKS NORTH STAR BOROUGH SCHOOL DISTRICT",
    # Add more as needed
}

date_pattern = re.compile(
    r"""
    \b(
        # 1. Weekday + Month + Day + Year: Tuesday, July 8, 2025
        (?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[a-z]*,?\s+
        (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+
        \d{1,2}(?:st|nd|rd|th)?,?\s+\d{2,4} |

        # 2. Month Day, Year: July 16th, 2025 or Aug 06 2025
        (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+
        \d{1,2}(?:st|nd|rd|th)?,?\s+\d{2,4} |

        # 3. Ordinal phrasing: the 14th day of July 2025
        (?:the\s+)?\d{1,2}(?:st|nd|rd|th)?\s+day\s+of\s+
        (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{2,4} |

        # 4. ISO numeric: 2025-07-16
        \d{4}-\d{2}-\d{2} |

        # 5. Other numeric: 06-10-25, 6/24/2025, 6.30.25
        \d{1,2}[-/\.]\d{1,2}[-/\.]\d{2,4}
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
    """
    Process a single Google Drive file object.
    file: dict with keys id, name, size, mimeType, webViewLink (from list_all_files)
    """

    file_id = file["id"]
    file_name = file["name"]
    file_size_bytes = int(file.get("size", 0))
    file_size_mb = round(file_size_bytes / (1024 * 1024), 2) if file_size_bytes else 0
    print('Processing File: ', {file_name})

    problem_dict = None
    row_dict = None

    # Check for PDFs
    if file["mimeType"] != "application/pdf" or not file_name.lower().endswith(".pdf"):
        problem_dict = {"File Name": file_name, "Link": file["webViewLink"], "Issue": "Not a PDF"}
        print('\n - Not a PDF. Storing as Problem File.')
        return None, problem_dict

    # Check valid filename
    if not is_valid_filename(file_name):
        problem_dict = {"File Name": file_name, "Link": file["webViewLink"], "Issue": "Invalid filename"}
        print('\n - Incorrect Filename. Storing as Problem File.')
        return None, problem_dict

    # Zero-byte check
    if file_size_bytes == 0:
        problem_dict = {"File Name": file_name, "Link": file["webViewLink"], "Issue": "Zero-byte file"}
        print('\n - Zero Byte File. Storing as Problem File.')
        return None, problem_dict
    if file_size_mb < 1:
        file_size_mb = 0.01

    # --- Extract info from filename ---
    try:
        district_name = file_name.split("_")[1]
        core_name = re.sub(
            r'\b(SCHOOL|DISTRICT|NO\.?|SD|TWP HSD|UD|COMMUNITY|CUSD|USD|COUNTY|PUBLIC|SCHOOLS|CITY|TOWNSHIP|ELEMENTARY|#\d+)\b',
            '', district_name, flags=re.IGNORECASE
        ).strip().upper()
        core_name = re.sub(r'\b[IVXLCDM]+\b|\b\d+\b', '', core_name)
        core_name = core_name.replace('-', ' ').replace('/', ' ')

        expanded_name = expand_acronyms(district_name).upper()
        type_doc = file_name.split("_")[-2]
        date_file = file_name.split("_")[-1].split(".")[0]
        print(f'Validating the following: \n - District Name: {district_name} \n - Type: {type_doc} \n - File Date: {date_file}')

        try:
            formatted_date = datetime.strptime(date_file, "%m-%d-%y").strftime("%Y-%m-%d")
            formatted_date_obj = datetime.strptime(formatted_date, "%Y-%m-%d")
        except ValueError:
            formatted_date_obj = None
            formatted_date = "Invalid date"

        if formatted_date_obj and formatted_date_obj > datetime.today() + timedelta(days=30):
            problem_dict = {"File Name": file_name, "Link": file["webViewLink"], "Issue": "Date over 30 days in future"}
            return None, problem_dict

        best_match = {
            "file": file_name,
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

        # --- Download PDF into memory ---
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)

        # --- Process PDF with fitz ---
        with fitz.open(stream=fh, filetype="pdf") as doc:
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
        print('File complete.')
        return row_dict, None

    except Exception as e:
        problem_dict = {"File Name": file_name, "Link": file["webViewLink"], "Issue": str(e)}
        return None, problem_dict

##==== Main Execution ======##
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count, freeze_support

def main():
    if not FOLDER_ID:
        print("Invalid folder id")
        return

    print('Starting Validation...')
    start_time = time.time()
    files = list_all_files_validator(FOLDER_ID, drive_service)
    validated_list = []
    problem_list = []

    with ProcessPoolExecutor(max_workers=cpu_count()) as executor:
        futures = {executor.submit(process_file, f): f for f in files}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing Files"):
            try:
                row_dict, problem_dict = future.result()
                if row_dict:
                    validated_list.append(row_dict)
                if problem_dict:
                    problem_list.append(problem_dict)
            except Exception as e:
                problem_list.append({"File Name": futures[future], "Link": None, "Issue": str(e)})

    validated_docs = pd.DataFrame(validated_list, columns=base_columns)
    problem_documents = pd.DataFrame(problem_list)

    print('Pasting data on spreadsheet...')
    # Update sheets
    if not problem_documents.empty:
        set_with_dataframe(sheet_problems, problem_documents, include_index=False, include_column_header=True, resize=True)
    if not validated_docs.empty:
        set_with_dataframe(sheet, validated_docs, include_index=False, include_column_header=True, resize=True)

    end_time = time.time()
    print(f"Processing complete ✅\n Total Time: {round(end_time - start_time,2)/60} minutes")


if __name__ == '__main__':
    freeze_support()  # needed for Windows / macOS
    main()

   


