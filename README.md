# Inventory-and-Validation

## Installing on Local Computer
1. Run git clone https://github.com/saguiar-burbio/Schedule-Reports-Creating.git in your terminal to clone repo.
   
### Set up Virtual Envirnoment
#### MAC
2. Create a virtual environment: `python3 -m venv env`

3. Activate virtual environment: `source env/bin/activate`

4. Install requirements: `pip install -r requirements.txt`

#### Windows
2. Create a virtual environment: `py -m venv env`

3. Activate virtual environment: `.\env\Scripts\activate`

4. Install requirements: `py -m pip install -r requirements.txt`

## Getting Google JSON File for Authentication
1. Go to the Google Cloud Console: https://console.cloud.google.com/
2. Create a new project (or select an existing one).
3. Enable APIs for your project:
   - Go to APIs & Services → Library.
   - Enable Google Drive API and Google Sheets API.
4. Create OAuth credentials:
   - Go to APIs & Services → Credentials → Create Credentials → OAuth Client ID.
   - Application type: Desktop App.
   - Give it a name and click Create.
   - Download the JSON file (this is your client_secret_XXXX.json file).
5. Save the JSON file locally:
   - Store it in the auth folder 
  
## Program Details
Note: These are currently only for SBD inventory and Validations.

### Inventory
The library inventory will updated the inventory in columns F:H for SBD. 

Please change the following variables to fit your needs:

`credentials_path `: your path to your JSON creditials to authenticate google

`FOLDERS_TO_SKIP `: folder to skip as needed. There are some already prepopulated. 

`SPREADSHEET_ID`: spreadsheet id where the library inventory lives. Currently populated for SBD.
`RANGE_NAME`: the tab and range where you want your data pasted. Currently populated for SBD.
`TYPE_OF_DOCUMENT`: type of document.
`BP_LIBRARY_ID`: folder id of the library inventory. Currently populated for SBD.

`RUN_ALL`: toggle to run entire library or only last 6 months. True = entire library, False = only last 6 months.

Current time for All library is about 4.5 minute(s). 
Current time for Last 6 months is about 1 minute(s).

If there are any issues or needed improvements, please contact saguiar@burbio.com or via messages.

### Validator
Note: this is currently only for SBD. 

The validator validates the following:
- District Name
- Date
- Type
- File Name Convention 
- Corrupt Files

Please change the following variables to fit your needs:

`credentials_path `: your path to your JSON creditials to authenticate google

`spreadsheet_name`: name going to the spreadsheet. 
`tab_name`: tab name where you want the data pasted.
`folder_path`: to the batch folder (i.e: /content/drive/MyDrive/Board Packet Library/CTRL F BD AUTO TEMP BATCH 2)

`district_acronyms`: specific acronyms that schools use in their minutes/documents

If there are any issues or needed improvements, please contact saguiar@burbio.com or via messages.

## Running Programs
### Inventory
If using VSCode, please click on `inventory.py` in the left hand panel. Change the variables above and click on the play button in the upper right hand corner. 

If running in the terminal, use the following code:


## Next Steps
**Inventory**
[] Rewrite the formulas into the program (will this cause issues with api?)

**Validator**
[] Similiarity Checker 
[] Add HASH Checker for Team Documents
[] Nonsearchable flag
[] Corrupt Checker (double check that this is working?)

**Validator and Inventory**
[] Strategic Plans
[] Spending Documents
[] Budget Documents
[] Bond Documents

