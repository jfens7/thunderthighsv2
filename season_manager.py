import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import os
import sys

# --- CONFIGURATION ---
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# ==========================================
# 1. ROBUST CREDENTIALS FINDER
# ==========================================
# This checks all the places your app usually hides the key file
possible_paths = [
    "credentials.json", 
    "backend/credentials.json", 
    "gold-coast-table-tennis-firebase-adminsdk.json",
    "backend/gold-coast-table-tennis-firebase-adminsdk.json"
]

creds_file = None
for path in possible_paths:
    if os.path.exists(path):
        creds_file = path
        break

if not creds_file:
    print("\n!!! CRITICAL ERROR: Could not find your Google Credentials file.")
    print(f"I looked in these locations: {possible_paths}")
    print(">> ACTION REQUIRED: Please find your .json key file and drag it into this folder.\n")
    sys.exit(1)

print(f"... Using credentials found at: {creds_file}")
CREDS = ServiceAccountCredentials.from_json_keyfile_name(creds_file, SCOPE)
CLIENT = gspread.authorize(CREDS)

# ==========================================
# 2. SPREADSHEET SETUP
# ==========================================
MAIN_SHEET_URL = "https://docs.google.com/spreadsheets/d/1tpxuUCl8ddpnBBr69vc4P1foRCRKWpts5-HaFPYb4po/edit?usp=sharing"
CONFIG_TAB_NAME = "Dates and Crap"
OUTPUT_TAB_NAME = "Calculated_Dates"

def get_season_config(wb):
    print(f"... Reading config from '{CONFIG_TAB_NAME}'")
    try:
        worksheet = wb.worksheet(CONFIG_TAB_NAME)
    except gspread.WorksheetNotFound:
        print(f"!!! CRITICAL ERROR: Could not find tab named '{CONFIG_TAB_NAME}'")
        return []

    raw_data = worksheet.get_all_values()
    if not raw_data:
        print("!!! Error: The config tab is empty!")
        return []

    headers = raw_data[0] # Row 1: Division Names
    configs = []

    # --- THE FIX: Loop through ALL rows (Spring, Winter, etc.) ---
    # We step by 2 because each season takes 2 rows (Dates + Weeks)
    for row_idx in range(1, len(raw_data), 2):
        if row_idx + 1 >= len(raw_data):
            break 

        start_dates = raw_data[row_idx]
        week_counts = raw_data[row_idx + 1]

        # Get Season Name from Column H (Index 7)
        # If blank, we make up a name like "Season_Row_5"
        season_name = start_dates[7] if len(start_dates) > 7 and start_dates[7] else f"Season_Row_{row_idx}"
        print(f"... Found Config for: {season_name}")

        for i in range(1, 7): # Columns B to G
            if i >= len(headers) or i >= len(start_dates) or i >= len(week_counts):
                continue
                
            div_name = headers[i]
            start_date_str = start_dates[i]
            total_weeks_str = week_counts[i]
            
            # Skip empty cells
            if not div_name or not start_date_str or not total_weeks_str: 
                continue
            
            try:
                weeks_int = int(total_weeks_str)
            except ValueError:
                continue

            configs.append({
                "season": season_name,
                "division": div_name,
                "start_date": start_date_str,
                "total_weeks": weeks_int
            })
            
    return configs

def calculate_schedule(configs):
    print("... Calculating Dates")
    all_schedule_data = []

    for config in configs:
        try:
            start_date_obj = datetime.strptime(config['start_date'], "%d/%m/%Y")
        except ValueError:
            print(f"!!! Error: Date format for {config['division']} ({config['season']}) is wrong. Use dd/mm/yyyy")
            continue

        for week_num in range(1, config['total_weeks'] + 1):
            days_to_add = (week_num - 1) * 7
            current_week_date = start_date_obj + timedelta(days=days_to_add)
            
            all_schedule_data.append({
                "Season": config['season'],
                "Division": config['division'],
                "Week": week_num,
                "Day": current_week_date.strftime("%A"),
                "Date": current_week_date.strftime("%d/%m/%Y"),
                "Date_Sort": current_week_date.strftime("%Y-%m-%d") 
            })
            
    return pd.DataFrame(all_schedule_data)

def upload_to_sheet(wb, df):
    print(f"... Uploading {len(df)} rows to '{OUTPUT_TAB_NAME}'")
    try:
        try:
            worksheet = wb.worksheet(OUTPUT_TAB_NAME)
            worksheet.clear() 
        except gspread.WorksheetNotFound:
            print(f"... Creating new tab: {OUTPUT_TAB_NAME}")
            worksheet = wb.add_worksheet(title=OUTPUT_TAB_NAME, rows="1000", cols="20")
            
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())
        print("SUCCESS: Dates updated!")
    except Exception as e:
        print(f"!!! Error uploading: {e}")

if __name__ == "__main__":
    try:
        print(f"Connecting to Spreadsheet...")
        wb = CLIENT.open_by_url(MAIN_SHEET_URL)
        
        configs = get_season_config(wb)
        if configs:
            schedule_df = calculate_schedule(configs)
            if not schedule_df.empty:
                upload_to_sheet(wb, schedule_df)
            else:
                print("!!! No dates were calculated.")
            
    except Exception as e:
        print(f"!!! ERROR: {e}")