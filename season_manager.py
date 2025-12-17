import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

# --- CONFIGURATION ---
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
# Ensure credentials.json is in the same folder
CREDS = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', SCOPE)
CLIENT = gspread.authorize(CREDS)

# 1. YOUR SPREADSHEET LINK (The main Results/History sheet)
MAIN_SHEET_URL = "https://docs.google.com/spreadsheets/d/1tpxuUCl8ddpnBBr69vc4P1foRCRKWpts5-HaFPYb4po/edit?usp=sharing"

# 2. TAB NAMES (Case Sensitive!)
CONFIG_TAB_NAME = "Dates and Crap"      # Where you type the start dates
OUTPUT_TAB_NAME = "Calculated_Dates"    # Where this script saves the clean list

def get_season_config(wb):
    print(f"... Reading config from '{CONFIG_TAB_NAME}'")
    try:
        worksheet = wb.worksheet(CONFIG_TAB_NAME)
    except gspread.WorksheetNotFound:
        print(f"!!! CRITICAL ERROR: Could not find tab named '{CONFIG_TAB_NAME}'")
        return []

    # Get all data
    raw_data = worksheet.get_all_values()
    
    if not raw_data:
        print("!!! Error: The config tab is empty!")
        return []

    headers = raw_data[0]      # Row 1: Division Names
    start_dates = raw_data[1]  # Row 2: Start Dates
    week_counts = raw_data[2]  # Row 3: Total Weeks
    
    # Get Season Name (Column H / Index 7)
    season_name = start_dates[7] if len(start_dates) > 7 and start_dates[7] else "Current Season"
    print(f"... Found Season: {season_name}")
    
    configs = []
    
    # Loop through columns B to G (Index 1 to 6)
    # Covers: Div 1, Div 2, Div 3, Div 4, Div 5, All Stars
    for i in range(1, 7): 
        if i >= len(headers) or i >= len(start_dates) or i >= len(week_counts):
            continue
            
        div_name = headers[i]
        start_date_str = start_dates[i]
        total_weeks_str = week_counts[i]
        
        # Skip if header or date is missing
        if not div_name or not start_date_str: 
            continue
            
        configs.append({
            "season": season_name,
            "division": div_name,
            "start_date": start_date_str,
            "total_weeks": int(total_weeks_str)
        })
        
    return configs

def calculate_schedule(configs):
    print("... Calculating Dates")
    all_schedule_data = []

    for config in configs:
        try:
            start_date_obj = datetime.strptime(config['start_date'], "%d/%m/%Y")
        except ValueError:
            print(f"!!! Error: Date format for {config['division']} is wrong. Use dd/mm/yyyy")
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