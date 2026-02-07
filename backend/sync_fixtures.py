import gspread
from google.oauth2.service_account import Credentials
import firebase_admin
from firebase_admin import credentials, firestore
import datetime
import re

# CONFIG
SHEET_ID = "1tpxuUCl8ddpnBBr69vc4P1foRCRKWpts5-HaFPYb4po"
FIREBASE_KEY = "firebase_credentials.json"
SHEETS_KEY = "credentials.json"

def get_today_obj():
    return datetime.date.today()

def parse_sheet_date(date_val):
    if not date_val: return None
    
    if isinstance(date_val, datetime.date): return date_val
    if isinstance(date_val, datetime.datetime): return date_val.date()
    
    s = str(date_val).strip()
    
    formats = [
        "%d/%m/%Y", "%d/%m/%y", "%d-%m-%Y", 
        "%Y-%m-%d", "%d-%b-%y", "%d %b %Y"
    ]
    
    for fmt in formats:
        try:
            return datetime.datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None

def sync_schedule():
    today = get_today_obj()
    print(f"🚀 Starting Schedule Sync (Future Matches from {today.strftime('%d/%m/%Y')})...")
    
    # 1. AUTHENTICATE
    if not firebase_admin._apps:
        cred = credentials.Certificate(FIREBASE_KEY)
        firebase_admin.initialize_app(cred)
    db = firestore.client()

    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SHEETS_KEY, scopes=scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID)

    # 2. SCAN FIXTURE TABS
    matches_found = []

    all_worksheets = sheet.worksheets()
    print(f"📋 Found {len(all_worksheets)} tabs in Google Sheet.")

    for ws in all_worksheets:
        # Check if it's a schedule tab
        if "Fixtures Schedule" not in ws.title:
            continue
        
        print(f"🔎 Processing tab: '{ws.title}'...")
        
        # 1. Determine Division from Tab Name (Default)
        tab_division = "Unknown"
        if "Div 1" in ws.title: tab_division = "Division 1"
        elif "Div 2" in ws.title: tab_division = "Division 2"
        elif "Div 3" in ws.title: tab_division = "Division 3"
        elif "Prem" in ws.title: tab_division = "Premier"

        try:
            records = ws.get_all_records()
            
            # TRACKING VARIABLES (For merged cells or filling down)
            current_week = "0"

            for row in records:
                # --- A. PARSE WEEK (Round) ---
                # Try multiple column names: 'Round', 'Week', 'Wk', 'Rnd'
                raw_round = str(row.get('Round', '') or row.get('Week', '') or row.get('Wk', '') or row.get('Rnd', '')).strip()
                
                # If we found a number in the round column, update our tracker
                week_match = re.search(r'\d+', raw_round)
                if week_match:
                    current_week = week_match.group()
                
                # If current_week is still "0", skip this row (bad data)
                if current_week == "0":
                    # Maybe the row is empty or it's a header line we missed
                    pass

                # --- B. PARSE DATE ---
                row_date = parse_sheet_date(row.get('Date', '') or row.get('Match Date', ''))
                
                # --- C. PARSE DIVISION (Row override) ---
                # If the row has a "Division" column, use that. Otherwise use tab name.
                row_div = str(row.get('Division', '') or row.get('Div', '')).strip()
                final_division = row_div if row_div else tab_division

                # CHECK: Is date valid and in the future?
                if row_date and row_date >= today:
                    home_team = str(row.get('Home Team', '') or row.get('Home', '')).strip()
                    away_team = str(row.get('Away Team', '') or row.get('Away', '')).strip()
                    table = str(row.get('Table', '0')).strip()
                    
                    if not home_team or not away_team: continue

                    matches_found.append({
                        "id": f"w{current_week}_t{table}", # ID: w7_t9
                        "data": {
                            "date": row_date.strftime("%d/%m/%Y"),
                            "division": final_division,
                            "week": current_week,
                            "table": table,
                            "home_team": home_team,
                            "away_team": away_team
                        }
                    })
        except Exception as e:
            print(f"⚠️ Error reading '{ws.title}': {e}")

    # 3. UPLOAD TO FIREBASE
    if not matches_found:
        print("ℹ️ No future matches found in any tab.")
        return

    print(f"📤 Uploading {len(matches_found)} matches to 'fixture_schedule'...")
    
    batch = db.batch()
    collection_ref = db.collection("fixture_schedule")
    
    count = 0
    for m in matches_found:
        doc_ref = collection_ref.document(m["id"])
        batch.set(doc_ref, m["data"])
        count += 1
        
        if count % 400 == 0:
            batch.commit()
            batch = db.batch()
            print(f"   ...committed {count} records")
    
    batch.commit()
    print(f"✅ Schedule Sync Complete! {count} matches uploaded.")

if __name__ == "__main__":
    sync_schedule()