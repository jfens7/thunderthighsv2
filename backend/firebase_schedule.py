import gspread
from google.oauth2.service_account import Credentials
import firebase_admin
from firebase_admin import credentials, firestore
import datetime
import os
import json

# --- CONFIG ---
SHEET_ID = "1tpxuUCl8ddpnBBr69vc4P1foRCRKWpts5-HaFPYb4po"
FIREBASE_KEY = "firebase_credentials.json"
SHEETS_KEY = "credentials.json"

def get_today_string():
    # Returns date in dd/mm/yyyy format to match your sheets
    return datetime.date.today().strftime("%d/%m/%Y")

def sync_schedule():
    print(f"📅 Starting Sync for: {get_today_string()}")

    # 1. Firebase Auth
    if not firebase_admin._apps:
        cred = credentials.Certificate(FIREBASE_KEY)
        firebase_admin.initialize_app(cred)
    db = firestore.client()

    # 2. Google Sheets Auth
    scope = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SHEETS_KEY, scopes=scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(SHEET_ID)

    today_str = get_today_string()
    all_fixtures = []

    # 3. Read through all "Season:" tabs
    for ws in sheet.worksheets():
        if "Season:" not in ws.title:
            continue
        
        print(f"📋 Checking {ws.title}...")
        records = ws.get_all_records()
        
        for row in records:
            # Check if row date matches today
            row_date = str(row.get('Date', '')).strip()
            if row_date == today_str:
                
                # Extract details based on your screenshot columns
                fixture = {
                    "date": today_str,
                    "table": str(row.get('Table', '0')),
                    "division": str(row.get('Division', 'Unknown')),
                    "home_team": str(row.get('Team 1', 'TBA')),
                    "away_team": str(row.get('Team 2', 'TBA')),
                    "match_status": "Scheduled",
                    "timestamp": firestore.SERVER_TIMESTAMP
                }

                # Helper to build player list if columns exist (Name 1, Name 2 etc)
                # This ensures the iPad knows who is playing
                p1 = str(row.get('Player 1', '')).strip()
                p2 = str(row.get('Player 2', '')).strip()
                fixture["home_players"] = [p1] if p1 else []
                fixture["away_players"] = [p2] if p2 else []

                all_fixtures.append(fixture)

    # 4. Push to Firebase
    if not all_fixtures:
        print("ℹ️ No matches found in Google Sheets for today.")
        return

    print(f"🚀 Found {len(all_fixtures)} matches. Uploading to 'fixtures' collection...")
    
    batch = db.batch()
    for fix in all_fixtures:
        # Create a unique ID so we don't double-up if we run the script twice
        doc_id = f"{fix['date'].replace('/','')}_{fix['table']}_{fix['division']}"
        doc_ref = db.collection("fixtures").document(doc_id)
        batch.set(doc_ref, fix)
    
    batch.commit()
    print("✅ Firebase Schedule Updated. iPad app should now see today's matches.")

if __name__ == "__main__":
    sync_schedule()