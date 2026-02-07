import gspread
from google.oauth2.service_account import Credentials
import firebase_admin
from firebase_admin import credentials, firestore
import datetime
import re
import json
import os

# --- CONFIG ---
SHEET_ID = "1tpxuUCl8ddpnBBr69vc4P1foRCRKWpts5-HaFPYb4po" 
CRED_FILE = "firebase_credentials.json" 

def parse_date(date_str):
    if not date_str: return datetime.datetime(1900, 1, 1)
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try: return datetime.datetime.strptime(str(date_str).strip(), fmt)
        except: continue
    return datetime.datetime(1900, 1, 1)

def migrate():
    # 1. Connect to Firebase
    try:
        cred = credentials.Certificate(CRED_FILE)
        firebase_admin.initialize_app(cred)
        db = firestore.client()
        print("🔥 Firebase Connected.")
    except Exception as e:
        print(f"❌ Firebase Auth Failed: {e}")
        return

    # 2. Connect to GSheets
    try:
        gc_creds = "credentials.json" 
        if not os.path.exists(gc_creds):
            print("❌ 'credentials.json' not found.")
            return
        
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_file(gc_creds, scopes=scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SHEET_ID)
        print("📄 Google Sheets Connected.")
    except Exception as e:
        print(f"❌ GSheets Auth Failed: {e}")
        return

    # 3. Iterate Worksheets
    batch = db.batch()
    count = 0
    total_matches = 0

    print("🚀 Starting Migration to 'Paper_match_results' (Failsafe Storage)...")

    for ws in sheet.worksheets():
        if "Season:" not in ws.title: continue
        
        season_name = ws.title.replace("Season:", "").strip()
        print(f"   Processing {season_name}...")
        
        rows = ws.get_all_records()
        
        for row in rows:
            # Basic Validation
            p1 = str(row.get('Name 1') or row.get('Player 1') or '').strip()
            p2 = str(row.get('Name 2') or row.get('Player 2') or '').strip()
            if not p1 or not p2: continue
            
            fmt = str(row.get('Format') or '').lower()
            if 'doubles' in fmt: continue

            # Scores
            try:
                s1 = int(row.get('Sets 1') or row.get('S1') or 0)
                s2 = int(row.get('Sets 2') or row.get('S2') or 0)
            except: continue

            # Date
            date_obj = parse_date(row.get('Date') or row.get('Match Date'))
            
            # Unique ID
            players = sorted([p1.lower(), p2.lower()])
            match_id = f"{date_obj.strftime('%Y%m%d')}_{players[0]}_{players[1]}"
            
            doc_data = {
                "source": "Paper", # Explicitly mark source
                "season": season_name,
                "date": date_obj.strftime("%d/%m/%Y"),
                "timestamp": date_obj,
                "division": str(row.get('Division') or row.get('Div') or 'Unknown'),
                "home_players": [p1],
                "away_players": [p2],
                "live_home_sets": s1,
                "live_away_sets": s2,
                "game_scores_history": "", # Paper matches have no history
                "match_status": "Finished"
            }

            # WRITE TO SEPARATE COLLECTION
            ref = db.collection('Paper_match_results').document(match_id)
            batch.set(ref, doc_data)
            
            count += 1
            total_matches += 1
            
            if count >= 400:
                batch.commit()
                batch = db.batch()
                print(f"   Saved {count} matches...")
                count = 0

    if count > 0:
        batch.commit()
    
    print(f"✅ Migration Complete! {total_matches} matches stored in 'Paper_match_results'.")

if __name__ == "__main__":
    migrate()