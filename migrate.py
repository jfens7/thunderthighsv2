import firebase_admin
from firebase_admin import credentials, firestore
import gspread
import datetime

# --- CONFIG ---
SHEET_NAME = "master-sheet"
CREDENTIALS_FILE = "firebase_key123.json"

# --- CONNECT ---
# 1. Connect to Sheets
try:
    gc = gspread.service_account(filename=CREDENTIALS_FILE)
    sh = gc.open(SHEET_NAME)
except:
    print("⚠️ Could not connect to Google Sheets.")

# 2. Connect to Firebase (Singleton check to avoid errors if imported)
if not firebase_admin._apps:
    cred = credentials.Certificate(CREDENTIALS_FILE)
    firebase_admin.initialize_app(cred)
db = firestore.client()

def sync_all_data(season_tab_name):
    """
    Main function called by the Admin Panel button.
    """
    print(f"🔄 STARTING SYNC for {season_tab_name}...")
    stats = {
        "users": migrate_users(),
        "profiles": migrate_profiles(),
        "matches": migrate_matches(season_tab_name)
    }
    print("✨ SYNC COMPLETE.")
    return stats

def migrate_users():
    print("   -> Syncing Users...")
    try:
        ws = sh.worksheet("Users")
        rows = ws.get_all_records()
        batch = db.batch()
        count = 0
        
        for row in rows:
            email = str(row.get("Email", "")).strip()
            if not email: continue
            
            doc_ref = db.collection("users").document(email)
            user_data = {
                "name": row.get("Name"),
                "tier": row.get("Tier", "FREE"),
                "linked_player": row.get("Linked_Player", ""),
                "hand": row.get("Hand", "Right"),
                "bio": row.get("Bio", ""),
                "photo_url": row.get("Photo_URL", ""),
                "is_admin": str(row.get("Is_Admin", "")).upper() == "TRUE"
            }
            batch.set(doc_ref, user_data, merge=True)
            count += 1
            
        batch.commit()
        return count
    except Exception as e:
        print(f"❌ User Sync Error: {e}")
        return 0

def migrate_profiles():
    print("   -> Syncing Profiles...")
    try:
        ws = sh.worksheet("Profiles")
        rows = ws.get_all_records()
        batch = db.batch()
        count = 0
        
        for row in rows:
            name = str(row.get("Name", "")).strip()
            if not name: continue
            
            doc_ref = db.collection("profiles").document(name)
            batch.set(doc_ref, row, merge=True)
            count += 1
            
        batch.commit()
        return count
    except Exception as e:
        print(f"❌ Profile Sync Error: {e}")
        return 0

def migrate_matches(season_tab_name):
    print(f"   -> Syncing Matches from '{season_tab_name}'...")
    try:
        ws = sh.worksheet(season_tab_name)
        rows = ws.get_all_records()
        count = 0
        
        # Note: For large datasets (>500), we process in chunks.
        # Here we do simple individual writes for safety/clarity.
        collection = db.collection("matches")
        
        for row in rows:
            p1 = str(row.get("Name 1", "")).strip()
            p2 = str(row.get("Name 2", "")).strip()
            if not p1 or not p2: continue
            
            # UNIQUE MATCH ID: Crucial for avoiding duplicates
            # Format: "Spring2025_W1_Jakob_vs_Foad"
            week = row.get('Week', 0)
            clean_season = season_tab_name.replace(" ", "").replace(":", "")
            match_id = f"{clean_season}_W{week}_{p1}_vs_{p2}".replace(" ", "_")
            
            match_data = {
                "source": "SHEETS",  # Tagging origin so we don't overwrite iPad games later
                "season": season_tab_name,
                "week": int(week) if week else 0,
                "date": str(row.get("Date", "")),
                "division": str(row.get("Division", "")),
                "p1_name": p1,
                "p2_name": p2,
                "p1_score": int(row.get("Sets 1", 0)),
                "p2_score": int(row.get("Sets 2", 0)),
                "is_doubles": 'double' in str(row.get("Format", "")).lower(),
                # Normalize 'S' status to boolean
                "p1_is_fillin": str(row.get("PS 1", "")).upper() == 'S',
                "p2_is_fillin": str(row.get("PS 2", "")).upper() == 'S'
            }
            
            collection.document(match_id).set(match_data, merge=True)
            count += 1

        return count
    except Exception as e:
        print(f"❌ Match Sync Error: {e}")
        return 0

if __name__ == "__main__":
    # Manual Test Run
    sync_all_data("Season: Spring 2025")