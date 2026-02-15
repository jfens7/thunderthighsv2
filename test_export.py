import firebase_admin
from firebase_admin import credentials, firestore
from backend.backend import ThunderData
import datetime
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_real_export_test():
    print("🚀 STARTING LIVE DATA EXPORT TEST...")

    # 1. Initialize Backend
    thunder = ThunderData() 
    db = thunder.db

    if not db:
        print("❌ FATAL: Could not connect to Firebase.")
        return

    # 2. SIMULATE A LIVE MATCH FINISHING ON THE iPAD
    # We use totally unique names to prove this isn't reading your Master Sheet.
    dummy_match = {
        "date": datetime.date.today().strftime("%d/%m/%Y"),
        "week": "Live Test",          # Shows it came from the live system
        "division": "Premier Live",   # Custom division name
        "home_team": "iPad Team 1",
        "away_team": "iPad Team 2",
        "home_players": ["iPad Player A"], # Unique Name
        "away_players": ["iPad Player B"], # Unique Name
        "match_status": "Finished",
        "table": "5",
        "set_scores": [
            {"home": 11, "away": 9}, 
            {"home": 11, "away": 9}, 
            {"home": 11, "away": 9}  # 3-0 Win
        ],
        "exported_to_sheet": False,   # This tells the backend "I am new!"
        "timestamp": firestore.SERVER_TIMESTAMP
    }

    print("📱 Simulating Match Finish on iPad...")
    new_ref = db.collection('match_results').add(dummy_match)
    doc_id = new_ref[1].id
    print(f"✅ Match Uploaded to Firebase (ID: {doc_id})")

    # 3. TRIGGER THE EXPORT ENGINE
    # The backend will wake up, see the new match in Firebase, and push it to Sheets.
    print("🔄 Backend Processing...")
    
    try:
        thunder._export_finished_matches_to_sheet()
        print("✅ Export cycle completed.")
    except Exception as e:
        print(f"❌ Export CRASHED: {e}")

    # 4. VERIFY
    print("🔎 Checking status...")
    time.sleep(2)
    updated_doc = db.collection('match_results').document(doc_id).get()
    
    if updated_doc.exists and updated_doc.to_dict().get('exported_to_sheet') == True:
        print("✅ SUCCESS: Match exported!")
        print("👉 Go check your 'Export Sheet'. You should see 'iPad Player A' vs 'iPad Player B'.")
    else:
        print("❌ FAILURE: Match was NOT marked as exported.")

if __name__ == "__main__":
    run_real_export_test()