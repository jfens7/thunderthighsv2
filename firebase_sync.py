import firebase_admin
from firebase_admin import credentials, firestore
import gspread
from datetime import datetime
import os
import json

class FirebaseSyncer:
    def __init__(self):
        # 1. Connect to Firebase
        # We use a service account or implicit auth if available
        # For this setup, we assume the environment is already configured or we use the local key
        try:
            # Check if app is already initialized to avoid double-init errors
            try:
                self.app = firebase_admin.get_app()
            except ValueError:
                cred = credentials.Certificate('firebase_credentials.json') # Ensure this file exists
                self.app = firebase_admin.initialize_app(cred)
            
            self.db = firestore.client()
            print("🔥 Syncer: Firebase Connected.")
        except Exception as e:
            print(f"⚠️ Syncer: Firebase connection failed. {e}")
            self.db = None

        # 2. Connect to Google Sheets
        try:
            self.gc = gspread.service_account(filename='credentials.json')
            self.sh = self.gc.open("TT Results 2026") # CHANGE THIS TO YOUR EXACT SHEET NAME
            print("📄 Syncer: Google Sheets Connected.")
        except Exception as e:
            print(f"⚠️ Syncer: GSheets connection failed. {e}")
            self.gc = None

    def sync_all(self):
        if not self.db or not self.gc:
            return {"success": False, "error": "Database connection missing"}

        try:
            # 1. Get 'Verified' matches from Firebase
            # These are matches the Admin has clicked "Approve" on.
            matches_ref = self.db.collection('fixtures')
            query = matches_ref.where('match_status', '==', 'Verified')
            docs = query.stream()

            moved_count = 0
            
            # 2. Open the target sheet
            # We assume a sheet named 'Season Results' or similar exists
            # You might want to make this dynamic based on the match date or division
            worksheet = self.sh.worksheet("Season Results") 

            for doc in docs:
                m = doc.to_dict()
                
                # PREPARE THE ROW DATA
                # We format this to match your paper sheet columns exactly
                # Adjust these columns to match your actual Google Sheet layout!
                
                # Format: Date | Division | Home Player | Away Player | Score | Game History (The "Rich Data")
                
                # Handle Names (Flatten arrays if needed)
                home_p = m.get('current_home_players', [m.get('home_team')])[0]
                away_p = m.get('current_away_players', [m.get('away_team')])[0]
                
                # Score Logic
                h_sets = m.get('live_home_sets', 0)
                a_sets = m.get('live_away_sets', 0)
                final_score = f"{h_sets}-{a_sets}"
                
                # The "Rich Data" - specific point scores (e.g. "11-9, 5-11")
                # We put this in a specific column so the backend can find it
                game_history = m.get('game_scores_history', '')

                row = [
                    m.get('date'),          # Col A: Date
                    m.get('division'),      # Col B: Division
                    home_p,                 # Col C: Player 1
                    away_p,                 # Col D: Player 2
                    final_score,            # Col E: Result
                    game_history            # Col F: Details (THE KEY FIELD)
                ]

                # 3. Append to Sheet
                worksheet.append_row(row)
                
                # 4. Archive in Firebase so we don't sync it twice
                # We change status from 'Verified' to 'Archived'
                doc.reference.update({'match_status': 'Archived'})
                
                moved_count += 1

            return {"success": True, "count": moved_count}

        except Exception as e:
            return {"success": False, "error": str(e)}

if __name__ == "__main__":
    syncer = FirebaseSyncer()
    res = syncer.sync_all()
    print(res)