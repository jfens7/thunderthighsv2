import gspread
from google.oauth2.service_account import Credentials
import firebase_admin
from firebase_admin import credentials, firestore
import datetime
import re
import os
import sys
import time
import signal

# --- CONFIG ---
SHEET_ID = "1tpxuUCl8ddpnBBr69vc4P1foRCRKWpts5-HaFPYb4po" 
CRED_FILES = ["firebase_credentials.json", "backend/firebase_credentials.json", "../firebase_credentials.json"]
GS_CRED_FILES = ["credentials.json", "backend/credentials.json", "../credentials.json"]

ABORT_MIGRATION = False

def signal_handler(sig, frame):
    global ABORT_MIGRATION
    print("\n🛑 FORCE STOP DETECTED...")
    ABORT_MIGRATION = True

signal.signal(signal.SIGINT, signal_handler)

def get_cred_path(file_list):
    for f in file_list:
        if os.path.exists(f): return f
    return None

def parse_date(date_str):
    if not date_str: return None
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d"):
        try: return datetime.datetime.strptime(str(date_str).strip(), fmt)
        except: continue
    return None

class Migrator:
    def __init__(self):
        self.db = None
        self.sheet = None
        self.batch = None
        self.op_count = 0
        self.date_lookup = {}

    def connect(self):
        # Firebase
        fb_cred = get_cred_path(CRED_FILES)
        if not fb_cred: return False, "No Firebase Creds"
        try:
            cred = credentials.Certificate(fb_cred)
            try: firebase_admin.get_app()
            except ValueError: firebase_admin.initialize_app(cred)
            self.db = firestore.client()
        except Exception as e: return False, f"Firebase Error: {e}"

        # Sheets
        gs_cred = get_cred_path(GS_CRED_FILES)
        if not gs_cred: return False, "No Sheets Creds"
        try:
            scope = ["https://www.googleapis.com/auth/spreadsheets"]
            c = Credentials.from_service_account_file(gs_cred, scopes=scope)
            self.sheet = gspread.authorize(c).open_by_key(SHEET_ID)
        except Exception as e: return False, f"Sheets Error: {e}"
        
        self.batch = self.db.batch()
        return True, "Connected"

    def log(self, msg):
        print(f"   -> {msg}")
        sys.stdout.flush()
        try:
            self.db.collection('Admin_State').document('terminal').set({
                'last_update': datetime.datetime.now(), 'log_line': msg
            }, merge=True)
            time.sleep(0.01)
        except: pass

    def check_abort(self):
        global ABORT_MIGRATION
        if ABORT_MIGRATION: return True
        try:
            doc = self.db.collection('Admin_State').document('migration_control').get()
            if doc.exists and doc.to_dict().get('status') == 'STOP':
                self.log("🛑 REMOTE STOP RECEIVED.")
                ABORT_MIGRATION = True
                return True
        except: pass
        return False

    def commit(self, force=False):
        if self.op_count >= 300 or (force and self.op_count > 0):
            self.batch.commit()
            self.batch = self.db.batch()
            self.op_count = 0
            self.log("... Saving Batch ...")

    # --- WORKER FUNCTIONS ---

    def process_matches(self, ws, season_name):
        rows = ws.get_all_values()
        if not rows: return
        headers = [str(h).lower().strip() for h in rows[0]]
        
        idx = {}
        for col, h in enumerate(headers):
            if h in ['name 1', 'player 1', 'name']: idx['p1'] = col
            elif h in ['name 2', 'player 2']: idx['p2'] = col
            elif h in ['sets 1', 's1', 'sets']: idx['s1'] = col
            elif h in ['sets 2', 's2']: idx['s2'] = col
            elif h in ['date', 'match date']: idx['date'] = col
            elif h in ['division', 'div']: idx['div'] = col
            elif h in ['format', 'type', 'match type']: idx['type'] = col
            elif h in ['round', 'rd', 'week']: idx['week'] = col

        if 'p1' not in idx or 'p2' not in idx: return

        for i, row in enumerate(rows[1:]):
            if i % 20 == 0 and self.check_abort(): return
            if not row: continue

            # Extract
            p1 = row[idx['p1']].strip() if 'p1' in idx and len(row) > idx['p1'] else ""
            p2 = row[idx['p2']].strip() if 'p2' in idx and len(row) > idx['p2'] else ""
            if not p1 and not p2: continue # Skip empty rows

            # Doubles Skip
            match_type = "Singles"
            if 'type' in idx and len(row) > idx['type']: match_type = row[idx['type']].strip()
            elif len(row) > 6: match_type = row[6].strip()
            if "double" in match_type.lower() or "dbl" in match_type.lower(): continue 

            # Date
            date_obj = parse_date(row[idx['date']]) if 'date' in idx and len(row) > idx['date'] else None
            div = row[idx['div']].strip() if 'div' in idx and len(row) > idx['div'] else "Unknown"
            week = row[idx['week']].strip() if 'week' in idx and len(row) > idx['week'] else "Unknown"

            if not date_obj:
                week_num = re.search(r'\d+', week).group(0) if re.search(r'\d+', week) else "Unknown"
                key = f"{season_name}|{div}|{week_num}"
                date_obj = self.date_lookup.get(key)
                if not date_obj:
                    # Guess Year
                    ym = re.search(r'20\d{2}', season_name)
                    y = int(ym.group(0)) if ym else 1900
                    date_obj = datetime.datetime(y, 1, 1)

            # Scores
            try:
                s1 = int(row[idx['s1']]) if 's1' in idx and len(row) > idx['s1'] else 0
                s2 = int(row[idx['s2']]) if 's2' in idx and len(row) > idx['s2'] else 0
            except: s1, s2 = 0, 0

            # Save
            if p1 and p2:
                players = sorted([p1.lower(), p2.lower()])
                mid = f"{date_obj.strftime('%Y%m%d')}_{players[0]}_{players[1]}"
                doc_data = {
                    "season": season_name, "division": div, "date": date_obj.strftime("%d/%m/%Y"),
                    "timestamp": date_obj, "home_players": [p1], "away_players": [p2],
                    "live_home_sets": s1, "live_away_sets": s2, "source": "Paper", "match_status": "Finished"
                }
                self.batch.set(self.db.collection('Archived_Seasons').document(mid), doc_data)
                self.op_count += 1
                self.commit()

    def process_teams(self, ws):
        rows = ws.get_all_values()
        if not rows: return
        headers = [str(h).lower().strip() for h in rows[0]]
        
        # Simple flexible mapping
        idx = {h: i for i, h in enumerate(headers)}
        
        for i, row in enumerate(rows[1:]):
            if i % 20 == 0 and self.check_abort(): return
            if not row: continue

            season = row[idx['season']].strip() if 'season' in idx else "Unknown"
            div = row[idx['division']].strip() if 'division' in idx else "Unknown"
            name = row[idx['team name']].strip() if 'team name' in idx else ""
            
            if not name: continue

            # Collect players dynamically
            players = []
            for k, v in idx.items():
                if 'player' in k or 'captain' in k:
                    if len(row) > v and row[v].strip():
                        players.append(row[v].strip())

            tid = f"{season}_{div}_{name}".replace(" ", "_").replace("/", "-")
            doc_data = {
                "season": season, "division": div, "name": name, "players": players
            }
            self.batch.set(self.db.collection('Teams').document(tid), doc_data)
            self.op_count += 1
            self.commit()

    def process_aliases(self, ws):
        rows = ws.get_all_values()
        for i, row in enumerate(rows[1:]):
            if len(row) < 2: continue
            bad = row[0].strip().lower()
            good = row[1].strip()
            if bad and good:
                self.batch.set(self.db.collection('Aliases').document(bad), {"correct": good})
                self.op_count += 1
                self.commit()

    def run_full_migration(self):
        ok, msg = self.connect()
        if not ok: print(msg); return

        self.log("--- STARTING FULL SHEET MERGE ---")
        self.db.collection('Admin_State').document('migration_control').set({'status': 'RUNNING'})

        # 1. Load Dates
        try:
            ws = None
            try: ws = self.sheet.worksheet("Calculated_Dates")
            except: ws = self.sheet.worksheet("Calculated Dates")
            if ws:
                records = ws.get_all_records()
                for r in records:
                    key = f"{r.get('Season','')}|{r.get('Division','')}|{r.get('Week','')}"
                    d = parse_date(r.get('Date',''))
                    if d: self.date_lookup[key] = d
                self.log(f"✅ Loaded {len(self.date_lookup)} Date Rules")
        except: self.log("⚠️ No Calculated Dates found")

        # 2. Iterate All Tabs
        for ws in self.sheet.worksheets():
            if self.check_abort(): break
            
            title = ws.title.strip()
            self.log(f"Scanning: {title}...")

            if "Season:" in title:
                self.process_matches(ws, title.replace("Season:", "").strip())
            elif title.lower() == "teams":
                self.process_teams(ws)
            elif title.lower() == "aliases":
                self.process_aliases(ws)
            
        self.commit(force=True)
        if ABORT_MIGRATION: self.log("🛑 MIGRATION HALTED.")
        else: self.log("✅ FULL MERGE COMPLETE.")

if __name__ == "__main__":
    m = Migrator()
    m.run_full_migration()