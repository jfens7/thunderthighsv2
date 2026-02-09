import gspread
from google.oauth2.service_account import Credentials
import re
import os
import sys
import datetime

# --- CONFIG ---
SOURCE_SHEET_ID = "1tpxuUCl8ddpnBBr69vc4P1foRCRKWpts5-HaFPYb4po"
NEW_SHEET_NAME = f"ThunderStats_Cleaned_{datetime.date.today()}"
SHARE_EMAIL = "jakobwill7@gmail.com" 

GS_CRED_FILES = ["credentials.json", "backend/credentials.json", "../credentials.json"]

def get_cred_path(file_list):
    for f in file_list:
        if os.path.exists(f): return f
    return None

def parse_season_order(tab_name):
    """Helper to sort seasons: Returns (Year, Season_Index)"""
    # Order: Summer(1), Autumn(2), Winter(3), Spring(4)
    season_map = {"summer": 1, "autumn": 2, "winter": 3, "spring": 4}
    
    name = tab_name.lower()
    year_match = re.search(r'20\d{2}', name)
    year = int(year_match.group(0)) if year_match else 0
    
    s_idx = 0
    for s, idx in season_map.items():
        if s in name: s_idx = idx; break
        
    return (year, s_idx)

def clean_sheet():
    print("🧹 STARTING SHEET CLEANUP...")

    # 1. Connect
    gs_cred = get_cred_path(GS_CRED_FILES)
    if not gs_cred: print("❌ No Credentials found."); return
    
    try:
        scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        c = Credentials.from_service_account_file(gs_cred, scopes=scope)
        client = gspread.authorize(c)
        source_sh = client.open_by_key(SOURCE_SHEET_ID)
        print("✅ Connected to Source Sheet.")
    except Exception as e: print(f"❌ Connection Failed: {e}"); return

    # 2. Analyze Tabs
    print("🔍 Analyzing Tabs...")
    config_tabs = []
    season_tabs = []
    
    for ws in source_sh.worksheets():
        title = ws.title.strip()
        lower_title = title.lower()

        # Categorize
        if "season:" in lower_title:
            season_tabs.append(ws)
        elif "team" in lower_title:
            config_tabs.append((ws, "Teams")) # Standardize Name
        elif "alias" in lower_title:
            config_tabs.append((ws, "Aliases"))
        elif "calculated" in lower_title:
            config_tabs.append((ws, "Calculated Dates"))
        else:
            print(f"   ⚠️ Skipping Unknown/Trash Tab: '{title}'")

    # 3. Sort Seasons (Newest First)
    season_tabs.sort(key=lambda w: parse_season_order(w.title), reverse=True)

    # 4. Create NEW Sheet
    print(f"✨ Creating New Sheet: '{NEW_SHEET_NAME}'...")
    try:
        new_sh = client.create(NEW_SHEET_NAME)
        # Share immediately so you can see it
        new_sh.share(SHARE_EMAIL, perm_type='user', role='writer') 
        print(f"   🔗 URL: {new_sh.url}")
    except Exception as e: print(f"❌ Creation Failed: {e}"); return

    # 5. Copy Data
    # A. Config Tabs First
    for ws, new_name in config_tabs:
        print(f"   ➡️ Copying {new_name}...")
        data = ws.get_all_values()
        if not data: continue
        
        try:
            new_ws = new_sh.add_worksheet(title=new_name, rows=len(data)+20, cols=len(data[0])+5)
        except: 
            # If tab exists (rare), get it
            new_ws = new_sh.worksheet(new_name)
            
        new_ws.update(data)
        # Bold Header
        new_ws.format("A1:Z1", {"textFormat": {"bold": True}})

    # B. Season Tabs
    for ws in season_tabs:
        print(f"   ➡️ Copying {ws.title}...")
        data = ws.get_all_values()
        if not data: continue
        
        # Keep original name
        try:
            new_ws = new_sh.add_worksheet(title=ws.title, rows=len(data)+20, cols=len(data[0])+5)
        except:
             # If tab exists, get it
            new_ws = new_sh.worksheet(ws.title)
            
        new_ws.update(data)
        # Bold Header
        new_ws.format("A1:Z1", {"textFormat": {"bold": True}})

    # 6. Cleanup
    # Delete the default 'Sheet1' created with new file
    try:
        default_ws = new_sh.worksheet("Sheet1")
        new_sh.del_worksheet(default_ws)
    except: pass

    print("\n✅ CLEANUP COMPLETE!")
    print(f"📂 New Sheet URL: {new_sh.url}")
    print("⚠️  IMPORTANT: Copy the ID from this URL and update your 'backend/migrate_to_firebase.py' SHEET_ID!")

if __name__ == "__main__":
    clean_sheet()