import gspread
from google.oauth2.service_account import Credentials

# ==========================================
# 👇 SPREADSHEET ID 👇
# ==========================================
SPREADSHEET_ID = "1tpxuUCl8ddpnBBr69vc4P1foRCRKWpts5-HaFPYb4po" 
# ==========================================

# TARGET PLAYER TO DEBUG
TARGET_PLAYER = "Lachlan Cherry"

def run_audit():
    print(f"🕵️ STARTING AUDIT FOR: {TARGET_PLAYER}")
    print("="*60)

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    try:
        creds = Credentials.from_service_account_file("backend/credentials.json", scopes=scopes)
    except FileNotFoundError:
        try:
            creds = Credentials.from_service_account_file("credentials.json", scopes=scopes)
        except:
            print("❌ Error: Could not find credentials.json")
            return

    client = gspread.authorize(creds)
    sheet = client.open_by_key(SPREADSHEET_ID)

    total_found = 0
    total_accepted = 0
    
    for worksheet in sheet.worksheets():
        if "Season:" not in worksheet.title:
            continue
            
        print(f"\n📂 Checking Tab: {worksheet.title}")
        records = worksheet.get_all_records()
        
        for i, row in enumerate(records, start=2): # Start at 2 to match Sheet Row numbers
            p1 = str(row.get('Name 1', '')).strip()
            p2 = str(row.get('Name 2', '')).strip()
            
            # Is our guy in this row?
            if TARGET_PLAYER.lower() not in p1.lower() and TARGET_PLAYER.lower() not in p2.lower():
                continue

            total_found += 1
            status = "❓ CHECKING"
            reason = ""
            
            # 1. CHECK FORMAT
            fmt = str(row.get('Format', row.get('Match Format', ''))).lower().strip()
            if "doubles" in fmt:
                print(f"   Row {i}: ❌ SKIPPED (Format is Doubles)")
                continue

            # 2. CHECK SCORES
            s1_raw = row.get('Sets 1')
            s2_raw = row.get('Sets 2')
            
            try:
                if str(s1_raw).strip() == "" or str(s2_raw).strip() == "":
                    print(f"   Row {i}: ❌ SKIPPED (Empty Scores) -> Sets 1: '{s1_raw}', Sets 2: '{s2_raw}'")
                    continue
                
                # Try to convert
                s1 = int(s1_raw)
                s2 = int(s2_raw)
                
                # 3. CHECK FILL-IN STATUS
                ps1 = str(row.get('PS 1', '')).upper()
                ps2 = str(row.get('PS 2', '')).upper()
                
                is_p1 = (TARGET_PLAYER.lower() in p1.lower())
                my_status = ps1 if is_p1 else ps2
                
                type_label = "Fill-in" if my_status == "S" else "Regular"
                
                print(f"   Row {i}: ✅ ACCEPTED as {type_label} | Score: {s1}-{s2} | vs {p2 if is_p1 else p1}")
                total_accepted += 1
                
            except ValueError:
                print(f"   Row {i}: ❌ CRASHED (Bad Score Format) -> Sets 1: '{s1_raw}', Sets 2: '{s2_raw}'")

    print("="*60)
    print(f"AUDIT COMPLETE.")
    print(f"Found {total_found} rows involving {TARGET_PLAYER}.")
    print(f"Successfully loaded {total_accepted} matches.")
    print("="*60)

if __name__ == "__main__":
    run_audit()