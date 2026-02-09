import sys
import os
import datetime

# Ensure we can import the backend
sys.path.append(os.path.join(os.getcwd(), 'backend'))

try:
    from backend import ThunderData
except ImportError:
    print("❌ Could not import backend. Make sure you are in the 'thunderthighsv2' folder.")
    sys.exit(1)

def diagnose():
    print("\n🩺 DIAGNOSING BACKEND LOGIC...")
    
    # 1. Init
    db = ThunderData()
    if not db.sheet_results:
        print("❌ No Sheet Connection.")
        return

    # 2. Open the specific sheet where Jakob was found
    target_sheet_name = "Season: Summer 2026"
    print(f"\n📂 Opening '{target_sheet_name}'...")
    
    try:
        ws = db.sheet_results.worksheet(target_sheet_name)
    except:
        print(f"❌ Could not open sheet '{target_sheet_name}'")
        return

    # 3. Analyze Headers
    print("\n--- HEADER ANALYSIS ---")
    raw_headers = ws.get_all_values()[0]
    print(f"RAW HEADERS (Row 1): {raw_headers}")
    
    # Run the internal safe records logic to see how it renames headers
    records = db._get_safe_records(ws)
    if not records:
        print("❌ _get_safe_records returned 0 records!")
        return
        
    print(f"MAPPED HEADERS (Internal): {list(records[0].keys())}")
    
    # 4. Find the Row (Jakob Fensom)
    print("\n--- ROW ANALYSIS (Row 458) ---")
    # Row 458 in sheet is index 456 in list (because header is removed)
    # Let's find a row with Jakob
    target_row = None
    for i, r in enumerate(records):
        # Scan all values in the record
        if any("jakob fensom" in str(v).lower() for v in r.values()):
            target_row = r
            print(f"✅ Found Jakob in Record #{i+2} (Sheet Row {i+2})")
            break
    
    if not target_row:
        print("❌ Could not find Jakob Fensom in the processed records.")
        print("   -> This means the row was dropped inside '_get_safe_records' or earlier.")
        return

    print(f"RAW RECORD DATA: {target_row}")

    # 5. Simulate Refresh Data Logic
    print("\n--- SIMULATING LOGIC ---")
    
    # Logic Step 1: Doubles Check
    match_type = str(db._get_val(target_row, ['Type', 'Match Type'], 'Singles')).strip()
    print(f"1. Match Type: '{match_type}'")
    if "Double" in match_type or "Dbl" in match_type:
        print("   ❌ REJECTED: Doubles check failed.")
    else:
        print("   ✅ PASSED: Doubles check.")

    # Logic Step 2: Name Extraction
    p1_raw = db._get_val(target_row, ['Name 1', 'Player 1', 'Name'])
    p2_raw = db._get_val(target_row, ['Name 2', 'Player 2'])
    p1 = db._clean_name(p1_raw)
    p2 = db._clean_name(p2_raw)
    
    print(f"2. Player 1: '{p1}' (Raw: '{p1_raw}')")
    print(f"   Player 2: '{p2}' (Raw: '{p2_raw}')")
    
    if not p1 or not p2:
        print("   ❌ REJECTED: One or both names are empty.")
    else:
        print("   ✅ PASSED: Names found.")

    # Logic Step 3: Date Parsing
    date_val = db._get_val(target_row, ['Date', 'Match Date'])
    parsed_date = db._parse_date(date_val)
    print(f"3. Date: '{date_val}' -> Parsed: {parsed_date}")
    
    if not parsed_date:
        # Check lookup
        div = str(db._get_val(target_row, ['Division', 'Div'], 'Unknown')).strip()
        week_num = "Unknown" # (simplified for test)
        lookup_key = f"Summer 2026|{div}|{week_num}"
        print(f"   -> Date missing. Checking lookup key: '{lookup_key}'")
        
        # Check the logic in your code:
        # if not parsed_date: parsed_date = datetime.date(1900, 1, 1) <--- THIS IS THE KEY
        # Let's see if your CURRENT loaded file actually has this line.
        
        import inspect
        source = inspect.getsource(db.refresh_data)
        if "1900, 1, 1" in source or "2026, 1, 1" in source:
             print("   ✅ CODE CHECK: Your backend DOES have a fallback date.")
        else:
             print("   ❌ CODE CHECK: Your backend DOES NOT have a fallback date.")
             print("   -> This matches the 'Missing Name' bug theory.")

if __name__ == "__main__":
    diagnose()