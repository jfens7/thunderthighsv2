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

def print_hex(s):
    """Prints string with hidden characters revealed."""
    return f"'{s}' (Bytes: {[b for b in s.encode('utf-8')]})"

def debug_search():
    print("\n🔍 STARTING ROSTER DEBUGGER...")
    print("--------------------------------")
    
    # 1. Initialize Backend
    print("1. Connecting to Database...")
    try:
        db = ThunderData()
        # Force a refresh to load all data
        db.refresh_data()
        print("   ✅ Connected & Refreshed.")
    except Exception as e:
        print(f"   ❌ Connection Failed: {e}")
        return

    # 2. Get User Input
    target_name = input("\n👤 Enter the exact Player Name you are looking for: ").strip()
    if not target_name:
        print("❌ No name entered.")
        return

    print(f"\n🔎 Hunting for: {print_hex(target_name)}")
    print("--------------------------------")

    found_in_memory = False
    found_in_sheets = False
    found_in_firebase = False

    # 3. Check Backend Memory (The Final List)
    print("\n[CHECK 1] Backend Memory (What the website sees):")
    all_players = list(db.get_all_players().keys())
    
    # Exact Match
    if target_name in all_players:
        print(f"   ✅ FOUND EXACT MATCH: '{target_name}' is in the active roster.")
        found_in_memory = True
    else:
        print(f"   ❌ NOT FOUND exactly.")
    
    # Fuzzy Match
    partial_matches = [p for p in all_players if target_name.lower() in p.lower()]
    if partial_matches:
        print(f"   ⚠️  However, I found these similar names in memory:")
        for p in partial_matches:
            print(f"       - {print_hex(p)}")
    else:
        print("       (No similar names found in memory)")


    # 4. Check Raw Google Sheets
    print("\n[CHECK 2] Raw Google Sheets Data:")
    if db.sheet_results:
        try:
            found_rows = []
            for ws in db.sheet_results.worksheets():
                title = ws.title
                # Skip non-data sheets
                if title in ["Calculated_Dates", "Aliases", "ratings Origin", "ratings updated"]: continue
                
                rows = ws.get_all_values()
                for i, row in enumerate(rows):
                    row_str = str(row).lower()
                    if target_name.lower() in row_str:
                        found_rows.append(f"Sheet '{title}', Row {i+1}: {row}")
            
            if found_rows:
                found_in_sheets = True
                print(f"   ✅ FOUND in Google Sheets {len(found_rows)} times:")
                for loc in found_rows[:5]: # Show first 5
                    print(f"       -> {loc}")
                if len(found_rows) > 5: print(f"       ... and {len(found_rows)-5} more.")
            else:
                print("   ❌ NOT FOUND in any Google Sheet row.")
        except Exception as e:
            print(f"   ⚠️ Could not scan sheets: {e}")
    else:
        print("   ⚠️ Skipped (No Sheets Connection)")


    # 5. Check Firebase
    print("\n[CHECK 3] Firebase Live Data:")
    if db.db:
        try:
            docs = db.db.collection('Live_match_results').stream()
            fb_count = 0
            for doc in docs:
                d = doc.to_dict()
                h_players = d.get('home_players', [])
                a_players = d.get('away_players', [])
                
                # Check arrays
                if any(target_name.lower() in str(p).lower() for p in h_players + a_players):
                    fb_count += 1
            
            if fb_count > 0:
                found_in_firebase = True
                print(f"   ✅ FOUND in {fb_count} Firebase match records.")
            else:
                print("   ❌ NOT FOUND in Firebase.")
        except Exception as e:
            print(f"   ⚠️ Could not scan Firebase: {e}")
    else:
        print("   ⚠️ Skipped (No Firebase Connection)")

    # 6. DIAGNOSIS
    print("\n--------------------------------")
    print("🩺 DIAGNOSIS:")
    
    if found_in_memory:
        print("✅ GOOD NEWS: The name IS in the system.")
        print("   -> If the search bar isn't finding it, it's a FRONTEND (website code) issue.")
        print("   -> Check 'scripts.html' logic.")
        
    elif found_in_sheets or found_in_firebase:
        print("⚠️  BAD NEWS: The name exists in data, but is NOT in the active roster.")
        print("   -> POSSIBLE CAUSE 1: They haven't played a valid match yet.")
        print("      (The system only adds players who have recorded a match result).")
        print("   -> POSSIBLE CAUSE 2: The match date is invalid or in the future.")
        print("   -> POSSIBLE CAUSE 3: The name is misspelled in the Sheet (check 'Aliases').")
        
    else:
        print("❌ BAD NEWS: The name doesn't exist anywhere.")
        print("   -> You need to add them to a match in Google Sheets or play a live match.")

if __name__ == "__main__":
    debug_search()
