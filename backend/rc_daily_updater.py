# backend/rc_daily_updater.py
import os
import json
import logging
import datetime
import requests
import re
from bs4 import BeautifulSoup
import gspread
from google.oauth2.service_account import Credentials
import urllib3
import firebase_admin
from firebase_admin import firestore

# Suppress SSL warnings for legacy PHP sites
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)

RESULTS_SPREADSHEET_ID = "1tpxuUCl8ddpnBBr69vc4P1foRCRKWpts5-HaFPYb4po"

def run_daily_rc_sync():
    logger.info("🌐 Starting Master Ratings Central Bulk Sync (ALL OF AUSTRALIA)...")
    
    states = ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']
    all_players_data = []
    headers = {'User-Agent': 'Mozilla/5.0'}
    
    # 1. Loop through every state to avoid timing out Ratings Central
    for state in states:
        logger.info(f"📡 Fetching players for {state}...")
        url = f"https://www.ratingscentral.com/PlayerList.php?PlayerName=&PlayerSport=Any&PlayerRegion={state}&PlayerCountry=Australia"
        
        try:
            resp = requests.get(url, headers=headers, verify=False, timeout=60)
            if resp.status_code != 200:
                logger.warning(f"⚠️ Failed to fetch {state}")
                continue
                
            soup = BeautifulSoup(resp.text, 'html.parser')
            state_count = 0
            
            for tr in soup.find_all('tr'):
                a_tag = tr.find('a', href=re.compile(r'PlayerID=(\d+)'))
                if not a_tag: continue
                
                rc_id = re.search(r'PlayerID=(\d+)', a_tag['href']).group(1)
                name = a_tag.get_text(strip=True)
                
                row_text = tr.get_text(separator='|', strip=True).replace('\xa0', ' ').replace('&plusmn;', '±')
                parts = row_text.split('|')
                
                rating = "1500"
                sd = "150"
                r_match = re.search(r'(\d{3,4})\s*(?:±|\+/-)\s*(\d{1,3})', row_text)
                if r_match:
                    rating = r_match.group(1)
                    sd = r_match.group(2)
                
                club = "Unknown"
                for p in parts:
                    if any(x in p for x in ["Table Tennis", "TTA", "Club", "Association", "Brisbane", "Gold Coast", "Townsville", "Moreton", "Sydney", "Melbourne", "Adelaide", "Perth", "Hobart"]):
                        club = p
                        break
                        
                all_players_data.append([
                    rc_id, name, rating, sd, club, state, "Australia", 
                    datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
                ])
                state_count += 1
                
            logger.info(f"✅ Found {state_count} players in {state}.")
                
        except Exception as e:
            logger.error(f"❌ Failed to scrape RC for {state}: {e}")

    if not all_players_data:
        logger.error("❌ No players found across Australia. Aborting sync.")
        return

    total_players = len(all_players_data)
    logger.info(f"🔥 Total Australian Players Scraped: {total_players}. Syncing to databases...")

    # 2. Upload directly to Master Google Sheet
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds_json = os.environ.get("GOOGLE_CREDS_JSON")
        if creds_json: 
            creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=scopes)
        else:
            paths = ["credentials.json", "backend/credentials.json"]
            found = next((p for p in paths if os.path.exists(p)), None)
            creds = Credentials.from_service_account_file(found, scopes=scopes) if found else None
            
        client = gspread.authorize(creds)
        sheet = client.open_by_key(RESULTS_SPREADSHEET_ID)
        
        try:
            ws = sheet.worksheet("RC_Directory")
        except:
            ws = sheet.add_worksheet(title="RC_Directory", rows=str(total_players + 100), cols="8")
            
        ws.clear()
        headers_list = ["RC_ID", "Name", "Rating", "SD", "Club", "State", "Country", "Last_Updated"]
        
        # Google Sheets might timeout on 10k+ rows, so we upload in chunks
        chunk_size = 2000
        ws.append_row(headers_list)
        for i in range(0, len(all_players_data), chunk_size):
            chunk = all_players_data[i:i + chunk_size]
            ws.append_rows(chunk)
            
        logger.info("✅ Successfully synced players to Google Sheets 'RC_Directory' tab!")
        
    except Exception as e:
        logger.error(f"❌ Failed to upload to Sheets: {e}")
        
    # 3. Sync to Firebase for Instant App Lookups
    try:
        if not firebase_admin._apps:
            # Initialize if not already running
            cred_path = 'firebase_credentials.json' if os.path.exists('firebase_credentials.json') else 'backend/firebase_credentials.json'
            firebase_admin.initialize_app(credentials.Certificate(cred_path))
            
        db = firestore.client()
        batch = db.batch()
        count = 0
        
        for p in all_players_data:
            doc_ref = db.collection('rc_directory').document(p[0])
            batch.set(doc_ref, {
                'rc_id': p[0], 
                'name': p[1], 
                'search_name': p[1].lower(),
                'rating': int(p[2]), 
                'sd': int(p[3]), 
                'club': p[4],
                'state': p[5],
                'last_updated': p[7]
            })
            count += 1
            if count >= 400: # Firestore batch limit is 500
                batch.commit()
                batch = db.batch()
                count = 0
                
        if count > 0: 
            batch.commit()
            
        logger.info(f"✅ Successfully synced {total_players} Australian players to Firebase. Search is now instant!")
    except Exception as e:
        logger.error(f"❌ Failed to push to Firebase: {e}")

if __name__ == "__main__":
    run_daily_rc_sync()