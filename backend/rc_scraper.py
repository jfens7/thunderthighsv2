# backend/rc_scraper.py
import requests
import re
import datetime
import logging
from bs4 import BeautifulSoup
import urllib.parse
import urllib3

# Suppress SSL warnings for legacy PHP sites like RC
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger(__name__)

class RatingsCentralScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }

    def _fetch_profile_data(self, rc_id, fallback_name=None):
        """Helper to explicitly fetch perfectly clean profile data directly from PlayerInfo.php"""
        info_url = f"https://www.ratingscentral.com/PlayerInfo.php?PlayerID={rc_id}"
        try:
            resp = requests.get(info_url, headers=self.headers, verify=False, timeout=5)
            if resp.status_code != 200: return None
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # 1. NAME EXTRACTION (Sniper method to strip ", Player Info")
            name_text = fallback_name or "Unknown Player"
            title_tag = soup.find('title')
            if title_tag:
                clean_name = title_tag.get_text().replace(', Player Info', '').replace('- Ratings Central', '').strip()
                if clean_name: name_text = clean_name

            # 2. RATING EXTRACTION (Bulletproof Regex ignoring weird spaces)
            rating_val = "N/A"
            raw_text = soup.get_text(separator=' ').replace('\xa0', ' ').replace('&plusmn;', '±')
            r_match = re.search(r'(\d{3,4})\s*(?:±|\+/-)\s*(\d{1,3})', raw_text)
            if r_match:
                rating_val = f"{r_match.group(1)}±{r_match.group(2)}"

            # 3. CLUB & LOCATION EXTRACTION (Pipe Split Method)
            player_club = "Unknown Club"
            location = "Unknown"
            
            for row in soup.find_all('tr'):
                row_text = row.get_text(separator='|', strip=True)
                parts = row_text.split('|')
                if len(parts) >= 2:
                    label = parts[0].lower()
                    if 'primary club' in label:
                        player_club = parts[1]
                    elif 'province' in label or 'state' in label:
                        location = parts[1]
                    elif 'country' in label and location == "Unknown":
                        location = parts[1]

            return {
                "id": rc_id,
                "name": name_text,
                "rating": rating_val,
                "location": location,
                "club": player_club,
                "recent_opponents": "Verified via RC Profile"
            }
        except Exception as e:
            logger.error(f"Error fetching profile info for {rc_id}: {e}")
            return None

    def search_by_name(self, player_name, target_club=None):
        if not player_name: return []
        
        name_parts = [p for p in player_name.strip().split() if len(p) > 1]
        queries_to_try = []
        
        # 1. Try "Lastname, Firstname" (RC's preferred format)
        if len(name_parts) > 1:
            queries_to_try.append(f"{name_parts[-1]}, {' '.join(name_parts[:-1])}")
        
        # 2. Try the exact string typed
        queries_to_try.append(player_name.strip())
        
        # 3. Fallback: Search just Lastname (Handles Firstname typos)
        if len(name_parts) > 1:
            queries_to_try.append(name_parts[-1])
            # 4. Fallback: Search just Firstname
            queries_to_try.append(name_parts[0])

        results = []
        seen_ids = set()

        for query in queries_to_try:
            try:
                # Use standard RC query params
                safe_name = urllib.parse.quote_plus(query)
                url = f"https://www.ratingscentral.com/PlayerList.php?PlayerName={safe_name}"
                resp = requests.get(url, headers=self.headers, verify=False, timeout=10)
                
                if resp.status_code != 200: continue
                soup = BeautifulSoup(resp.text, 'html.parser')

                # =================================================================
                # SCENARIO A: EXACT MATCH PROFILE
                # We do NOT trust the URL. We check the HTML for "Ratings Central ID"
                # =================================================================
                rc_id = None
                rc_id_label = soup.find(string=re.compile("Ratings Central ID", re.IGNORECASE))
                
                if rc_id_label:
                    row = rc_id_label.find_parent('tr')
                    if row:
                        cells = row.find_all(['td', 'th'])
                        if len(cells) >= 2:
                            rc_id = cells[-1].get_text(strip=True)

                if rc_id:
                    if rc_id not in seen_ids:
                        seen_ids.add(rc_id)
                        # Fetch clean data and break the loop immediately
                        prof = self._fetch_profile_data(rc_id, fallback_name=query.title())
                        if prof: results.append(prof)
                    if len(results) > 0: break
                    continue

                # =================================================================
                # SCENARIO B: A LIST OF MULTIPLE PLAYERS
                # =================================================================
                links = soup.find_all('a', href=re.compile(r'Player(?:Info|Main|)\.php\?PlayerID=(\d+)'))
                
                for a in links:
                    rc_id = re.search(r'PlayerID=(\d+)', a['href'], re.I).group(1)
                    if rc_id in seen_ids: continue
                    seen_ids.add(rc_id)
                    
                    # Fetch perfectly clean data for this ID directly from their profile
                    prof = self._fetch_profile_data(rc_id)
                    if prof: results.append(prof)
                    
                    # Stop after 5 results so we don't freeze the frontend
                    if len(results) >= 5: break
                
                if len(results) > 0: break
                    
            except Exception as e:
                logger.error(f"RC Live Search Error on query '{query}': {e}")
                
        return results

    def deep_scrape_profile(self, rc_id):
        """The God-Tier Scraper: Leverages the pure CSV output trick for perfect stability!"""
        if not rc_id or not str(rc_id).isdigit(): return None
        
        stats = {
            "rc_id": rc_id,
            "rc_rating": 1500.0,
            "rc_sd": 150.0,
            "last_synced": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
            "events": [],
            "recent_matches": [],
            "total_wins": 0,
            "total_losses": 0
        }

        try:
            # 1. Grab Core Stats
            info_url = f"https://www.ratingscentral.com/PlayerInfo.php?PlayerID={rc_id}"
            resp_info = requests.get(info_url, headers=self.headers, verify=False, timeout=10)
            if resp_info.status_code == 200:
                text = BeautifulSoup(resp_info.text, 'html.parser').get_text(separator=' ').replace('\xa0', ' ')
                match = re.search(r'(\d{3,4})\s*(?:±|\+/-|&plusmn;)\s*(\d{1,3})', text, re.IGNORECASE)
                if match:
                    stats["rc_rating"] = float(match.group(1))
                    stats["rc_sd"] = float(match.group(2))

            # 2. Grab Match History using the CSV Output Trick
            csv_url = f"https://www.ratingscentral.com/MatchList.php?PlayerID={rc_id}&CSV_Output=Text"
            resp_csv = requests.get(csv_url, headers=self.headers, verify=False, timeout=10)
            
            if resp_csv.status_code == 200:
                lines = resp_csv.text.splitlines()
                wins = 0
                losses = 0
                
                for line in lines[1:]: # Skip header
                    parts = line.split(',')
                    if len(parts) >= 6:
                        date_str = parts[0].strip().replace('"', '')
                        event_name = parts[1].strip().replace('"', '')
                        opp_name = parts[2].strip().replace('"', '')
                        result = parts[3].strip().replace('"', '').upper()
                        score = parts[4].strip().replace('"', '')
                        
                        if result in ['W', 'L']:
                            if result == 'W': wins += 1
                            if result == 'L': losses += 1
                            
                            if len(stats["recent_matches"]) < 20:
                                stats["recent_matches"].append({
                                    "opponent": opp_name,
                                    "result": "Win" if result == 'W' else "Loss",
                                    "score": score
                                })
                                
                            if len(stats["events"]) < 10:
                                event_exists = False
                                for e in stats["events"]:
                                    if e["name"] == event_name:
                                        event_exists = True
                                        break
                                if not event_exists:
                                    stats["events"].append({
                                        "date": date_str,
                                        "name": event_name,
                                        "change": "See Graph"
                                    })
                                    
                stats["total_wins"] = wins
                stats["total_losses"] = losses
                
            return stats
        except Exception as e:
            logger.error(f"Deep Scraper failed for ID {rc_id}: {e}")
            return None