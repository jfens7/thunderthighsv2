import gspread
from google.oauth2.service_account import Credentials
import datetime
import re
import os
import json
import sys
import logging
import random
import string

# --- SETUP LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ensure backend folder is in path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path: sys.path.append(current_dir)

# Import Sky Engine (Optional)
try:
    from sky_engine import SkyEngine
except ImportError:
    try: from backend.sky_engine import SkyEngine
    except: SkyEngine = None

# Import Rating Logic
try:
    from ratings_logic import calculate_match, DEFAULT_RATING, DEFAULT_RD, DEFAULT_VOL
except ImportError:
    logger.error("⚠️ ratings_logic.py not found! Using dummy logic.")
    DEFAULT_RATING, DEFAULT_RD, DEFAULT_VOL = 1500.0, 350.0, 0.06
    def calculate_match(w, l, s1, s2): return {'winner': w, 'loser': l}

RESULTS_SPREADSHEET_ID = "1tpxuUCl8ddpnBBr69vc4P1foRCRKWpts5-HaFPYb4po" 
RATING_START_DATE = datetime.date(2025, 12, 25)

class RatingEngine:
    def __init__(self):
        self.players = {} 

    def get_rating(self, name):
        if name not in self.players:
            self.players[name] = {'rating': DEFAULT_RATING, 'rd': DEFAULT_RD, 'vol': DEFAULT_VOL}
        return self.players[name]

    def set_seed(self, name, rating, rd=None, vol=None):
        try:
            r_val = float(rating)
            if rd and str(rd).strip(): rd_val = float(rd)
            else: rd_val = DEFAULT_RD
            if vol and str(vol).strip(): vol_val = float(vol)
            else: vol_val = DEFAULT_VOL

            if vol_val <= 0.0001: vol_val = DEFAULT_VOL
            if rd_val < 0: rd_val = DEFAULT_RD

            self.players[name] = {'rating': r_val, 'rd': rd_val, 'vol': vol_val}
        except ValueError:
            pass

    def update_match(self, p1_name, p2_name, s1, s2):
        if s1 == s2: return 
        
        p1_stats = self.get_rating(p1_name)
        p2_stats = self.get_rating(p2_name)

        if s1 > s2:
             res = calculate_match(p1_stats, p2_stats, s1, s2)
             self.players[p1_name] = res['winner']
             self.players[p2_name] = res['loser']
        else:
             res = calculate_match(p2_stats, p1_stats, s2, s1)
             self.players[p2_name] = res['winner']
             self.players[p1_name] = res['loser']

class ThunderData:
    def __init__(self):
        self.scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        self.client = None
        self.sheet_results = None
        self.sky_engine = SkyEngine() if SkyEngine else None
        
        self.rating_engine = RatingEngine()
        self.all_players = {}
        self.season_stats = {}
        self.seasons_list = ["Career"] 
        self.divisions_list = set()
        self.date_lookup = {} 
        self.weekly_matches = {} 
        self.player_ids = {} 
        self.alias_map = {} 
        
        self._authenticate()
        if self.sheet_results: self.refresh_data()

    def _authenticate(self):
        try:
            creds_json = os.environ.get("GOOGLE_CREDS_JSON")
            if creds_json:
                info = json.loads(creds_json)
                self.creds = Credentials.from_service_account_info(info, scopes=self.scopes)
            else:
                paths = ["credentials.json", "backend/credentials.json", "gold-coast-table-tennis-firebase-adminsdk.json"]
                found = next((p for p in paths if os.path.exists(p)), None)
                if found: self.creds = Credentials.from_service_account_file(found, scopes=self.scopes)
                else: self.creds = None
            
            if self.creds:
                self.client = gspread.authorize(self.creds)
                self.sheet_results = self.client.open_by_key(RESULTS_SPREADSHEET_ID)
                logger.info("✅ Connected to Google Sheets")
        except Exception as e:
            logger.error(f"❌ Auth Error: {e}")

    def get_sky_data(self, lat=None, lon=None):
        if self.sky_engine: return self.sky_engine.get_environment_data(lat, lon)
        return {"is_day": False, "temp": 20, "condition": "Clear", "holiday": "normal"}

    def _clean_name(self, name): 
        if not name: return ""
        clean = " ".join(str(name).split())
        if clean.lower() in self.alias_map:
            return self.alias_map[clean.lower()]
        return clean.title()

    # --- UPDATED FUZZY MATCHER (FIXES HIDDEN SPACES) ---
    def _get_val(self, row, keys, default=''):
        # 1. Fast exact match
        for k in keys:
            if k in row: return row[k]
        
        # 2. Fuzzy match (ignore case and spaces)
        # Create a map: { "name1": "Name 1 ", "sets1": "Sets  1" }
        row_keys_norm = {k.strip().lower(): k for k in row.keys()}
        
        for k in keys:
            norm_k = k.strip().lower()
            if norm_k in row_keys_norm:
                return row[row_keys_norm[norm_k]]
                
        return default

    def _parse_date(self, date_str):
        if not date_str: return None
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d"):
            try: return datetime.datetime.strptime(str(date_str).strip(), fmt).date()
            except ValueError: continue
        return None

    def _generate_player_id(self):
        chars = string.ascii_uppercase + string.digits
        return ''.join(random.choice(chars) for _ in range(8))

    def _load_calculated_dates(self):
        self.date_lookup = {}
        try:
            ws = self.sheet_results.worksheet("Calculated_Dates")
            for row in ws.get_all_records():
                s = str(row.get('Season','')).strip()
                d = str(row.get('Division','')).strip()
                w = str(row.get('Week','')).strip()
                date_val = str(row.get('Date',''))
                parsed = self._parse_date(date_val)
                if parsed: self.date_lookup[f"{s}|{d}|{w}"] = parsed
        except: pass

    def _load_aliases(self):
        self.alias_map = {}
        try:
            try: ws = self.sheet_results.worksheet("Aliases")
            except: 
                ws = self.sheet_results.add_worksheet(title="Aliases", rows=1000, cols=2)
                ws.append_row(["Bad Name", "Good Name"])
            
            for row in ws.get_all_records():
                bad = str(row.get('Bad Name')).strip().lower()
                good = str(row.get('Good Name')).strip()
                if bad and good:
                    self.alias_map[bad] = good
            logger.info(f"✅ Loaded {len(self.alias_map)} aliases.")
        except Exception as e:
            logger.error(f"⚠️ Alias Load Error: {e}")

    def _load_seed_ratings(self):
        try:
            ws_origin = self.sheet_results.worksheet("ratings Origin")
            for row in ws_origin.get_all_records():
                name = self._clean_name(row.get('Player'))
                rating = row.get('Rating')
                rd = row.get('Deviation') or row.get('RD')
                vol = row.get('Volatility')
                if name and rating:
                    self.rating_engine.set_seed(name, rating, rd, vol)
            logger.info("✅ Loaded Ratings Origin (Baseline)")
        except Exception as e: 
            logger.warning(f"⚠️ Could not load 'ratings Origin': {e}")

    def _save_updated_ratings(self):
        try:
            try: ws = self.sheet_results.worksheet("ratings updated")
            except: ws = self.sheet_results.add_worksheet(title="ratings updated", rows=1000, cols=5)
            data = [['Player', 'Rating', 'Deviation', 'Volatility']]
            sorted_players = sorted(self.rating_engine.players.items(), key=lambda x: x[1]['rating'], reverse=True)
            for name, stats in sorted_players:
                data.append([name, int(stats['rating']), int(stats['rd']), round(stats['vol'], 6)])
            ws.clear()
            ws.update('A1', data)
            logger.info(f"💾 Saved {len(sorted_players)} ratings to 'ratings updated'")
        except Exception as e:
            logger.error(f"❌ Failed to save ratings: {e}")

    def _update_master_roster(self):
        try:
            self.player_ids = {} 
            try: ws = self.sheet_results.worksheet("Players")
            except: 
                ws = self.sheet_results.add_worksheet(title="Players", rows=1000, cols=4)
                ws.append_row(["Player Name", "Player ID", "Date Added", "Status"])

            all_values = ws.get_all_values()
            if not all_values: return

            updates = []
            existing_names = {}
            headers = [str(h).lower().strip() for h in all_values[0]]
            try:
                name_col = headers.index("player name")
                id_col = headers.index("player id")
            except: name_col, id_col = 0, 1

            for i, row in enumerate(all_values[1:], start=2): 
                if not row: continue
                p_name = str(row[name_col]).strip() if len(row) > name_col else ""
                clean_n = self._clean_name(p_name)
                p_id = str(row[id_col]).strip() if len(row) > id_col else ""
                
                if clean_n:
                    if not p_id:
                        new_id = self._generate_player_id()
                        updates.append({'range': f"{chr(65+id_col)}{i}", 'values': [[new_id]]}) 
                        p_id = new_id
                    self.player_ids[clean_n] = p_id
                    existing_names[clean_n.lower()] = True

            active_players = set(self.all_players.keys())
            new_rows = []
            today_str = datetime.date.today().strftime("%Y-%m-%d")
            
            for p_name in active_players:
                if p_name.lower() not in existing_names:
                    new_id = self._generate_player_id()
                    new_rows.append([p_name, new_id, today_str, "Active"])
                    self.player_ids[p_name] = new_id

            if updates: ws.batch_update(updates)
            if new_rows: ws.append_rows(new_rows)
            
        except Exception as e:
            logger.error(f"❌ Master Roster Error: {e}")

    def get_potential_duplicates(self):
        seen = {}
        dupes = []
        try:
            ws = self.sheet_results.worksheet("Players")
            names = [r['Player Name'] for r in ws.get_all_records() if r['Player Name']]
            for name in names:
                key = name.lower().replace(" ", "")
                if key in seen: dupes.append({'name1': seen[key], 'name2': name})
                else: seen[key] = name
            return dupes
        except: return []

    def rename_player_roster(self, old_name, new_name):
        try:
            ws = self.sheet_results.worksheet("Players")
            cell = ws.find(old_name)
            if cell:
                ws.update_cell(cell.row, cell.col, new_name)
                self.refresh_data()
                return True
            return False
        except: return False

    def add_alias(self, bad_name, good_name):
        try:
            ws = self.sheet_results.worksheet("Aliases")
            ws.append_row([bad_name, good_name])
            self.refresh_data() 
            return True
        except: return False

    # --- SAFE LOADER TO FIX DUPLICATE HEADERS ---
    def _get_safe_records(self, worksheet):
        try:
            all_values = worksheet.get_all_values()
            if not all_values: return []
            
            raw_headers = all_values[0]
            clean_headers = []
            counts = {'name': 0, 'sets': 0, 'ps': 0, 'pos': 0, 'player': 0}
            
            for h in raw_headers:
                h_str = str(h).strip()
                h_lower = h_str.lower()
                key = None
                if h_lower == 'name': key = 'name'
                elif h_lower in ['sets', 's']: key = 'sets'
                elif h_lower in ['pos', 'ps']: key = 'pos'
                elif h_lower == 'player': key = 'player'
                
                if key:
                    counts[key] += 1
                    clean_headers.append(f"{h_str} {counts[key]}")
                else:
                    clean_headers.append(h_str)

            records = []
            for row in all_values[1:]:
                if len(row) < len(clean_headers):
                    row = row + [''] * (len(clean_headers) - len(row))
                record = {}
                for i, header in enumerate(clean_headers):
                    record[header] = row[i]
                records.append(record)
            return records
        except Exception as e:
            logger.error(f"Safe Read Error: {e}")
            return []

    def refresh_data(self):
        logger.info("⚡️ Refreshing data...")
        self.all_players = {}
        self.season_stats = {}
        self.seasons_list = ["Career"] 
        self.divisions_list = set()
        self.weekly_matches = {} 
        self.rating_engine = RatingEngine() 
        
        self._load_calculated_dates()
        self._load_aliases()
        self._load_seed_ratings()

        match_queue = [] 

        if not self.sheet_results: return

        for worksheet in self.sheet_results.worksheets():
            title = worksheet.title
            if "Season:" not in title: continue
            
            season_name = title.replace("Season:", "").strip()
            self.seasons_list.append(season_name)
            self.season_stats[season_name] = {}
            if season_name not in self.weekly_matches: self.weekly_matches[season_name] = {}
            
            try:
                # Use Safe Loader Here
                records = self._get_safe_records(worksheet)
                
                for row in records:
                    fmt = str(self._get_val(row, ['Format', 'Match Format', 'Type'])).lower().strip()
                    if "doubles" in fmt: continue 
                    
                    # --- FUZZY LOOKUP FOR NAMES ---
                    # Will find 'Name 1', 'name 1 ', 'Name 1', etc.
                    p1 = self._clean_name(self._get_val(row, ['Name 1', 'Player 1', 'Name']))
                    p2 = self._clean_name(self._get_val(row, ['Name 2', 'Player 2']))
                    
                    if not p1 and not p2: continue
                    if not p1: p1 = "Unknown"
                    if not p2: p2 = "Unknown"

                    div = str(self._get_val(row, ['Division', 'Div'], 'Unknown')).strip()
                    if div: self.divisions_list.add(div)
                    
                    p1_fill = "S" in str(self._get_val(row, ['PS 1', 'Pos 1', 'Pos'])).upper()
                    p2_fill = "S" in str(self._get_val(row, ['PS 2', 'Pos 2'])).upper()

                    match_date_obj = None
                    week_num = "Unknown"
                    round_val = self._get_val(row, ['Round', 'Rd', 'Week'])
                    if round_val:
                        try: week_num = int(re.search(r'\d+', str(round_val)).group())
                        except: pass
                    
                    raw_date = self._get_val(row, ['Date', 'Match Date'])
                    parsed_row_date = self._parse_date(raw_date)

                    if parsed_row_date: match_date_obj = parsed_row_date
                    elif week_num != "Unknown":
                        lookup = f"{season_name}|{div}|{week_num}"
                        if lookup in self.date_lookup: match_date_obj = self.date_lookup[lookup]
                    
                    if not match_date_obj: 
                        match_date_obj = datetime.date(1900, 1, 1)
                        match_date_str = ""
                    else:
                        match_date_str = match_date_obj.strftime("%d/%m/%Y")

                    s1_val = self._get_val(row, ['Sets 1', 'S1', 'Sets'])
                    s2_val = self._get_val(row, ['Sets 2', 'S2'])
                    try:
                        if str(s1_val).strip() == "" or str(s2_val).strip() == "": continue
                        p1_sets = int(s1_val); p2_sets = int(s2_val)
                    except: continue 

                    self._update_player_stats(self.season_stats[season_name], p1, p1_sets, p2_sets, True, p2, p1_fill, div, match_date_str, week_num, season_name)
                    self._update_player_stats(self.all_players, p1, p1_sets, p2_sets, True, p2, p1_fill, div, match_date_str, week_num, season_name)
                    self._update_player_stats(self.season_stats[season_name], p2, p1_sets, p2_sets, False, p1, p2_fill, div, match_date_str, week_num, season_name)
                    self._update_player_stats(self.all_players, p2, p1_sets, p2_sets, False, p1, p2_fill, div, match_date_str, week_num, season_name)

                    match_queue.append({ 'p1': p1, 'p2': p2, 's1': p1_sets, 's2': p2_sets, 'date': match_date_obj })

                    if week_num != "Unknown":
                        w_key = str(week_num)
                        if w_key not in self.weekly_matches[season_name]: self.weekly_matches[season_name][w_key] = []
                        self.weekly_matches[season_name][w_key].append({'p1': p1, 'p2': p2, 'score': f"{p1_sets}-{p2_sets}", 'division': div, 'date': match_date_str})

            except Exception as e: logger.error(f"❌ Error loading {title}: {e}")

        logger.info(f"📚 Total Matches Loaded: {len(match_queue)}")

        match_queue.sort(key=lambda x: x['date'])
        rating_matches = [m for m in match_queue if m['date'] > RATING_START_DATE]
        logger.info(f"🧮 Rating {len(rating_matches)} matches (Post-Christmas 2025)...")
        
        for m in rating_matches:
            self.rating_engine.update_match(m['p1'], m['p2'], m['s1'], m['s2'])

        self._save_updated_ratings()
        self._update_master_roster()

    def _update_player_stats(self, stat_dict, player_name, p1_sets, p2_sets, is_p1, opponent, is_fillin, division, date_str, week_num, season_name):
        if player_name not in stat_dict:
            stat_dict[player_name] = {'regular': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'fillin': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'combined': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}}
        buckets = [stat_dict[player_name]['combined']]
        if is_fillin: buckets.append(stat_dict[player_name]['fillin'])
        else: buckets.append(stat_dict[player_name]['regular'])
        my_sets = p1_sets if is_p1 else p2_sets
        op_sets = p2_sets if is_p1 else p1_sets
        result = "Win" if my_sets > op_sets else "Loss"
        for stats in buckets:
            stats['matches'] += 1; stats['sets_won'] += my_sets; stats['sets_lost'] += op_sets
            if result == "Win": stats['wins'] += 1
            else: stats['losses'] += 1
            stats['history'].append({'season': season_name, 'week': week_num, 'date': date_str, 'opponent': opponent, 'result': result, 'score': f"{my_sets}-{op_sets}", 'type': 'Fill-in' if is_fillin else 'Regular', 'division': division})

    def get_all_players(self): return self.all_players
    def get_matches_by_week(self, season, week):
        if season not in self.weekly_matches: return []
        matches = self.weekly_matches[season].get(str(week), [])
        unique = []; seen = set()
        for m in matches:
            mid = f"{sorted([m['p1'],m['p2']])[0]}|{sorted([m['p1'],m['p2']])[1]}|{m['division']}"
            if mid not in seen: seen.add(mid); unique.append(m)
        return unique

    def get_division_rankings(self, season, division, max_week=None):
        if season not in self.season_stats: return []
        ranking_list = []
        for player_name, stats in self.season_stats[season].items():
            rat = self.rating_engine.get_rating(player_name)
            
            reg_hist = [m for m in stats['regular']['history'] if m['division'] == division]
            fill_hist = [m for m in stats['fillin']['history'] if m['division'] == division]
            
            if max_week and str(max_week) != "All":
                try:
                    limit = int(max_week)
                    reg_hist = [m for m in reg_hist if m['week'] != "Unknown" and int(m['week']) <= limit]
                    fill_hist = [m for m in fill_hist if m['week'] != "Unknown" and int(m['week']) <= limit]
                except: pass
            
            if not reg_hist and not fill_hist: continue 
            
            def calc_summary(history):
                wins = sum(1 for m in history if m['result'] == "Win")
                return {'wins': wins, 'losses': len(history) - wins, 'matches': len(history)}
            
            ranking_list.append({
                'name': player_name, 
                'rating_val': int(rat['rating']), 
                'sigma': int(rat['rd']),
                'regular': calc_summary(reg_hist), 
                'fillin': calc_summary(fill_hist)
            })
        return ranking_list

    def get_all_player_rankings(self, season="Career"):
        data = self.all_players if season == "Career" else self.season_stats.get(season, {})
        ranking_list = []
        for player_name, stats in data.items():
            s = stats['regular']
            if s['matches'] > 0:
                rat = self.rating_engine.get_rating(player_name)
                ranking_list.append({
                    'name': player_name, 
                    'rating': int(rat['rating']),
                    'wins': s['wins'], 
                    'losses': s['losses'], 
                    'matches': s['matches'], 
                    'win_rate': round((s['wins']/s['matches'])*100, 1)
                })
        ranking_list.sort(key=lambda x: (-x['rating'], -x['wins']))
        return ranking_list

    def get_player_stats(self, player_name, season="Career", division="All", week="All", start_date=None, end_date=None):
        data = self.all_players if season == "Career" else self.season_stats.get(season, {})
        if player_name not in data: return None
        
        raw = data[player_name]
        rat = self.rating_engine.get_rating(player_name)
        
        p_id = self.player_ids.get(player_name, "N/A")
        
        start_obj = self._parse_date(start_date) if start_date else None
        end_obj = self._parse_date(end_date) if end_date else None
        
        def format_bucket(stats):
            hist = stats['history']
            if division != "All": hist = [m for m in hist if m.get('division') == division]
            if week != "All": hist = [m for m in hist if str(m.get('week')) == str(week)]
            if start_obj or end_obj:
                hist = [m for m in hist if self._parse_date(m.get('date')) and (not start_obj or self._parse_date(m.get('date')) >= start_obj) and (not end_obj or self._parse_date(m.get('date')) <= end_obj)]
            
            wins = sum(1 for m in hist if m['result'] == "Win")
            win_rate = round((wins / len(hist)) * 100, 1) if hist else 0
            try: hist.sort(key=lambda x: datetime.datetime.strptime(x['date'], "%d/%m/%Y") if x['date'] else datetime.datetime.min)
            except: pass
            disp_hist = list(hist); disp_hist.reverse()
            return {'matches': len(hist), 'wins': wins, 'losses': len(hist)-wins, 'win_rate': f"{win_rate}%", 'sets_won': 0, 'sets_lost': 0, 'match_history': disp_hist}
        
        return {
            'name': player_name, 
            'id': p_id, 
            'rating': int(rat['rating']), 
            'sigma': int(rat['rd']),
            'regular': format_bucket(raw['regular']), 
            'fillin': format_bucket(raw['fillin']), 
            'combined': format_bucket(raw['combined'])
        }

    # --- PASSTHROUGHS ---
    def get_teams(self, season):
        try: return [r for r in self.sheet_results.worksheet("Teams").get_all_records() if str(r['Season']) == str(season)]
        except: return []
    def save_team(self, season, division, team_name, l1, l2, l3=""):
        try:
            ws = self.sheet_results.worksheet("Teams")
            records = ws.get_all_records()
            row = next((i+2 for i, r in enumerate(records) if str(r['Season'])==str(season) and str(r['Division'])==str(division) and str(r['Team Name'])==str(team_name)), None)
            if row: ws.update(f"D{row}:F{row}", [[l1, l2, l3]])
            else: ws.append_row([season, division, team_name, l1, l2, l3]); return True
        except: return False
    def delete_team(self, season, division, team_name):
        try:
            ws = self.sheet_results.worksheet("Teams"); records = ws.get_all_records()
            row = next((i+2 for i, r in enumerate(records) if str(r['Season'])==str(season) and str(r['Division'])==str(division) and str(r['Team Name'])==str(team_name)), None)
            if row: ws.delete_rows(row); return True
            return False
        except: return False
    def create_new_season(self, season_name):
        try: self.sheet_results.add_worksheet(title=f"Season: {season_name}", rows=1000, cols=20).update('A1', [["Round","Date","Table","Division","Team 1","Team 2","Pos 1","Pos 2","Player 1","Player 2","Score","S1","S2","S3","S4","S5"]]); self.refresh_data(); return True
        except: return False
    def delete_season(self, season_name):
        try: self.sheet_results.del_worksheet(self.sheet_results.worksheet(f"Season: {season_name}")); self.refresh_data(); return True
        except: return False
    def get_review_requests(self):
        try: records = self.sheet_results.worksheet("Reports").get_all_records(); 
        except: records = []
        for i, r in enumerate(records): r['row_id'] = i + 2
        return records
    def submit_request(self, reporter, email, season, match_info, description):
        try: self.sheet_results.worksheet("Reports").append_row([datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"), reporter, season, match_info, description, "Pending", email, ""]); return True
        except: return False
    def update_report_status(self, row_id, new_status, reason=""):
        try: self.sheet_results.worksheet("Reports").update(f"F{row_id}:H{row_id}", [[new_status, "", reason]]); return True
        except: return False
    def get_seasons(self): return self.seasons_list
    def get_divisions(self): return sorted(list(self.divisions_list))
    def get_head_to_head(self, p1, p2):
        if p1 not in self.all_players: return None
        matches = [m for m in self.all_players[p1]['combined']['history'] if m['opponent'].lower() == p2.lower()]
        matches.reverse(); wins = sum(1 for m in matches if m['result'] == "Win")
        return {'player1': p1, 'player2': p2, 'matches': matches, 'p1_wins': wins, 'p2_wins': len(matches) - wins}