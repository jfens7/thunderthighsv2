import gspread
from google.oauth2.service_account import Credentials
import firebase_admin
from firebase_admin import credentials, firestore
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

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path: sys.path.append(current_dir)

# Import Dependencies
try:
    from sky_engine import SkyEngine
except ImportError:
    try: from backend.sky_engine import SkyEngine
    except: SkyEngine = None

try:
    from ratings_logic import calculate_match, DEFAULT_RATING, DEFAULT_RD, DEFAULT_VOL
except ImportError:
    logger.error("⚠️ ratings_logic.py not found! Using dummy logic.")
    DEFAULT_RATING, DEFAULT_RD, DEFAULT_VOL = 1500.0, 350.0, 0.06
    def calculate_match(w, l, s1, s2): return {'winner': w, 'loser': l}

# --- CONFIG ---
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
             self.players[p1_name] = res['winner']; self.players[p2_name] = res['loser']
        else:
             res = calculate_match(p2_stats, p1_stats, s2, s1)
             self.players[p2_name] = res['winner']; self.players[p1_name] = res['loser']

class ThunderData:
    def __init__(self):
        self.scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        self.client = None
        self.sheet_results = None
        self.db = None 
        
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
        self.date_to_week_map = {} 
        
        self._authenticate()
        self.refresh_data()

    def _authenticate(self):
        # 1. Google Sheets Auth
        try:
            creds_json = os.environ.get("GOOGLE_CREDS_JSON")
            if creds_json:
                info = json.loads(creds_json)
                self.creds = Credentials.from_service_account_info(info, scopes=self.scopes)
            else:
                paths = ["credentials.json", "backend/credentials.json"]
                found = next((p for p in paths if os.path.exists(p)), None)
                if found: self.creds = Credentials.from_service_account_file(found, scopes=self.scopes)
                else: self.creds = None
            
            if self.creds:
                self.client = gspread.authorize(self.creds)
                self.sheet_results = self.client.open_by_key(RESULTS_SPREADSHEET_ID)
                logger.info("✅ Connected to Google Sheets")
        except Exception as e:
            logger.error(f"❌ Google Sheets Auth Error: {e}")

        # 2. Firebase Auth
        try:
            try: app = firebase_admin.get_app()
            except ValueError:
                cred_path = 'firebase_credentials.json'
                if not os.path.exists(cred_path) and os.path.exists('backend/firebase_credentials.json'):
                    cred_path = 'backend/firebase_credentials.json'
                cred = credentials.Certificate(cred_path)
                firebase_admin.initialize_app(cred)
            self.db = firestore.client()
            logger.info("🔥 Connected to Firebase")
        except Exception as e:
            logger.warning(f"⚠️ Firebase Connection Failed: {e}")
            self.db = None

    def _clean_name(self, name): 
        if not name: return ""
        clean = " ".join(str(name).split())
        if clean.lower() in self.alias_map: return self.alias_map[clean.lower()]
        return clean.title()

    def _get_val(self, row, keys, default=''):
        row_keys_norm = {k.strip().lower(): k for k in row.keys()}
        for k in keys:
            norm_k = k.strip().lower()
            if norm_k in row_keys_norm: return row[row_keys_norm[norm_k]]
        return default

    def _parse_date(self, date_str):
        if not date_str: return None
        if isinstance(date_str, datetime.date): return date_str
        if isinstance(date_str, datetime.datetime): return date_str.date()
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d"):
            try: return datetime.datetime.strptime(str(date_str).strip(), fmt).date()
            except ValueError: continue
        return None

    def _generate_player_id(self):
        chars = string.ascii_uppercase + string.digits
        return ''.join(random.choice(chars) for _ in range(8))

    def _load_calculated_dates(self):
        self.date_lookup = {}
        self.date_to_week_map = {}
        try:
            ws = self.sheet_results.worksheet("Calculated_Dates")
            for row in ws.get_all_records():
                s = str(row.get('Season','')).strip()
                d = str(row.get('Division','')).strip()
                w = str(row.get('Week','')).strip()
                date_val = str(row.get('Date',''))
                parsed = self._parse_date(date_val)
                if parsed: 
                    self.date_lookup[f"{s}|{d}|{w}"] = parsed
                    key = f"{s}|{parsed.strftime('%Y-%m-%d')}"
                    self.date_to_week_map[key] = w
            logger.info(f"✅ Loaded {len(self.date_lookup)} calculated dates.")
        except: 
            logger.warning("⚠️ Could not load 'Calculated_Dates' tab.")

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
                if bad and good: self.alias_map[bad] = good
            logger.info(f"✅ Loaded {len(self.alias_map)} aliases.")
        except: pass

    def _load_seed_ratings(self):
        try:
            ws_origin = self.sheet_results.worksheet("ratings Origin")
            for row in ws_origin.get_all_records():
                name = self._clean_name(row.get('Player'))
                rating = row.get('Rating')
                rd = row.get('Deviation') or row.get('RD')
                vol = row.get('Volatility')
                if name and rating: self.rating_engine.set_seed(name, rating, rd, vol)
            logger.info("✅ Loaded Ratings Origin")
        except: pass

    def _save_updated_ratings(self):
        if not self.sheet_results: return
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
        except: pass

    def _update_master_roster(self):
        # Syncs to Google Sheet (Legacy / Backup)
        if not self.sheet_results: return
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
            try: name_col, id_col = headers.index("player name"), headers.index("player id")
            except: name_col, id_col = 0, 1
            for i, row in enumerate(all_values[1:], start=2): 
                if not row: continue
                p_name = str(row[name_col]).strip() if len(row) > name_col else ""
                clean_n = self._clean_name(p_name)
                p_id = str(row[id_col]).strip() if len(row) > id_col else ""
                if clean_n:
                    if not p_id:
                        new_id = self._generate_player_id(); updates.append({'range': f"{chr(65+id_col)}{i}", 'values': [[new_id]]}); p_id = new_id
                    self.player_ids[clean_n] = p_id; existing_names[clean_n.lower()] = True
            
            # --- ADD NEW IPAD PLAYERS TO SHEET ---
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
        except Exception as e: logger.error(f"❌ Master Roster Error: {e}")

    def _sync_to_firebase(self):
        # Syncs STATS to 'player_profiles' (for Website)
        if not self.db: return
        try:
            batch = self.db.batch()
            batch_count = 0
            MAX_BATCH = 450
            for player_name, stats in self.all_players.items():
                rat = self.rating_engine.get_rating(player_name)
                safe_id = re.sub(r'[^a-zA-Z0-9]', '_', player_name).lower()
                doc_ref = self.db.collection('player_profiles').document(safe_id)
                data = {'name': player_name, 'rating': int(rat['rating']), 'wins': stats['combined']['wins'], 'losses': stats['combined']['losses'], 'matches_played': stats['combined']['matches'], 'last_updated': datetime.datetime.now()}
                batch.set(doc_ref, data, merge=True)
                batch_count += 1
                if batch_count >= MAX_BATCH: batch.commit(); batch = self.db.batch(); batch_count = 0
            if batch_count > 0: batch.commit()
            logger.info("🔥 Synced player profiles to Firebase.")
        except Exception as e: logger.error(f"Firebase Profile Sync Error: {e}")

    # --- NEW: SYNC ROSTER FOR IPAD ---
    def _sync_roster_to_firestore_search(self):
        # Syncs NAMES to 'players' collection (for iPad Search)
        if not self.db: return
        try:
            # We want to push ALL players we know about (from Sheets + iPad) to the search list
            all_known_names = set(self.all_players.keys())
            
            # We assume names in self.all_players are already "Cleaned"
            batch = self.db.batch()
            batch_count = 0
            MAX_BATCH = 450
            
            for name in all_known_names:
                # Use name as ID for simplicity in search or a safe ID
                safe_id = re.sub(r'[^a-zA-Z0-9]', '_', name).lower()
                # If name is empty, skip
                if not name.strip(): continue
                
                doc_ref = self.db.collection('players').document(safe_id)
                
                # We only need the name and active status for the iPad to find it
                data = {
                    'name': name,
                    'active': True,
                    'isProvisional': False # It came from our master list
                }
                
                batch.set(doc_ref, data, merge=True)
                batch_count += 1
                
                if batch_count >= MAX_BATCH:
                    batch.commit()
                    batch = self.db.batch()
                    batch_count = 0
            
            if batch_count > 0: batch.commit()
            logger.info(f"📱 Synced {len(all_known_names)} players to iPad Search list.")
            
        except Exception as e:
            logger.error(f"iPad Roster Sync Error: {e}")

    def _get_safe_records(self, worksheet):
        try:
            all_values = worksheet.get_all_values()
            if not all_values: return []
            raw_headers = all_values[0]
            clean_headers = []
            counts = {'name': 0, 'sets': 0, 'ps': 0, 'pos': 0, 'player': 0}
            for h in raw_headers:
                h_str = str(h).strip(); h_lower = h_str.lower(); key = None
                if h_lower == 'name': key = 'name'
                elif h_lower in ['sets', 's']: key = 'sets'
                elif h_lower in ['pos', 'ps']: key = 'pos'
                elif h_lower == 'player': key = 'player'
                if key: counts[key] += 1; clean_headers.append(f"{h_str} {counts[key]}")
                else: clean_headers.append(h_str)
            records = []
            for row in all_values[1:]:
                if len(row) < len(clean_headers): row = row + [''] * (len(clean_headers) - len(row))
                record = {}; 
                for i, header in enumerate(clean_headers): record[header] = row[i]
                records.append(record)
            return records
        except: return []

    def _deduplicate_matches(self, matches):
        unique_map = {}
        for m in matches:
            p1 = m['p1'].lower().strip(); p2 = m['p2'].lower().strip()
            if not p1 or not p2: continue
            players = sorted([p1, p2])
            date_key = m['date'].strftime("%Y%m%d") if m['date'] else "nodate"
            match_key = f"{date_key}_{players[0]}_{players[1]}"
            is_rich = False
            if m.get('rich_stats'): is_rich = True
            elif len(str(m.get('game_history', ''))) > 5: is_rich = True
            if match_key not in unique_map: unique_map[match_key] = m
            else:
                existing = unique_map[match_key]
                existing_is_rich = False
                if existing.get('rich_stats'): existing_is_rich = True
                elif len(str(existing.get('game_history', ''))) > 5: existing_is_rich = True
                if is_rich and not existing_is_rich: unique_map[match_key] = m
        return list(unique_map.values())

    def refresh_data(self):
        logger.info("⚡️ Refreshing data...")
        self.all_players = {}
        self.season_stats = {}
        self.seasons_list = ["Career"] 
        self.divisions_list = set()
        self.weekly_matches = {} 
        self.rating_engine = RatingEngine() 
        if self.sheet_results: self._load_calculated_dates(); self._load_aliases(); self._load_seed_ratings()
        raw_match_queue = [] 
        if self.sheet_results:
            logger.info("📄 Reading from Google Sheets...")
            for worksheet in self.sheet_results.worksheets():
                title = worksheet.title
                if "Season:" not in title: continue
                season_name = title.replace("Season:", "").strip()
                self.seasons_list.append(season_name)
                if season_name not in self.season_stats: self.season_stats[season_name] = {}
                if season_name not in self.weekly_matches: self.weekly_matches[season_name] = {}
                try:
                    records = self._get_safe_records(worksheet)
                    for row in records:
                        row_vals = [str(v).lower() for v in row.values()]
                        if any("doubles" in v for v in row_vals): continue
                        p1 = self._clean_name(self._get_val(row, ['Name 1', 'Player 1', 'Name']))
                        p2 = self._clean_name(self._get_val(row, ['Name 2', 'Player 2']))
                        if not p1 or not p2: continue
                        div = str(self._get_val(row, ['Division', 'Div'], 'Unknown')).strip()
                        if div: self.divisions_list.add(div)
                        p1_fill = "S" in str(self._get_val(row, ['PS 1', 'Pos 1', 'Pos'])).upper()
                        p2_fill = "S" in str(self._get_val(row, ['PS 2', 'Pos 2'])).upper()
                        round_val = self._get_val(row, ['Round', 'Rd', 'Week'])
                        week_num = "Unknown"
                        if round_val:
                            try: week_num = int(re.search(r'\d+', str(round_val)).group())
                            except: pass
                        raw_date = self._get_val(row, ['Date', 'Match Date'])
                        parsed_date = self._parse_date(raw_date)
                        if (not parsed_date) and str(week_num) != "Unknown": parsed_date = self.date_lookup.get(f"{season_name}|{div}|{week_num}")
                        if not parsed_date: parsed_date = datetime.date(1900, 1, 1)
                        try: s1 = int(self._get_val(row, ['Sets 1', 'S1', 'Sets'])); s2 = int(self._get_val(row, ['Sets 2', 'S2']))
                        except: continue
                        raw_match_queue.append({'p1': p1, 'p2': p2, 's1': s1, 's2': s2, 'date': parsed_date, 'season': season_name, 'week': week_num, 'div': div, 'p1_fill': p1_fill, 'p2_fill': p2_fill, 'game_history': '', 'rich_stats': None})
                except Exception as e: logger.error(f"❌ Error loading Sheet {title}: {e}")
        if self.db:
            try:
                self.sync_todays_fixtures() 
                docs = self.db.collection('match_results').stream()
                for doc in docs:
                    d = doc.to_dict()
                    date_val = d.get('date'); parsed_date = self._parse_date(date_val) or datetime.date.today()
                    season_name = d.get('season', f"Season: {parsed_date.year}")
                    home_p = d.get('home_players', []); away_p = d.get('away_players', [])
                    if not home_p or not away_p: continue
                    p1 = self._clean_name(home_p[0]); p2 = self._clean_name(away_p[0])
                    s1 = d.get('live_home_sets', 0); s2 = d.get('live_away_sets', 0)
                    if s1 == 0 and s2 == 0 and d.get('game_scores_history'):
                        t1=0; t2=0
                        for s in str(d.get('game_scores_history')).split(','):
                            try: 
                                if int(s.split('-')[0]) > int(s.split('-')[1]): t1+=1
                                else: t2+=1
                            except: pass
                        s1=t1; s2=t2
                    week_val = d.get('week', 'Unknown')
                    if str(week_val) == "Unknown" and parsed_date: key = f"{season_name}|{parsed_date.strftime('%Y-%m-%d')}"; week_val = self.date_to_week_map.get(key, "Unknown")
                    rich_stats = {'duration': d.get('match_duration'), 'timeouts': d.get('timeouts'), 'serve_stats': d.get('serve_stats'), 'comebacks': d.get('comebacks'), 'fastest_rally': d.get('fastest_rally')}
                    raw_match_queue.append({'p1': p1, 'p2': p2, 's1': s1, 's2': s2, 'date': parsed_date, 'season': season_name, 'week': week_val, 'div': d.get('division', 'Unknown'), 'p1_fill': False, 'p2_fill': False, 'game_history': d.get('game_scores_history', ''), 'rich_stats': rich_stats})
            except: pass
        cleaned_matches = self._deduplicate_matches(raw_match_queue)
        logger.info(f"📚 Total Matches Merged & Loaded: {len(cleaned_matches)}")
        cleaned_matches.sort(key=lambda x: x['date'])
        player_set = set()
        for m in cleaned_matches:
            if m['date'] > RATING_START_DATE: self.rating_engine.update_match(m['p1'], m['p2'], m['s1'], m['s2'])
            d_str = m['date'].strftime("%d/%m/%Y")
            for p, sets_for, sets_against, is_p1, opp, fill in [(m['p1'], m['s1'], m['s2'], True, m['p2'], m['p1_fill']), (m['p2'], m['s1'], m['s2'], False, m['p1'], m['p2_fill'])]:
                self._update_player_stats(self.season_stats.get(m['season'], {}), p, sets_for, sets_against, is_p1, opp, fill, m['div'], d_str, m['week'], m['season'], m.get('game_history', ''), m.get('rich_stats'))
                self._update_player_stats(self.all_players, p, sets_for, sets_against, is_p1, opp, fill, m['div'], d_str, m['week'], m['season'], m.get('game_history', ''), m.get('rich_stats'))
                player_set.add(p)
            if str(m['week']) != "Unknown":
                if m['season'] not in self.weekly_matches: self.weekly_matches[m['season']] = {}
                wk = str(m['week'])
                if wk not in self.weekly_matches[m['season']]: self.weekly_matches[m['season']][wk] = []
                self.weekly_matches[m['season']][wk].append({'p1': m['p1'], 'p2': m['p2'], 'score': f"{m['s1']}-{m['s2']}", 'division': m['div'], 'date': d_str})
        logger.info(f"👤 Total Unique Players Loaded: {len(player_set)}")
        self._save_updated_ratings(); self._update_master_roster(); self._sync_to_firebase(); self._sync_roster_to_firestore_search() # <--- NEW CALL

    def _update_player_stats(self, stat_dict, player_name, p1_sets, p2_sets, is_p1, opponent, is_fillin, division, date_str, week_num, season_name, details="", rich_stats=None):
        if stat_dict is None: return 
        if player_name not in stat_dict: stat_dict[player_name] = {'regular': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'fillin': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'combined': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}}
        buckets = [stat_dict[player_name]['combined']]; 
        if is_fillin: buckets.append(stat_dict[player_name]['fillin'])
        else: buckets.append(stat_dict[player_name]['regular'])
        my_sets = p1_sets if is_p1 else p2_sets; op_sets = p2_sets if is_p1 else p1_sets; result = "Win" if my_sets > op_sets else "Loss"
        for stats in buckets:
            stats['matches'] += 1; stats['sets_won'] += my_sets; stats['sets_lost'] += op_sets
            if result == "Win": stats['wins'] += 1
            else: stats['losses'] += 1
            stats['history'].append({'season': season_name, 'week': week_num, 'date': date_str, 'opponent': opponent, 'result': result, 'score': f"{my_sets}-{op_sets}", 'type': 'Fill-in' if is_fillin else 'Regular', 'division': division, 'details': details, 'rich_stats': rich_stats})

    def sync_todays_fixtures(self):
        if not self.sheet_results or not self.db: return
        try:
            today_str = datetime.date.today().strftime("%d/%m/%Y")
            target_sheet = None
            for ws in self.sheet_results.worksheets():
                if "Season: Summer 2026" in ws.title: target_sheet = ws; break 
            if not target_sheet: return
            records = self._get_safe_records(target_sheet)
            batch = self.db.batch(); updates = 0
            for row in records:
                if str(row.get('Date', '')) == today_str:
                    p1 = self._clean_name(row.get('Name 1', ''))
                    p2 = self._clean_name(row.get('Name 2', ''))
                    table = row.get('Table', '')
                    if p1 and p2 and table:
                        docs = self.db.collection('fixture_schedule').where('date', '==', today_str).where('table', '==', str(table)).stream()
                        for doc in docs:
                            d = doc.to_dict()
                            if d.get('match_status') == 'Live': continue 
                            ref = self.db.collection('fixture_schedule').document(doc.id)
                            batch.update(ref, {'home_players': [p1], 'away_players': [p2]})
                            updates += 1
            if updates > 0: batch.commit(); logger.info(f"🔄 Synced {updates} matches with player names.")
        except Exception as e: logger.error(f"Sync Error: {e}")

    def get_all_players(self): return self.all_players
    def get_matches_by_week(self, season, week):
        if season not in self.weekly_matches: return []
        matches = self.weekly_matches[season].get(str(week), [])
        return matches
    def get_division_rankings(self, season, division, max_week=None):
        if season not in self.season_stats: return []
        ranking_list = []
        for player_name, stats in self.season_stats[season].items():
            rat = self.rating_engine.get_rating(player_name)
            reg_hist = [m for m in stats['regular']['history'] if m['division'] == division]
            fill_hist = [m for m in stats['fillin']['history'] if m['division'] == division]
            if max_week and str(max_week) != "All":
                try: limit = int(max_week)
                except: limit = 99
                reg_hist = [m for m in reg_hist if str(m['week']).isdigit() and int(m['week']) <= limit]
                fill_hist = [m for m in fill_hist if str(m['week']).isdigit() and int(m['week']) <= limit]
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
    def get_player_stats(self, player_name, season="Career", division="All", week="All", start_date=None, end_date=None):
        data = self.all_players if season == "Career" else self.season_stats.get(season, {})
        if player_name not in data: return None
        raw = data[player_name]
        rat = self.rating_engine.get_rating(player_name)
        def format_bucket(stats):
            hist = stats['history']
            if division != "All": hist = [m for m in hist if m.get('division') == division]
            wins = sum(1 for m in hist if m['result'] == "Win")
            win_rate = round((wins / len(hist)) * 100, 1) if hist else 0
            disp_hist = list(hist); disp_hist.reverse()
            return {'matches': len(hist), 'wins': wins, 'losses': len(hist)-wins, 'win_rate': f"{win_rate}%", 'match_history': disp_hist}
        return {
            'name': player_name, 
            'rating': int(rat['rating']), 
            'combined': format_bucket(raw['combined'])
        }
    def get_seasons(self): return self.seasons_list
    def get_divisions(self): return sorted(list(self.divisions_list))