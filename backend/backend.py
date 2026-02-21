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
import difflib

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
EXPORT_SPREADSHEET_ID = "1Vo4HUelw9Vvy24BjuQ7XVhWI6F7dYtlwrrzT9bv0CVw"   
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
            rd_val = float(rd) if rd and str(rd).strip() else DEFAULT_RD
            vol_val = float(vol) if vol and str(vol).strip() else DEFAULT_VOL
            if vol_val <= 0.0001: vol_val = DEFAULT_VOL
            if rd_val < 0: rd_val = DEFAULT_RD
            self.players[name] = {'rating': r_val, 'rd': rd_val, 'vol': vol_val}
        except ValueError: pass
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
        self.sheet_results = None; self.sheet_export = None; self.db = None 
        self.sky_engine = SkyEngine() if SkyEngine else None
        self.rating_engine = RatingEngine()
        self.all_players = {}; self.season_stats = {}; self.seasons_list = ["Career"] 
        self.divisions_list = set(); self.date_lookup = {}; self.weekly_matches = {} 
        self.player_ids = {}; self.alias_map = {}; self.date_to_week_map = {} 
        self._authenticate()
        # REMOVED: self.refresh_data() -> This allows the server to boot instantly.

    def _authenticate(self):
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
                try: self.sheet_results = self.client.open_by_key(RESULTS_SPREADSHEET_ID); logger.info("✅ Connected to Master")
                except: logger.error("❌ Master Sheet Fail")
                try: self.sheet_export = self.client.open_by_key(EXPORT_SPREADSHEET_ID)
                except: pass
        except Exception as e: logger.error(f"Auth Error: {e}")
        try:
            try: app = firebase_admin.get_app()
            except ValueError:
                cred_path = 'firebase_credentials.json'
                if not os.path.exists(cred_path) and os.path.exists('backend/firebase_credentials.json'): cred_path = 'backend/firebase_credentials.json'
                cred = credentials.Certificate(cred_path)
                firebase_admin.initialize_app(cred)
            self.db = firestore.client()
            logger.info("🔥 Connected to Firebase")
        except: self.db = None

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

    def _generate_player_id(self): return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))

    def _load_calculated_dates(self):
        self.date_lookup = {}; self.date_to_week_map = {}
        try:
            ws = self.sheet_results.worksheet("Calculated_Dates")
            for row in ws.get_all_records():
                s = str(row.get('Season','')).strip(); d = str(row.get('Division','')).strip(); w = str(row.get('Week','')).strip()
                date_val = str(row.get('Date','')); parsed = self._parse_date(date_val)
                if parsed: self.date_lookup[f"{s}|{d}|{w}"] = parsed; self.date_to_week_map[f"{s}|{parsed.strftime('%Y-%m-%d')}"] = w
        except: pass

    def _load_aliases(self):
        self.alias_map = {}
        try:
            ws = self.sheet_results.worksheet("Aliases")
            for row in ws.get_all_records():
                bad = str(row.get('Bad Name')).strip().lower(); good = str(row.get('Good Name')).strip()
                if bad and good: self.alias_map[bad] = good
        except: pass

    def _load_seed_ratings(self):
        try:
            ws_origin = self.sheet_results.worksheet("ratings Origin")
            for row in ws_origin.get_all_records():
                name = self._clean_name(row.get('Player')); rating = row.get('Rating'); rd = row.get('Deviation'); vol = row.get('Volatility')
                if name and rating: self.rating_engine.set_seed(name, rating, rd, vol)
        except: pass

    def _save_updated_ratings(self):
        if not self.sheet_results: return
        try:
            try: ws = self.sheet_results.worksheet("ratings updated")
            except: ws = self.sheet_results.add_worksheet(title="ratings updated", rows=1000, cols=5)
            data = [['Player', 'Rating', 'Deviation', 'Volatility']]
            sorted_players = sorted(self.rating_engine.players.items(), key=lambda x: x[1]['rating'], reverse=True)
            for name, stats in sorted_players: data.append([name, int(stats['rating']), int(stats['rd']), round(stats['vol'], 6)])
            ws.clear(); ws.update('A1', data)
        except: pass

    def _update_master_roster(self):
        if not self.sheet_results: return
        try:
            self.player_ids = {} 
            try: ws = self.sheet_results.worksheet("Players")
            except: ws = self.sheet_results.add_worksheet(title="Players", rows=1000, cols=4); ws.append_row(["Player Name", "Player ID", "Date Added", "Status"])
            all_values = ws.get_all_values(); updates = []; existing_names = {}
            if not all_values: return
            headers = [str(h).lower().strip() for h in all_values[0]]
            try: name_col, id_col = headers.index("player name"), headers.index("player id")
            except: name_col, id_col = 0, 1
            for i, row in enumerate(all_values[1:], start=2): 
                if not row: continue
                p_name = str(row[name_col]).strip() if len(row) > name_col else ""
                clean_n = self._clean_name(p_name)
                p_id = str(row[id_col]).strip() if len(row) > id_col else ""
                if clean_n:
                    if not p_id: new_id = self._generate_player_id(); updates.append({'range': f"{chr(65+id_col)}{i}", 'values': [[new_id]]}); p_id = new_id
                    self.player_ids[clean_n] = p_id; existing_names[clean_n.lower()] = True
            active_players = set(self.all_players.keys()); new_rows = []
            today_str = datetime.date.today().strftime("%Y-%m-%d")
            for p_name in active_players:
                if p_name.lower() not in existing_names: new_id = self._generate_player_id(); new_rows.append([p_name, new_id, today_str, "Active"]); self.player_ids[p_name] = new_id
            if updates: ws.batch_update(updates)
            if new_rows: ws.append_rows(new_rows)
        except: pass

    def _sync_to_firebase(self):
        if not self.db: return
        try:
            batch = self.db.batch(); batch_count = 0
            for player_name, stats in self.all_players.items():
                rat = self.rating_engine.get_rating(player_name)
                safe_id = re.sub(r'[^a-zA-Z0-9]', '_', player_name).lower()
                doc_ref = self.db.collection('player_profiles').document(safe_id)
                data = {'name': player_name, 'rating': int(rat['rating']), 'wins': stats['combined']['wins'], 'losses': stats['combined']['losses'], 'matches_played': stats['combined']['matches'], 'last_updated': datetime.datetime.now()}
                batch.set(doc_ref, data, merge=True); batch_count += 1
                if batch_count >= 400: batch.commit(); batch = self.db.batch(); batch_count = 0
            if batch_count > 0: batch.commit()
        except: pass

    def _get_safe_records(self, worksheet):
        try:
            all_values = worksheet.get_all_values()
            if not all_values: return []
            raw_headers = all_values[0]; clean_headers = []
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
            
            if match_key not in unique_map: 
                unique_map[match_key] = m
            else:
                existing = unique_map[match_key]
                if m.get('manual_override'): unique_map[match_key] = m
                elif existing.get('manual_override'): continue
                elif m.get('rich_stats') and not existing.get('rich_stats'): unique_map[match_key] = m
        return list(unique_map.values())

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

    def refresh_data(self):
        logger.info("⚡️ Fetching and Processing Data...")
        self.all_players = {}; self.season_stats = {}; self.seasons_list = ["Career"]; self.divisions_list = set(); self.weekly_matches = {}; self.rating_engine = RatingEngine() 
        if self.sheet_results: self._load_calculated_dates(); self._load_aliases(); self._load_seed_ratings()
        raw_match_queue = [] 
        
        # 1. LOAD FROM GOOGLE SHEETS
        if self.sheet_results:
            for worksheet in self.sheet_results.worksheets():
                title = worksheet.title
                if "Season:" not in title: continue
                season_name = title.replace("Season:", "").strip(); self.seasons_list.append(season_name)
                if season_name not in self.season_stats: self.season_stats[season_name] = {}
                if season_name not in self.weekly_matches: self.weekly_matches[season_name] = {}
                try:
                    records = self._get_safe_records(worksheet)
                    for row in records:
                        row_vals = [str(v).lower() for v in row.values()]
                        if any("doubles" in v for v in row_vals): continue
                        p1 = self._clean_name(self._get_val(row, ['Name 1', 'Player 1', 'Name'])); p2 = self._clean_name(self._get_val(row, ['Name 2', 'Player 2']))
                        if not p1 or not p2: continue
                        div = str(self._get_val(row, ['Division', 'Div'], 'Unknown')).strip(); self.divisions_list.add(div)
                        p1_fill = "S" in str(self._get_val(row, ['PS 1', 'Pos 1', 'Pos'])).upper(); p2_fill = "S" in str(self._get_val(row, ['PS 2', 'Pos 2'])).upper()
                        round_val = self._get_val(row, ['Round', 'Rd', 'Week']); week_num = "Unknown"
                        if round_val:
                            try: week_num = int(re.search(r'\d+', str(round_val)).group())
                            except: pass
                        raw_date = self._get_val(row, ['Date', 'Match Date']); parsed_date = self._parse_date(raw_date)
                        if (not parsed_date) and str(week_num) != "Unknown": parsed_date = self.date_lookup.get(f"{season_name}|{div}|{week_num}")
                        if not parsed_date: parsed_date = datetime.date(1900, 1, 1)
                        try: s1 = int(self._get_val(row, ['Sets 1', 'S1', 'Sets'])); s2 = int(self._get_val(row, ['Sets 2', 'S2']))
                        except: continue
                        raw_match_queue.append({'p1': p1, 'p2': p2, 's1': s1, 's2': s2, 'date': parsed_date, 'season': season_name, 'week': week_num, 'div': div, 'p1_fill': p1_fill, 'p2_fill': p2_fill, 'game_history': '', 'rich_stats': None, 'manual_override': False})
                except: pass
                
        # 2. LOAD FROM FIREBASE (iPad)
        if self.db:
            try:
                docs = self.db.collection('match_results').stream()
                for doc in docs:
                    d = doc.to_dict(); date_val = d.get('date'); parsed_date = self._parse_date(date_val) or datetime.date.today()
                    season_name = d.get('season', f"Season: {parsed_date.year}"); home_p = d.get('home_players', []); away_p = d.get('away_players', [])
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
                    raw_match_queue.append({'p1': p1, 'p2': p2, 's1': s1, 's2': s2, 'date': parsed_date, 'season': season_name, 'week': week_val, 'div': d.get('division', 'Unknown'), 'p1_fill': False, 'p2_fill': False, 'game_history': d.get('game_scores_history', ''), 'rich_stats': {'duration': d.get('match_duration')}, 'manual_override': d.get('manual_override', False)})
            except: pass

        # 3. LOAD CORRECTIONS AND OVERRIDE
        corrections = {}
        if self.db:
            try:
                c_docs = self.db.collection('match_corrections').stream()
                for doc in c_docs:
                    c = doc.to_dict()
                    c_p1 = self._clean_name(c.get('p1', ''))
                    c_p2 = self._clean_name(c.get('p2', ''))
                    d_str = str(c.get('date', ''))
                    players = sorted([c_p1, c_p2])
                    key = f"{d_str}_{players[0]}_{players[1]}"
                    corrections[key] = {'s1': c.get('s1', 0), 's2': c.get('s2', 0), 'c_p1': c_p1}
            except Exception as e: logger.error(f"Error loading corrections: {e}")

        # APPLY CORRECTIONS (INTERCEPT THE DATA)
        for m in raw_match_queue:
            players = sorted([m['p1'], m['p2']])
            d_str = m['date'].strftime("%d/%m/%Y") if m['date'] else "nodate"
            key = f"{d_str}_{players[0]}_{players[1]}"
            if key in corrections:
                c = corrections[key]
                if m['p1'] == c['c_p1']:
                    m['s1'] = c['s1']; m['s2'] = c['s2']
                else:
                    m['s1'] = c['s2']; m['s2'] = c['s1']
                m['manual_override'] = True

        # 4. CALCULATE STATS & SAVE
        cleaned_matches = self._deduplicate_matches(raw_match_queue); cleaned_matches.sort(key=lambda x: x['date'])
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
                wk = str(m['week']); 
                if wk not in self.weekly_matches[m['season']]: self.weekly_matches[m['season']][wk] = []
                self.weekly_matches[m['season']][wk].append({'p1': m['p1'], 'p2': m['p2'], 'score': f"{m['s1']}-{m['s2']}", 'division': m['div'], 'date': d_str})
        
        logger.info(f"👤 Total Unique Players Loaded: {len(player_set)}")
        self._save_updated_ratings(); self._update_master_roster(); self._sync_to_firebase()

    # --- ADMIN METHODS ---
    
    def admin_search_history(self, query_text):
        q_lower = query_text.lower()
        results = []
        seen = set()
        for p_name, data in self.all_players.items():
            if q_lower in p_name.lower() or q_lower in str(data.get('combined', {}).get('history', [])).lower():
                for m in data['combined']['history']:
                    date = m.get('date', '')
                    opp = m.get('opponent', '')
                    if q_lower in p_name.lower() or q_lower in opp.lower() or q_lower in date:
                        players = sorted([p_name, opp])
                        match_hash = f"{date}_{players[0]}_{players[1]}"
                        if match_hash not in seen:
                            seen.add(match_hash)
                            score = m.get('score', '0-0')
                            try: s1, s2 = map(int, score.split('-'))
                            except: s1, s2 = 0, 0
                            
                            results.append({
                                'id': match_hash,
                                'date': date,
                                'division': m.get('division', ''),
                                'home_players': [p_name],
                                'away_players': [opp],
                                'score': score,
                                's1': s1,
                                's2': s2
                            })
        return results

    def admin_update_historical_match(self, p1, p2, date, s1, s2):
        if not self.db: return False
        try:
            self.db.collection('match_corrections').add({
                'p1': p1, 'p2': p2, 'date': date,
                's1': int(s1), 's2': int(s2),
                'timestamp': firestore.SERVER_TIMESTAMP
            })
            self.refresh_data()
            return True
        except: return False

    def user_submit_report(self, p1, p2, date, reporter, problem, suggested_s1, suggested_s2):
        if not self.db: return False
        try:
            report_data = {
                'p1': p1, 'p2': p2, 'date': date,
                'reporter': reporter, 'problem': problem,
                'suggested_s1': int(suggested_s1), 'suggested_s2': int(suggested_s2),
                'match_desc': f"{p1} vs {p2} on {date}",
                'status': 'Pending', 'timestamp': firestore.SERVER_TIMESTAMP
            }
            self.db.collection('match_reports').add(report_data); return True
        except: return False

    def admin_get_reports(self):
        if not self.db: return []
        try:
            docs = self.db.collection('match_reports').where('status', '==', 'Pending').order_by('timestamp', direction=firestore.Query.DESCENDING).stream()
            reports = []
            for d in docs: data = d.to_dict(); data['id'] = d.id; reports.append(data)
            return reports
        except: return []

    def admin_resolve_report(self, report_id, action):
        if not self.db: return False
        try:
            report_ref = self.db.collection('match_reports').document(report_id)
            report = report_ref.get().to_dict()
            if action == 'approve':
                self.db.collection('match_corrections').add({
                    'p1': report['p1'],
                    'p2': report['p2'],
                    'date': report['date'],
                    's1': report['suggested_s1'],
                    's2': report['suggested_s2'],
                    'timestamp': firestore.SERVER_TIMESTAMP
                })
                report_ref.update({'status': 'Approved'})
                self.refresh_data()
            elif action == 'reject': 
                report_ref.update({'status': 'Rejected'})
            return True
        except: return False

    def admin_merge_players(self, bad_name, good_name):
        if not self.db: return False
        batch = self.db.batch(); count = 0
        home_games = self.db.collection('match_results').where('home_players', 'array_contains', bad_name).stream()
        for doc in home_games:
            d = doc.to_dict(); new_home = [good_name if p == bad_name else p for p in d.get('home_players', [])]
            batch.update(doc.reference, {'home_players': new_home, 'home_team': good_name}); count += 1
        away_games = self.db.collection('match_results').where('away_players', 'array_contains', bad_name).stream()
        for doc in away_games:
            d = doc.to_dict(); new_away = [good_name if p == bad_name else p for p in d.get('away_players', [])]
            batch.update(doc.reference, {'away_players': new_away, 'away_team': good_name}); count += 1
        safe_id_bad = re.sub(r'[^a-zA-Z0-9]', '_', bad_name).lower()
        batch.delete(self.db.collection('players').document(safe_id_bad))
        batch.delete(self.db.collection('player_profiles').document(safe_id_bad))
        if count > 0: batch.commit()
        if self.sheet_results:
            try: self.sheet_results.worksheet("Aliases").append_row([bad_name, good_name])
            except: pass
        self.refresh_data(); return True

    def admin_get_merge_suggestions(self):
        names = list(self.all_players.keys())
        suggestions = []; seen = set()
        for name in names:
            matches = difflib.get_close_matches(name, names, n=3, cutoff=0.85)
            for m in matches:
                if m == name: continue
                pair = tuple(sorted((name, m)))
                if pair not in seen:
                    c1 = self.all_players[name]['combined']['matches']; c2 = self.all_players[m]['combined']['matches']
                    if c1 == 0 and c2 == 0: continue
                    bad = name if c1 < c2 else m; good = m if bad == name else name
                    suggestions.append({'bad': bad, 'good': good, 'reason': f"Similarity {int(difflib.SequenceMatcher(None, name, m).ratio()*100)}%"})
                    seen.add(pair)
        return suggestions

    def get_all_players(self): return self.all_players
    def get_matches_by_week(self, season, week): return self.weekly_matches.get(season, {}).get(str(week), [])
    def get_seasons(self): return self.seasons_list
    def get_divisions(self): return sorted(list(self.divisions_list))
    
    def get_roster_with_meta(self):
        roster = []
        for name, data in self.all_players.items():
            rat = self.rating_engine.get_rating(name)
            c = data['combined']; m = c['matches']
            divs = {}; 
            for h in c['history']: divs[h['division']] = divs.get(h['division'], 0) + 1
            main_div = max(divs, key=divs.get) if divs else "New"
            roster.append({"name": name, "label": f"{name} ({main_div} • {int(rat['rating'])} • {m} games)"})
        return sorted(roster, key=lambda x: x['name'])
        
    def get_division_rankings(self, season, division, max_week=None):
        if season not in self.season_stats: return []
        ranking_list = []
        for player_name, stats in self.season_stats[season].items():
            rat = self.rating_engine.get_rating(player_name)
            reg_hist = [m for m in stats['regular']['history'] if m['division'] == division]
            fill_hist = [m for m in stats['fillin']['history'] if m['division'] == division]
            if not reg_hist and not fill_hist: continue 
            def calc_summary(history):
                wins = sum(1 for m in history if m['result'] == "Win")
                return {'wins': wins, 'losses': len(history) - wins, 'matches': len(history)}
            ranking_list.append({'name': player_name, 'rating_val': int(rat['rating']), 'sigma': int(rat['rd']), 'regular': calc_summary(reg_hist), 'fillin': calc_summary(fill_hist)})
        return ranking_list
        
    def get_player_stats(self, player_name, season="Career", division="All", week="All", start_date=None, end_date=None):
        data = self.all_players if season == "Career" else self.season_stats.get(season, {})
        if player_name not in data: return None
        raw = data[player_name]; rat = self.rating_engine.get_rating(player_name)
        def format_bucket(stats):
            hist = stats['history']
            if division != "All": hist = [m for m in hist if m.get('division') == division]
            wins = sum(1 for m in hist if m['result'] == "Win")
            win_rate = round((wins / len(hist)) * 100, 1) if hist else 0
            disp_hist = list(hist); disp_hist.reverse()
            return {'matches': len(hist), 'wins': wins, 'losses': len(hist)-wins, 'win_rate': f"{win_rate}%", 'match_history': disp_hist}
        return {'name': player_name, 'rating': int(rat['rating']), 'combined': format_bucket(raw['combined'])}