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
import math
import hashlib

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path: sys.path.append(current_dir)

try: from sky_engine import SkyEngine
except ImportError:
    try: from backend.sky_engine import SkyEngine
    except: SkyEngine = None

DEFAULT_RATING = 1000.0
DEFAULT_RD = 350.0
DEFAULT_VOL = 0.06
TAU = 0.5        
SCALE = 173.7178 

def _glicko2_core(mu, phi, vol, opponent_mu, opponent_phi, score):
    def g(p): return 1.0 / math.sqrt(1.0 + 3.0 * p**2 / (math.pi**2))
    def E(m, mj, pj): return 1.0 / (1.0 + math.exp(-g(pj) * (m - mj)))
    g_j = g(opponent_phi)
    E_j = E(mu, opponent_mu, opponent_phi)
    v = 1.0 / (g_j**2 * E_j * (1.0 - E_j))
    delta = v * g_j * (score - E_j)
    a = math.log(vol**2)
    def f(x):
        ex = math.exp(x)
        num = ex * (delta**2 - phi**2 - v - ex)
        den = 2.0 * (phi**2 + v + ex)**2
        return (num / den) - ((x - a) / TAU**2)
    A = a
    if delta**2 > phi**2 + v: B = math.log(delta**2 - phi**2 - v)
    else:
        k = 1
        while f(a - k * TAU) < 0: k += 1
        B = a - k * TAU
    fA, fB = f(A), f(B)
    for _ in range(100): 
        if abs(B - A) <= 0.000001: break
        C = A + (A - B) * fA / (fB - fA)
        fC = f(C)
        if fC * fB <= 0: A, fA = B, fB
        else: fA /= 2.0
        B, fB = C, fC
    new_vol = math.exp(A / 2.0)
    phi_star = math.sqrt(phi**2 + new_vol**2)
    new_phi = 1.0 / math.sqrt(1.0 / phi_star**2 + 1.0 / v)
    new_mu = mu + new_phi**2 * g_j * (score - E_j)
    return new_mu, new_phi, new_vol

def calculate_match(w, l, s1, s2):
    mu_w, phi_w, vol_w = (w['rating'] - 1500) / SCALE, w['rd'] / SCALE, w['vol']
    mu_l, phi_l, vol_l = (l['rating'] - 1500) / SCALE, l['rd'] / SCALE, l['vol']
    total_sets = s1 + s2
    if total_sets == 0: return {'winner': w, 'loser': l}
    w_score = 0.75 + 0.25 * ((s1 - s2) / total_sets)
    l_score = 1.0 - w_score
    new_mu_w, new_phi_w, new_vol_w = _glicko2_core(mu_w, phi_w, vol_w, mu_l, phi_l, w_score)
    new_mu_l, new_phi_l, new_vol_l = _glicko2_core(mu_l, phi_l, vol_l, mu_w, phi_w, l_score)
    w['rating'] = new_mu_w * SCALE + 1500
    w['rd'] = max(30.0, new_phi_w * SCALE)
    w['vol'] = new_vol_w
    l['rating'] = new_mu_l * SCALE + 1500
    l['rd'] = max(30.0, new_phi_l * SCALE)
    l['vol'] = new_vol_l
    return {'winner': w, 'loser': l}

RESULTS_SPREADSHEET_ID = "1tpxuUCl8ddpnBBr69vc4P1foRCRKWpts5-HaFPYb4po" 
EXPORT_SPREADSHEET_ID = "1Vo4HUelw9Vvy24BjuQ7XVhWI6F7dYtlwrrzT9bv0CVw"   
RATING_START_DATE = datetime.date(2025, 12, 25)

class RatingEngine:
    def __init__(self): self.players = {} 
    def get_rating(self, name):
        if name not in self.players: self.players[name] = {'rating': DEFAULT_RATING, 'rd': DEFAULT_RD, 'vol': DEFAULT_VOL}
        return self.players[name]
    def set_seed(self, name, rating, rd=None, vol=None):
        try:
            r_val = float(rating); rd_val = float(rd) if rd and str(rd).strip() else DEFAULT_RD; vol_val = float(vol) if vol and str(vol).strip() else DEFAULT_VOL
            if vol_val <= 0.0001: vol_val = DEFAULT_VOL
            if rd_val < 0: rd_val = DEFAULT_RD
            self.players[name] = {'rating': r_val, 'rd': rd_val, 'vol': vol_val}
        except ValueError: pass
    def update_match(self, p1_name, p2_name, s1, s2):
        if s1 == s2: return 
        p1_stats = self.get_rating(p1_name); p2_stats = self.get_rating(p2_name)
        if s1 > s2: res = calculate_match(p1_stats, p2_stats, s1, s2); self.players[p1_name] = res['winner']; self.players[p2_name] = res['loser']
        else: res = calculate_match(p2_stats, p1_stats, s2, s1); self.players[p2_name] = res['winner']; self.players[p1_name] = res['loser']

class ThunderData:
    def __init__(self):
        self.scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        self.client = None
        self.sheet_results = None; self.db = None 
        self.rating_engine = RatingEngine()
        self.all_players = {}; self.season_stats = {}; self.seasons_list = ["Career"] 
        self.divisions_list = set(); self.date_lookup = {}; self.weekly_matches = {} 
        self.player_ids = {}; self.alias_map = {}; self.date_to_week_map = {} 
        self._authenticate()

    def _authenticate(self):
        try:
            creds_json = os.environ.get("GOOGLE_CREDS_JSON")
            if creds_json: self.creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=self.scopes)
            else:
                paths = ["credentials.json", "backend/credentials.json"]
                found = next((p for p in paths if os.path.exists(p)), None)
                self.creds = Credentials.from_service_account_file(found, scopes=self.scopes) if found else None
            if self.creds:
                self.client = gspread.authorize(self.creds)
                try: self.sheet_results = self.client.open_by_key(RESULTS_SPREADSHEET_ID); logger.info("✅ Connected to Master")
                except: logger.error("❌ Master Sheet Fail")
        except Exception as e: logger.error(f"Auth Error: {e}")
        try:
            try: app = firebase_admin.get_app()
            except ValueError:
                cred_path = 'firebase_credentials.json'
                if not os.path.exists(cred_path) and os.path.exists('backend/firebase_credentials.json'): cred_path = 'backend/firebase_credentials.json'
                firebase_admin.initialize_app(credentials.Certificate(cred_path))
            self.db = firestore.client()
            self._ensure_config_exists()
        except: self.db = None
        
    def _ensure_config_exists(self):
        if not self.db: return
        doc_ref = self.db.collection('system_config').document('main')
        if not doc_ref.get().exists: doc_ref.set({'tournament_mode_active': False})

    def _clean_name(self, name): 
        if not name: return ""
        clean = " ".join(str(name).split())
        return self.alias_map.get(clean.lower(), clean.title())

    def _get_val(self, row, keys, default=''):
        row_keys_norm = {k.strip().lower(): k for k in row.keys()}
        for k in keys:
            if k.strip().lower() in row_keys_norm: return row[row_keys_norm[k.strip().lower()]]
        return default

    def _parse_date(self, date_str):
        if not date_str: return None
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d"):
            try: return datetime.datetime.strptime(str(date_str).strip(), fmt).date()
            except ValueError: continue
        return None

    def _generate_player_id(self): return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))

    def _load_calculated_dates(self):
        self.date_lookup = {}; self.date_to_week_map = {}
        try:
            for row in self.sheet_results.worksheet("Calculated_Dates").get_all_records():
                s = str(row.get('Season','')).strip(); d = str(row.get('Division','')).strip(); w = str(row.get('Week','')).strip()
                parsed = self._parse_date(str(row.get('Date','')))
                if parsed: self.date_lookup[f"{s}|{d}|{w}"] = parsed; self.date_to_week_map[f"{s}|{parsed.strftime('%Y-%m-%d')}"] = w
        except: pass

    def _load_aliases(self):
        self.alias_map = {}
        try:
            for row in self.sheet_results.worksheet("Aliases").get_all_records():
                bad = str(row.get('Bad Name')).strip().lower(); good = str(row.get('Good Name')).strip()
                if bad and good: self.alias_map[bad] = good
        except: pass

    def _load_seed_ratings(self):
        try:
            ws = self.sheet_results.worksheet("Ratings base")
            rows = ws.get_all_values()
            if not rows: return
            headers = [str(h).lower().strip() for h in rows[0]]
            p_idx = headers.index('player') if 'player' in headers else 0
            peterman_idx = 1 
            r_idx = headers.index('rating') if 'rating' in headers else 2
            d_idx = headers.index('deviation') if 'deviation' in headers else 3
            v_idx = headers.index('volatility') if 'volatility' in headers else 4
            for row in rows[1:]:
                if len(row) <= p_idx: continue
                name = self._clean_name(row[p_idx])
                if not name: continue
                peterman_id = str(row[peterman_idx]).strip() if len(row) > peterman_idx else ""
                rating = row[r_idx] if len(row) > r_idx else ""
                dev = row[d_idx] if len(row) > d_idx else ""
                vol = row[v_idx] if len(row) > v_idx else ""
                if rating: self.rating_engine.set_seed(name, rating, dev, vol)
                if name not in self.all_players:
                    self.all_players[name] = {'regular': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'fillin': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'combined': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}}
                self.all_players[name]['peterman_id'] = peterman_id
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
            counts = {'name': 0, 'sets': 0, 'ps': 0, 'player': 0}
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
        final_matches = []
        groups = {}
        for m in matches:
            p1 = m['p1'].lower().strip(); p2 = m['p2'].lower().strip()
            if not p1 or not p2: continue
            players = sorted([p1, p2])
            date_key = m['date'].strftime("%Y%m%d") if m['date'] else "nodate"
            match_key = f"{date_key}_{players[0]}_{players[1]}"
            if match_key not in groups: groups[match_key] = []
            groups[match_key].append(m)
            
        for key, group in groups.items():
            if len(group) == 1:
                final_matches.append(group[0])
                continue
            firebase_m = [m for m in group if m.get('rich_stats') is not None]
            sheet_m = [m for m in group if m.get('rich_stats') is None]
            merged = []
            for fm in firebase_m:
                merged.append(fm)
                if sheet_m: sheet_m.pop(0) 
            for sm in sheet_m: merged.append(sm)
            final_matches.extend(merged)
        return final_matches

    def _update_player_stats(self, stat_dict, player_name, p1_sets, p2_sets, is_p1, opponent, is_fillin, division, date_str, week_num, season_name, details="", rich_stats=None, match_id=""):
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
            stats['history'].append({'season': season_name, 'week': week_num, 'date': date_str, 'opponent': opponent, 'result': result, 'score': f"{my_sets}-{op_sets}", 'type': 'Fill-in' if is_fillin else 'Regular', 'division': division, 'details': details, 'rich_stats': rich_stats, 'match_id': match_id})

    def refresh_data(self):
        logger.info("⚡️ Fetching and Processing Data...")
        self.all_players = {}; self.season_stats = {}; self.seasons_list = ["Career"]; self.divisions_list = set(); self.weekly_matches = {}; self.rating_engine = RatingEngine() 
        if self.sheet_results: self._load_calculated_dates(); self._load_aliases(); self._load_seed_ratings()
        raw_match_queue = [] 
        
        if self.sheet_results:
            for worksheet in self.sheet_results.worksheets():
                title = worksheet.title
                if "season" not in title.lower(): continue
                season_name = re.sub(r'(?i)^season\s*:\s*', '', title).strip()
                if season_name not in self.seasons_list: self.seasons_list.append(season_name)
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
                
        if self.db:
            try:
                docs = self.db.collection('match_results').stream()
                for doc in docs:
                    d = doc.to_dict()
                    if d.get('status') == 'pending' or d.get('status') == 'rejected': continue
                    date_val = d.get('date'); parsed_date = self._parse_date(date_val) or datetime.date.today()
                    raw_season = str(d.get('season', f"Season: {parsed_date.year}"))
                    season_name = re.sub(r'(?i)^season\s*:\s*', '', raw_season).strip()
                    if season_name not in self.seasons_list and season_name != "Unknown": self.seasons_list.append(season_name)
                    if season_name not in self.season_stats: self.season_stats[season_name] = {}
                    if season_name not in self.weekly_matches: self.weekly_matches[season_name] = {}
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
                    rich = d.get('richStats', {})
                    rich['total_duration'] = d.get('total_duration', '00:00'); rich['play_duration'] = d.get('play_duration', '00:00'); rich['set_scores'] = d.get('set_scores', [])
                    raw_match_queue.append({'p1': p1, 'p2': p2, 's1': s1, 's2': s2, 'date': parsed_date, 'season': season_name, 'week': week_val, 'div': d.get('division', 'Unknown'), 'p1_fill': False, 'p2_fill': False, 'game_history': d.get('game_scores_history', ''), 'rich_stats': rich, 'manual_override': d.get('manual_override', False)})
            except: pass

        logger.info(f"📊 FINAL LOADED SEASONS LIST: {self.seasons_list}")

        corrections = {}
        if self.db:
            try:
                c_docs = self.db.collection('match_corrections').stream()
                for doc in c_docs:
                    c = doc.to_dict(); c_p1 = self._clean_name(c.get('p1', '')); c_p2 = self._clean_name(c.get('p2', '')); d_str = str(c.get('date', ''))
                    players = sorted([c_p1, c_p2]); key = f"{d_str}_{players[0]}_{players[1]}"
                    corrections[key] = {'s1': c.get('s1', 0), 's2': c.get('s2', 0), 'c_p1': c_p1}
            except Exception as e: logger.error(f"Error loading corrections: {e}")

        for m in raw_match_queue:
            players = sorted([m['p1'], m['p2']]); d_str = m['date'].strftime("%d/%m/%Y") if m['date'] else "nodate"; key = f"{d_str}_{players[0]}_{players[1]}"
            if key in corrections:
                c = corrections[key]
                if m['p1'] == c['c_p1']: m['s1'] = c['s1']; m['s2'] = c['s2']
                else: m['s1'] = c['s2']; m['s2'] = c['s1']
                m['manual_override'] = True

        cleaned_matches = self._deduplicate_matches(raw_match_queue); cleaned_matches.sort(key=lambda x: x['date'])
        player_set = set()
        for m in cleaned_matches:
            players = sorted([m['p1'], m['p2']])
            d_str = m['date'].strftime("%d/%m/%Y") if m['date'] else "nodate"
            
            # --- NEW: GENERATE UNIQUE MATCH ID ---
            raw_id_string = f"{d_str}_{players[0]}_{players[1]}_{m['s1']}_{m['s2']}"
            match_id = hashlib.md5(raw_id_string.encode()).hexdigest()[:6].upper()
            
            if m['date'] > RATING_START_DATE: self.rating_engine.update_match(m['p1'], m['p2'], m['s1'], m['s2'])
            
            for p, sets_for, sets_against, is_p1, opp, fill in [(m['p1'], m['s1'], m['s2'], True, m['p2'], m['p1_fill']), (m['p2'], m['s1'], m['s2'], False, m['p1'], m['p2_fill'])]:
                self._update_player_stats(self.season_stats.get(m['season'], {}), p, sets_for, sets_against, is_p1, opp, fill, m['div'], d_str, m['week'], m['season'], m.get('game_history', ''), m.get('rich_stats'), match_id)
                self._update_player_stats(self.all_players, p, sets_for, sets_against, is_p1, opp, fill, m['div'], d_str, m['week'], m['season'], m.get('game_history', ''), m.get('rich_stats'), match_id)
                player_set.add(p)
            if str(m['week']) != "Unknown":
                if m['season'] not in self.weekly_matches: self.weekly_matches[m['season']] = {}
                wk = str(m['week']); 
                if wk not in self.weekly_matches[m['season']]: self.weekly_matches[m['season']][wk] = []
                self.weekly_matches[m['season']][wk].append({'p1': m['p1'], 'p2': m['p2'], 'score': f"{m['s1']}-{m['s2']}", 'division': m['div'], 'date': d_str})
        
        if self.db:
            try:
                overrides = self.db.collection('rating_overrides').stream()
                for doc in overrides:
                    d = doc.to_dict()
                    p_name = d.get('name')
                    if p_name in self.rating_engine.players:
                        self.rating_engine.players[p_name]['rating'] = float(d.get('rating', 1500))
            except Exception as e: logger.error(f"Error applying overrides: {e}")

        logger.info(f"👤 Total Unique Players Loaded: {len(player_set)}")
        self._save_updated_ratings(); self._update_master_roster(); self._sync_to_firebase()

    def get_matches_by_week(self, season, week): return self.weekly_matches.get(season, {}).get(str(week), [])
    def get_all_players(self): return self.all_players
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
            roster.append({"name": name, "label": f"{name} ({main_div} • {int(rat['rating'])} • {m} games)", "rating": int(rat['rating'])})
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
        peterman_id = self.all_players.get(player_name, {}).get('peterman_id', '')
        return {'name': player_name, 'rating': int(rat['rating']), 'combined': format_bucket(raw['combined']), 'peterman_id': peterman_id}

    def get_system_config(self):
        if not self.db: return {"tournament_mode_active": False}
        try: doc = self.db.collection('system_config').document('main').get(); return doc.to_dict() if doc.exists else {"tournament_mode_active": False}
        except: return {"tournament_mode_active": False}
        
    def set_system_config(self, key, value):
        if not self.db: return False
        try: self.db.collection('system_config').document('main').set({key: value}, merge=True); return True
        except: return False

    def admin_get_teams(self):
        if not self.db: return {}
        try: docs = self.db.collection('teams').stream(); return {d.id: d.to_dict().get('players', []) for d in docs}
        except: return {}

    def admin_update_team(self, team_name, players_list):
        if not self.db: return False
        try: self.db.collection('teams').document(team_name).set({'players': players_list}, merge=True); return True
        except: return False

    def user_submit_feedback(self, category, text, contact, context="Not provided"):
        if not self.db: return False
        try:
            self.db.collection('feedback').add({'type': category, 'message': text, 'contact': contact, 'context': context, 'timestamp': firestore.SERVER_TIMESTAMP, 'status': 'New'})
            return True
        except: return False

    def admin_get_ranks(self):
        if not self.db: return {}
        try: docs = self.db.collection('player_ranks').stream(); return {d.to_dict().get('name'): d.to_dict().get('rank') for d in docs}
        except: return {}

    def admin_set_rank(self, player_name, rank):
        if not self.db: return False
        try: safe_id = re.sub(r'[^a-zA-Z0-9]', '_', player_name).lower(); self.db.collection('player_ranks').document(safe_id).set({'name': player_name, 'rank': int(rank)}); return True
        except: return False
        
    def admin_override_rating(self, player_name, new_rating):
        if not self.db: return False
        try:
            safe_id = re.sub(r'[^a-zA-Z0-9]', '_', player_name).lower()
            self.db.collection('rating_overrides').document(safe_id).set({
                'name': player_name,
                'rating': float(new_rating),
                'timestamp': firestore.SERVER_TIMESTAMP
            })
            return True
        except Exception as e:
            logger.error(f"Error overriding rating: {e}")
            return False
            
    def admin_get_reports(self):
        if not self.db: 
            return []
        try: 
            reports = []
            m_docs = self.db.collection('match_reports').where('status', '==', 'Pending').stream()
            for d in m_docs:
                data = d.to_dict()
                reports.append({
                    'id': d.id,
                    'type': 'MATCH_ERROR',
                    'reporter': data.get('reporter', 'Unknown'),
                    'date': data.get('date', 'Unknown Date'),
                    'title': f"{data.get('p1', '')} vs {data.get('p2', '')}",
                    'problem': data.get('problem', ''),
                    'suggested_s1': data.get('suggested_s1', ''),
                    'suggested_s2': data.get('suggested_s2', ''),
                    'match_id': data.get('match_id', ''),
                    'timestamp': data.get('timestamp')
                })
            f_docs = self.db.collection('feedback').where('status', '==', 'New').stream()
            for d in f_docs:
                data = d.to_dict()
                ts = data.get('timestamp')
                date_str = ts.strftime('%d/%m/%Y') if hasattr(ts, 'strftime') else 'Recent'
                reports.append({
                    'id': d.id,
                    'type': 'FEEDBACK',
                    'reporter': data.get('contact', 'Anonymous'),
                    'date': date_str,
                    'title': f"Feedback: {data.get('type', 'General')}",
                    'problem': f"Context: {data.get('context', '')}\n\nMessage: {data.get('message', '')}",
                    'suggested_s1': '',
                    'suggested_s2': '',
                    'match_id': '',
                    'timestamp': ts
                })
            def safe_ts(x):
                ts = x.get('timestamp')
                if hasattr(ts, 'timestamp'): return ts.timestamp()
                if isinstance(ts, (int, float)): return ts
                return 0
            reports.sort(key=safe_ts, reverse=True)
            return reports
        except Exception as e: 
            logger.error(f"Error in admin_get_reports: {str(e)}")
            return []

    def admin_resolve_report(self, report_id, action):
        if not self.db: return False
        try:
            report_ref = self.db.collection('match_reports').document(report_id)
            doc = report_ref.get()
            if doc.exists:
                report = doc.to_dict()
                if action == 'approve': 
                    self.db.collection('match_corrections').add({'p1': report['p1'], 'p2': report['p2'], 'date': report['date'], 's1': report['suggested_s1'], 's2': report['suggested_s2'], 'timestamp': firestore.SERVER_TIMESTAMP})
                    report_ref.update({'status': 'Approved'})
                    self.refresh_data()
                elif action == 'reject': 
                    report_ref.update({'status': 'Rejected'})
                return True
            feed_ref = self.db.collection('feedback').document(report_id)
            if feed_ref.get().exists:
                if action == 'approve': feed_ref.update({'status': 'Resolved'})
                elif action == 'reject': feed_ref.update({'status': 'Dismissed'})
                return True
            return False
        except: return False
    
    def admin_get_pending_approvals(self):
        if not self.db: return []
        try:
            docs = self.db.collection('match_results').where('status', '==', 'pending').stream()
            res = []
            for d in docs: data = d.to_dict(); data['id'] = d.id; ts = data.get('timestamp'); data['ts_str'] = str(ts) if ts else "Unknown"; res.append(data)
            return sorted(res, key=lambda x: x.get('ts_str', ''), reverse=True)
        except: return []

    def admin_resolve_approval(self, doc_id, action, s1=None, s2=None):
        if not self.db: return False
        try:
            doc_ref = self.db.collection('match_results').document(doc_id)
            if action == 'approve': doc_ref.update({'status': 'approved', 'live_home_sets': int(s1) if s1 is not None else 0, 'live_away_sets': int(s2) if s2 is not None else 0}); self.refresh_data()
            elif action == 'reject': doc_ref.update({'status': 'rejected'})
            return True
        except: return False

    def admin_get_recent_approved(self):
        if not self.db: return []
        try:
            docs = self.db.collection('match_results').stream(); res = []
            for d in docs:
                data = d.to_dict()
                if data.get('status', 'approved') == 'approved': data['id'] = d.id; ts = data.get('timestamp'); data['ts_sort'] = ts.timestamp() if hasattr(ts, 'timestamp') else 0; res.append(data)
            res.sort(key=lambda x: x.get('ts_sort', 0), reverse=True)
            return res[:50]
        except: return []

    def admin_delete_match_result(self, doc_id):
        if not self.db: return False
        try: self.db.collection('match_results').document(doc_id).delete(); self.refresh_data(); return True
        except: return False

    def admin_search_history(self, query_text, season_filter=None, div_filter=None, week_filter=None, date_filter=None):
        q_lower = query_text.lower() if query_text else ""
        results = []; seen = set()
        for p_name, data in self.all_players.items():
            for m in data.get('combined', {}).get('history', []):
                date = str(m.get('date', '')); opp = str(m.get('opponent', '')); m_season = str(m.get('season', '')); m_div = str(m.get('division', '')); m_week = str(m.get('week', ''))
                match_id = str(m.get('match_id', ''))
                if season_filter and season_filter != "All" and season_filter != m_season: continue
                if div_filter and div_filter != "All" and div_filter != m_div: continue
                if week_filter and week_filter != "All" and week_filter != m_week: continue
                if date_filter and date_filter != date: continue
                # NEW: Allows searching by Match ID now
                if q_lower and q_lower not in p_name.lower() and q_lower not in opp.lower() and q_lower not in date and q_lower not in match_id.lower(): continue
                players = sorted([p_name, opp]); match_hash = f"{date}_{m_season}_{m_week}_{players[0]}_{players[1]}_{m.get('score', '0-0')}"
                if match_hash not in seen:
                    seen.add(match_hash); score = m.get('score', '0-0')
                    try: s1, s2 = map(int, score.split('-'))
                    except: s1, s2 = 0, 0
                    results.append({'id': match_hash, 'date': date, 'division': m_div, 'season': m_season, 'week': m_week, 'home_players': [p_name], 'away_players': [opp], 'score': score, 's1': s1, 's2': s2, 'match_id': match_id})
        def sort_key(x):
            try: return datetime.datetime.strptime(x['date'], "%d/%m/%Y")
            except: return datetime.datetime(1900, 1, 1)
        results.sort(key=sort_key, reverse=True)
        return results[:150]

    def admin_update_historical_match(self, p1, p2, date, s1, s2):
        if not self.db: return False
        try: self.db.collection('match_corrections').add({'p1': p1, 'p2': p2, 'date': date, 's1': int(s1), 's2': int(s2), 'timestamp': firestore.SERVER_TIMESTAMP}); self.refresh_data(); return True
        except: return False

    def user_submit_report(self, p1, p2, date, reporter, problem, suggested_s1, suggested_s2, match_id=""):
        if not self.db: return False
        try: self.db.collection('match_reports').add({'match_id': match_id, 'p1': p1, 'p2': p2, 'date': date, 'reporter': reporter, 'problem': problem, 'suggested_s1': int(suggested_s1), 'suggested_s2': int(suggested_s2), 'match_desc': f"{p1} vs {p2} on {date}", 'status': 'Pending', 'timestamp': firestore.SERVER_TIMESTAMP}); return True
        except: return False

    def admin_merge_players(self, bad_name, good_name):
        if not self.db: return False
        batch = self.db.batch(); count = 0
        home_games = self.db.collection('match_results').where('home_players', 'array_contains', bad_name).stream()
        for doc in home_games: d = doc.to_dict(); new_home = [good_name if p == bad_name else p for p in d.get('home_players', [])]; batch.update(doc.reference, {'home_players': new_home, 'home_team': good_name}); count += 1
        away_games = self.db.collection('match_results').where('away_players', 'array_contains', bad_name).stream()
        for doc in away_games: d = doc.to_dict(); new_away = [good_name if p == bad_name else p for p in d.get('away_players', [])]; batch.update(doc.reference, {'away_players': new_away, 'away_team': good_name}); count += 1
        safe_id_bad = re.sub(r'[^a-zA-Z0-9]', '_', bad_name).lower()
        batch.delete(self.db.collection('players').document(safe_id_bad))
        batch.delete(self.db.collection('player_profiles').document(safe_id_bad))
        if count > 0: batch.commit()
        if self.sheet_results:
            try: self.sheet_results.worksheet("Aliases").append_row([bad_name, good_name])
            except: pass
        self.refresh_data(); return True

    def admin_get_merge_suggestions(self):
        names = list(self.all_players.keys()); suggestions = []; seen = set()
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

    def admin_generate_balanced_teams(self, players, team_size):
        pool = []
        for p in players:
            name = p.get('name'); rating_data = self.rating_engine.get_rating(name)
            pool.append({'name': name, 'rank': int(p.get('rank', 3)), 'lock': p.get('lock', ''), 'rating': int(rating_data['rating'])})
        rank1 = sorted([p for p in pool if p['rank'] == 1], key=lambda x: x['rating'], reverse=True)
        rank2 = sorted([p for p in pool if p['rank'] == 2], key=lambda x: x['rating']) 
        rank3 = sorted([p for p in pool if p['rank'] == 3], key=lambda x: x['rating'], reverse=True)
        teams = []
        num_teams = len(rank1) if len(rank1) > 0 else (len(pool) // team_size)
        for i in range(num_teams):
            r1_player = rank1[i] if i < len(rank1) else None
            t_players = [r1_player] if r1_player else []
            t_rating = r1_player['rating'] if r1_player else 0
            if r1_player and r1_player['lock']:
                locked_player = next((p for p in pool if p['name'] == r1_player['lock']), None)
                if locked_player:
                    t_players.append(locked_player); t_rating += locked_player['rating']
                    if locked_player in rank2: rank2.remove(locked_player)
                    if locked_player in rank3: rank3.remove(locked_player)
            teams.append({'id': i+1, 'name': f"Team {i+1}", 'players': t_players, 'total_rating': t_rating})
        teams.sort(key=lambda x: x['total_rating'], reverse=True) 
        for r2_player in rank2:
            eligible_teams = [t for t in teams if len(t['players']) < 2]
            if eligible_teams:
                eligible_teams.sort(key=lambda x: x['total_rating'])
                chosen_team = eligible_teams[0]; chosen_team['players'].append(r2_player); chosen_team['total_rating'] += r2_player['rating']
                if r2_player['lock']:
                    locked_p3 = next((p for p in rank3 if p['name'] == r2_player['lock']), None)
                    if locked_p3: chosen_team['players'].append(locked_p3); chosen_team['total_rating'] += locked_p3['rating']; rank3.remove(locked_p3)
        if team_size == 3:
            for r3_player in rank3:
                eligible_teams = [t for t in teams if len(t['players']) < 3]
                if eligible_teams:
                    eligible_teams.sort(key=lambda x: x['total_rating'])
                    chosen_team = eligible_teams[0]; chosen_team['players'].append(r3_player); chosen_team['total_rating'] += r3_player['rating']
        for t in teams: t['average_rating'] = int(t['total_rating'] / len(t['players'])) if len(t['players']) > 0 else 0
        return teams