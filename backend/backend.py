import gspread
from google.oauth2.service_account import Credentials
import firebase_admin
from firebase_admin import credentials, firestore, auth
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
import urllib.request
import urllib.parse
import base64

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path: sys.path.append(current_dir)

try: from sky_engine import SkyEngine
except ImportError:
    try: from backend.sky_engine import SkyEngine
    except: SkyEngine = None

# --- CUSTOM RATINGS CENTRAL HYBRID ENGINE ---
DEFAULT_RATING = 1000.0
DEFAULT_RD = 300.0  # Starting Uncertainty (Standard Deviation)

def calculate_match(w, l, s1, s2):
    total_sets = s1 + s2
    if total_sets == 0: return {'winner': w, 'loser': l}
    
    w_score = 0.75 + 0.25 * ((s1 - s2) / total_sets)
    l_score = 1.0 - w_score
    
    E_w = 1.0 / (1.0 + 10.0 ** ((l['rating'] - w['rating']) / 400.0))
    E_l = 1.0 - E_w
    
    w_dampener = 50.0 / max(50.0, l['rd'] * 0.5)
    l_dampener = 50.0 / max(50.0, w['rd'] * 0.5)
    
    K_w = max(50.0, w['rd'] * 0.8)
    K_l = max(50.0, l['rd'] * 0.8)
    
    w_shift = K_w * w_dampener * (w_score - E_w)
    l_shift = K_l * l_dampener * (l_score - E_l)
    
    w['rating'] += w_shift
    l['rating'] += l_shift
    
    w['rd'] = max(50.0, w['rd'] - 4.0)
    l['rd'] = max(50.0, l['rd'] - 4.0)
    
    return {'winner': w, 'loser': l}

RESULTS_SPREADSHEET_ID = "1tpxuUCl8ddpnBBr69vc4P1foRCRKWpts5-HaFPYb4po" 
EXPORT_SPREADSHEET_ID = "1Vo4HUelw9Vvy24BjuQ7XVhWI6F7dYtlwrrzT9bv0CVw"   
RATING_START_DATE = datetime.date(2025, 12, 25)
SUPER_ADMIN_EMAIL = "jakobwill7@gmail.com"

class RatingEngine:
    def __init__(self): self.players = {} 
    
    def get_rating(self, name):
        if name not in self.players: self.players[name] = {'rating': DEFAULT_RATING, 'rd': DEFAULT_RD, 'vol': 0.06}
        return self.players[name]
        
    def set_seed(self, name, rating, rd=None, vol=None):
        try:
            r_val = float(rating); rd_val = float(rd) if rd and str(rd).strip() else DEFAULT_RD
            if rd_val < 0: rd_val = DEFAULT_RD
            v_val = float(vol) if vol is not None else 0.06
            self.players[name] = {'rating': r_val, 'rd': rd_val, 'vol': v_val}
        except ValueError: pass
        
    def update_match(self, p1_name, p2_name, s1, s2):
        if s1 == s2: return {'p1_delta': 0, 'p2_delta': 0}
        p1_stats = self.get_rating(p1_name)
        p2_stats = self.get_rating(p2_name)
        r1_old = p1_stats['rating']
        r2_old = p2_stats['rating']
        
        if s1 > s2: 
            res = calculate_match(p1_stats, p2_stats, s1, s2)
            self.players[p1_name] = res['winner']
            self.players[p2_name] = res['loser']
        else: 
            res = calculate_match(p2_stats, p1_stats, s2, s1)
            self.players[p2_name] = res['winner']
            self.players[p1_name] = res['loser']
            
        return {
            'p1_delta': self.players[p1_name]['rating'] - r1_old,
            'p2_delta': self.players[p2_name]['rating'] - r2_old
        }

class ThunderData:
    def __init__(self):
        self.scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        self.client = None
        self.sheet_results = None; self.db = None 
        self.rating_engine = RatingEngine()
        self.all_players = {}; self.season_stats = {}; self.seasons_list = ["Career"] 
        self.divisions_list = set(); self.date_lookup = {}; self.weekly_matches = {} 
        self.player_ids = {}; self.id_to_name = {}; self.alias_map = {}; self.date_to_week_map = {} 
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

    def record_page_view(self, ip_address):
        if not self.db: return
        try:
            today_str = datetime.datetime.now().strftime("%Y-%m-%d")
            doc_ref = self.db.collection('daily_traffic').document(today_str)
            doc_ref.set({'date': today_str, 'views': firestore.Increment(1), 'ips': firestore.ArrayUnion([ip_address])}, merge=True)
        except: pass

    def get_traffic_stats(self):
        if not self.db: return {'views': 0, 'uniques': 0}
        try:
            today_str = datetime.datetime.now().strftime("%Y-%m-%d")
            doc = self.db.collection('daily_traffic').document(today_str).get()
            if doc.exists: data = doc.to_dict(); return {'views': data.get('views', 0), 'uniques': len(data.get('ips', []))}
            return {'views': 0, 'uniques': 0}
        except: return {'views': 0, 'uniques': 0}
        
    def _ensure_config_exists(self):
        if not self.db: return
        doc_ref = self.db.collection('system_config').document('main')
        if not doc_ref.get().exists: doc_ref.set({'tournament_mode_active': False})

    def verify_admin_token(self, token):
        if not self.db: return None
        try:
            decoded_token = auth.verify_id_token(token); email = decoded_token.get('email')
            if not email: return None
            if email.lower() == SUPER_ADMIN_EMAIL.lower():
                self.db.collection('admin_users').document(email.lower()).set({'email': email.lower(), 'role': 'super_admin'}, merge=True)
                return {'email': email.lower(), 'role': 'super_admin'}
            doc = self.db.collection('admin_users').document(email.lower()).get()
            if doc.exists: return {'email': email.lower(), 'role': doc.to_dict().get('role', 'pending')}
            else:
                self.db.collection('admin_users').document(email.lower()).set({'email': email.lower(), 'role': 'pending'})
                return {'email': email.lower(), 'role': 'pending'}
        except Exception as e: return None

    def get_admin_users(self):
        if not self.db: return []
        try: docs = self.db.collection('admin_users').stream(); return [d.to_dict() for d in docs]
        except: return []

    def approve_admin(self, email, action):
        if not self.db: return False
        try:
            ref = self.db.collection('admin_users').document(email)
            if action == 'approve': ref.update({'role': 'admin'})
            elif action == 'revoke': ref.update({'role': 'pending'})
            elif action == 'delete': ref.delete()
            return True
        except: return False

    def _log_audit(self, admin_email, action_type, description, undo_payload):
        if not self.db: return
        try: self.db.collection('admin_audit_logs').add({'admin': admin_email, 'action': action_type, 'description': description, 'undo_payload': json.dumps(undo_payload), 'timestamp': firestore.SERVER_TIMESTAMP, 'status': 'active'})
        except: pass

    def get_audit_logs(self):
        if not self.db: return []
        try:
            docs = self.db.collection('admin_audit_logs').order_by('timestamp', direction=firestore.Query.DESCENDING).limit(100).stream()
            logs = []
            aest_tz = datetime.timezone(datetime.timedelta(hours=10))
            for d in docs:
                data = d.to_dict(); data['id'] = d.id; ts = data.get('timestamp')
                data['time_str'] = ts.astimezone(aest_tz).strftime("%d/%m/%Y %I:%M:%S %p") if ts else "Unknown Time"
                data['timestamp'] = str(ts); logs.append(data)
            return logs
        except: return []

    def undo_audit_action(self, log_id, super_admin_email):
        if not self.db: return False
        try:
            doc_ref = self.db.collection('admin_audit_logs').document(log_id)
            doc = doc_ref.get()
            if not doc.exists: return False
            data = doc.to_dict()
            if data.get('status') == 'undone': return False
            
            action = data.get('action'); payload = json.loads(data.get('undo_payload', '{}'))
            
            if action == 'OVERRIDE_RATING':
                name = payload.get('name'); safe_id = re.sub(r'[^a-zA-Z0-9]', '_', name).lower()
                self.db.collection('rating_overrides').document(safe_id).delete()
            elif action == 'UPDATE_MATCH': self.db.collection('match_corrections').document(payload.get('correction_id')).delete()
            elif action == 'FORCE_FINISH_LIVE':
                self.db.collection('match_results').document(payload.get('result_id')).delete()
                if payload.get('schedule_id') and payload.get('fixture_data'): self.db.collection('fixture_schedule').document(payload.get('schedule_id')).set(payload.get('fixture_data'))
            elif action == 'WIPE_LIVE':
                if payload.get('schedule_id') and payload.get('fixture_data'): self.db.collection('fixture_schedule').document(payload.get('schedule_id')).set(payload.get('fixture_data'))
            elif action == 'BULK_DATE_FIX':
                if 'rule_id' in payload: self.db.collection('bulk_date_rules').document(payload['rule_id']).delete()

            doc_ref.update({'status': 'undone', 'undone_by': super_admin_email})
            self.refresh_data(); return True
        except: return False

    def _clean_name(self, name): 
        if not name: return ""
        clean = " ".join(str(name).split()); return self.alias_map.get(clean.lower(), clean.title())

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

    def _generate_player_id(self): return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6))
    def _slugify(self, text): return re.sub(r'[^a-z0-9]', '', str(text).lower())
    def _extract_week(self, text): match = re.search(r'\d+', str(text)); return match.group() if match else str(text).strip().lower()

    def _load_calculated_dates(self):
        self.date_lookup = {}; self.date_to_week_map = {}
        try:
            if self.sheet_results:
                records = self.sheet_results.worksheet("Calculated_Dates").get_all_records()
                for row in records:
                    s = self._slugify(row.get('Season', '')); d = self._slugify(row.get('Division', '')); w = self._extract_week(row.get('Week', ''))
                    raw_date = str(row.get('Date','')); parsed = self._parse_date(raw_date)
                    if parsed: 
                        self.date_lookup[f"{s}|{d}|{w}"] = parsed
                        self.date_to_week_map[f"{s}|{parsed.strftime('%Y-%m-%d')}"] = w
            if self.db:
                rules = self.db.collection('bulk_date_rules').stream()
                for r in rules:
                    d_rule = r.to_dict(); s = self._slugify(d_rule.get('season', '')); div = self._slugify(d_rule.get('division', '')); w = self._extract_week(d_rule.get('week', ''))
                    parsed = self._parse_date(d_rule.get('date'))
                    if parsed: self.date_lookup[f"{s}|{div}|{w}"] = parsed
        except: pass

    def admin_bulk_fix_date(self, season, division, week, new_date, admin_email="Unknown"):
        if not self.db: return False
        try:
            rule_id = f"{self._slugify(season)}_{self._slugify(division)}_{self._extract_week(week)}"
            self.db.collection('bulk_date_rules').document(rule_id).set({'season': season, 'division': division, 'week': week, 'date': new_date, 'author': admin_email, 'timestamp': firestore.SERVER_TIMESTAMP})
            self._log_audit(admin_email, 'BULK_DATE_FIX', f"Smart-mapped all missing matches for {season} {division} Week {week} to {new_date}.", {"rule_id": rule_id})
            self.refresh_data(); return True
        except: return False

    def _load_aliases(self):
        self.alias_map = {}
        try:
            for row in self.sheet_results.worksheet("Aliases").get_all_records():
                bad = str(row.get('Bad Name')).strip().lower(); good = str(row.get('Good Name')).strip()
                if bad and good: self.alias_map[bad] = good
        except: pass

    def _load_seed_ratings(self):
        try:
            ws = self.sheet_results.worksheet("Ratings base"); rows = ws.get_all_values()
            if not rows: return
            headers = [str(h).lower().strip() for h in rows[0]]; p_idx = headers.index('player') if 'player' in headers else 0
            for row in rows[1:]:
                if len(row) <= p_idx: continue
                name = self._clean_name(row[p_idx])
                if not name: continue
                if name not in self.all_players: self.all_players[name] = {'regular': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'fillin': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'combined': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}}
        except: pass

    def _update_master_roster(self):
        if not self.sheet_results: return
        try:
            self.player_ids = {}; self.id_to_name = {}
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
                clean_n = self._clean_name(p_name); p_id = str(row[id_col]).strip() if len(row) > id_col else ""
                if clean_n:
                    if not p_id: new_id = self._generate_player_id(); updates.append({'range': f"{chr(65+id_col)}{i}", 'values': [[new_id]]}); p_id = new_id
                    self.player_ids[clean_n] = p_id; existing_names[clean_n.lower()] = True
            active_players = set(self.all_players.keys()); new_rows = []; today_str = datetime.date.today().strftime("%Y-%m-%d")
            for p_name in active_players:
                if p_name.lower() not in existing_names: new_id = self._generate_player_id(); new_rows.append([p_name, new_id, today_str, "Active"]); self.player_ids[p_name] = new_id
            self.id_to_name = {v: k for k, v in self.player_ids.items()}
            if updates: ws.batch_update(updates)
            if new_rows: ws.append_rows(new_rows)
        except: pass

    def admin_get_player_directory(self):
        return [{"name": name, "id": pid, "label": f"{name} (ID: {pid})"} for name, pid in self.player_ids.items()]

    def _sync_to_firebase(self):
        if not self.db: return
        try:
            batch = self.db.batch(); batch_count = 0
            for player_name, stats in self.all_players.items():
                rat = self.rating_engine.get_rating(player_name); safe_id = re.sub(r'[^a-zA-Z0-9]', '_', player_name).lower()
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
            raw_headers = all_values[0]; clean_headers = []; counts = {'name': 0, 'sets': 0, 'ps': 0, 'player': 0}
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
        final_matches = []; groups = {}
        for m in matches:
            p1 = m['p1'].lower().strip(); p2 = m['p2'].lower().strip()
            if not p1 or not p2: continue
            players = sorted([p1, p2]); date_key = m['date'].strftime("%Y%m%d") if m['date'] else "nodate"
            match_key = f"{date_key}_{players[0]}_{players[1]}"
            if match_key not in groups: groups[match_key] = []
            groups[match_key].append(m)
        for key, group in groups.items():
            if len(group) == 1: final_matches.append(group[0]); continue
            firebase_m = [m for m in group if m.get('rich_stats') is not None]
            sheet_m = [m for m in group if m.get('rich_stats') is None]
            merged = []
            for fm in firebase_m:
                merged.append(fm)
                if sheet_m: sheet_m.pop(0) 
            for sm in sheet_m: merged.append(sm)
            final_matches.extend(merged)
        return final_matches

    def _update_player_stats(self, stat_dict, player_name, my_sets, op_sets, is_p1, opponent, is_fillin, division, date_str, week_num, season_name, details="", rich_stats=None, match_id="", delta=0, sheet_name="Unknown", row_index="?"):
        if stat_dict is None: return 
        if player_name not in stat_dict: stat_dict[player_name] = {'regular': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'fillin': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'combined': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}}
        buckets = [stat_dict[player_name]['combined']]
        if is_fillin: buckets.append(stat_dict[player_name]['fillin'])
        else: buckets.append(stat_dict[player_name]['regular'])
        
        result = "Win" if my_sets > op_sets else "Loss"
        
        for stats in buckets:
            stats['matches'] += 1; stats['sets_won'] += my_sets; stats['sets_lost'] += op_sets
            if result == "Win": stats['wins'] += 1
            else: stats['losses'] += 1
            stats['history'].append({'season': season_name, 'week': week_num, 'date': date_str, 'opponent': opponent, 'result': result, 'score': f"{my_sets}-{op_sets}", 'type': 'Fill-in' if is_fillin else 'Regular', 'division': division, 'details': details, 'rich_stats': rich_stats, 'match_id': match_id, 'delta': delta, 'sheet_name': sheet_name, 'row_index': row_index})

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
                    for i, row in enumerate(records):
                        row_vals = [str(v).lower() for v in row.values()]
                        if any("doubles" in v for v in row_vals): continue
                        p1 = self._clean_name(self._get_val(row, ['Name 1', 'Player 1', 'Name'])); p2 = self._clean_name(self._get_val(row, ['Name 2', 'Player 2']))
                        if not p1 or not p2: continue
                        div = str(self._get_val(row, ['Division', 'Div'], 'Unknown')).strip(); self.divisions_list.add(div)
                        p1_fill = "S" in str(self._get_val(row, ['PS 1', 'Pos 1', 'Pos'])).upper(); p2_fill = "S" in str(self._get_val(row, ['PS 2', 'Pos 2'])).upper()
                        round_val = self._get_val(row, ['Round', 'Rd', 'Week']); week_num = self._extract_week(round_val) if round_val else "unknown"
                        raw_date = self._get_val(row, ['Date', 'Match Date']); parsed_date = self._parse_date(raw_date)
                        if (not parsed_date) and str(week_num) != "unknown": 
                            lookup_key = f"{self._slugify(season_name)}|{self._slugify(div)}|{week_num}"
                            parsed_date = self.date_lookup.get(lookup_key)
                        if not parsed_date: parsed_date = datetime.date(1900, 1, 1)
                        try: s1 = int(self._get_val(row, ['Sets 1', 'S1', 'Sets'])); s2 = int(self._get_val(row, ['Sets 2', 'S2']))
                        except: continue
                        raw_match_queue.append({'p1': p1, 'p2': p2, 's1': s1, 's2': s2, 'date': parsed_date, 'season': season_name, 'week': week_num, 'div': div, 'p1_fill': p1_fill, 'p2_fill': p2_fill, 'game_history': '', 'rich_stats': None, 'manual_override': False, 'sheet_name': worksheet.title, 'row_index': str(i + 2)})
                except: pass
                
        if self.db:
            try:
                docs = self.db.collection('match_results').stream()
                for doc in docs:
                    d = doc.to_dict()
                    if d.get('status') == 'pending' or d.get('status') == 'rejected': continue
                    date_val = d.get('date'); parsed_date = self._parse_date(date_val) or datetime.date.today()
                    raw_season = str(d.get('season', f"Season: {parsed_date.year}")); season_name = re.sub(r'(?i)^season\s*:\s*', '', raw_season).strip()
                    if season_name not in self.seasons_list and season_name != "Unknown": self.seasons_list.append(season_name)
                    if season_name not in self.season_stats: self.season_stats[season_name] = {}
                    if season_name not in self.weekly_matches: self.weekly_matches[season_name] = {}
                    home_p = d.get('home_players', []); away_p = d.get('away_players', [])
                    if not home_p or not away_p: continue
                    p1 = self._clean_name(home_p[0]); p2 = self._clean_name(away_p[0]); s1 = d.get('live_home_sets', 0); s2 = d.get('live_away_sets', 0)
                    if s1 == 0 and s2 == 0 and d.get('game_scores_history'):
                        t1=0; t2=0
                        for s in str(d.get('game_scores_history')).split(','):
                            try: 
                                if int(s.split('-')[0]) > int(s.split('-')[1]): t1+=1
                                else: t2+=1
                            except: pass
                        s1=t1; s2=t2
                    week_val = self._extract_week(d.get('week', 'Unknown'))
                    if str(week_val) == "unknown" and parsed_date: 
                        key = f"{self._slugify(season_name)}|{parsed_date.strftime('%Y-%m-%d')}"
                        week_val = self.date_to_week_map.get(key, "unknown")
                    rich = d.get('richStats', {})
                    rich['total_duration'] = d.get('total_duration', '00:00'); rich['play_duration'] = d.get('play_duration', '00:00'); rich['set_scores'] = d.get('set_scores', [])
                    raw_match_queue.append({'p1': p1, 'p2': p2, 's1': s1, 's2': s2, 'date': parsed_date, 'season': season_name, 'week': week_val, 'div': d.get('division', 'Unknown'), 'p1_fill': False, 'p2_fill': False, 'game_history': d.get('game_scores_history', ''), 'rich_stats': rich, 'manual_override': d.get('manual_override', False), 'sheet_name': 'Live Match Data', 'row_index': 'Firebase'})
            except: pass

        logger.info(f"📊 FINAL LOADED SEASONS LIST: {self.seasons_list}")

        corrections = {}
        if self.db:
            try:
                c_docs = self.db.collection('match_corrections').stream()
                for doc in c_docs:
                    c = doc.to_dict(); c_p1 = self._clean_name(c.get('p1', '')); c_p2 = self._clean_name(c.get('p2', '')); d_str = str(c.get('date', ''))
                    players = sorted([c_p1, c_p2]); key = f"{d_str}_{players[0]}_{players[1]}"
                    corrections[key] = {'s1': c.get('s1', 0), 's2': c.get('s2', 0), 'c_p1': c_p1, 'new_date': c.get('new_date')}
            except Exception as e: pass

        for m in raw_match_queue:
            players = sorted([m['p1'], m['p2']]); d_str = m['date'].strftime("%d/%m/%Y") if m['date'] else "nodate"; key = f"{d_str}_{players[0]}_{players[1]}"
            if key in corrections:
                c = corrections[key]
                if m['p1'] == c['c_p1']: m['s1'] = c['s1']; m['s2'] = c['s2']
                else: m['s1'] = c['s2']; m['s2'] = c['s1']
                m['manual_override'] = True
                if c.get('new_date'):
                    parsed_new = self._parse_date(c['new_date'])
                    if parsed_new: m['date'] = parsed_new

        cleaned_matches = self._deduplicate_matches(raw_match_queue); cleaned_matches.sort(key=lambda x: x['date'])
        
        overrides_dict = {}
        if self.db:
            try:
                for doc in self.db.collection('rating_overrides').stream():
                    d = doc.to_dict()
                    overrides_dict[d.get('name')] = {
                        'rating': float(d.get('rating', 1500)), 
                        'rd': float(d.get('rd', 75.0)), 
                        'vol': float(d.get('vol', 0.06)),
                        'date': d.get('date_str', '1900-01-01')
                    }
            except Exception as e: pass

        player_set = set(); player_overrides_applied = set()

        for m in cleaned_matches:
            players = sorted([m['p1'], m['p2']]); d_str = m['date'].strftime("%d/%m/%Y") if m['date'] else "nodate"; d_str_fmt = m['date'].strftime("%Y-%m-%d") if m['date'] else "1900-01-01"
            raw_id_string = f"{d_str}_{players[0]}_{players[1]}_{m['s1']}_{m['s2']}"; match_id = hashlib.md5(raw_id_string.encode()).hexdigest()[:6].upper()
            for p in [m['p1'], m['p2']]:
                if p in overrides_dict and p not in player_overrides_applied:
                    if d_str_fmt >= overrides_dict[p]['date']:
                        if p not in self.rating_engine.players: self.rating_engine.get_rating(p)
                        self.rating_engine.players[p]['rating'] = overrides_dict[p]['rating']
                        self.rating_engine.players[p]['rd'] = overrides_dict[p]['rd']
                        self.rating_engine.players[p]['vol'] = overrides_dict[p]['vol']
                        player_overrides_applied.add(p)

            p1_delta = 0; p2_delta = 0
            if m['date'] > RATING_START_DATE: 
                deltas = self.rating_engine.update_match(m['p1'], m['p2'], m['s1'], m['s2']); p1_delta = deltas.get('p1_delta', 0); p2_delta = deltas.get('p2_delta', 0)
            
            for p, sets_for, sets_against, is_p1, opp, fill, delta in [(m['p1'], m['s1'], m['s2'], True, m['p2'], m['p1_fill'], p1_delta), (m['p2'], m['s2'], m['s1'], False, m['p1'], m['p2_fill'], p2_delta)]:
                self._update_player_stats(self.season_stats.get(m['season'], {}), p, sets_for, sets_against, is_p1, opp, fill, m['div'], d_str, m['week'], m['season'], m.get('game_history', ''), m.get('rich_stats'), match_id, delta, m.get('sheet_name', 'Unknown'), m.get('row_index', '?'))
                self._update_player_stats(self.all_players, p, sets_for, sets_against, is_p1, opp, fill, m['div'], d_str, m['week'], m['season'], m.get('game_history', ''), m.get('rich_stats'), match_id, delta, m.get('sheet_name', 'Unknown'), m.get('row_index', '?'))
                player_set.add(p)

            if str(m['week']) != "unknown":
                if m['season'] not in self.weekly_matches: self.weekly_matches[m['season']] = {}
                wk = str(m['week']); 
                if wk not in self.weekly_matches[m['season']]: self.weekly_matches[m['season']][wk] = []
                self.weekly_matches[m['season']][wk].append({'p1': m['p1'], 'p2': m['p2'], 'score': f"{m['s1']}-{m['s2']}", 'division': m['div'], 'date': d_str})
        
        for p, over in overrides_dict.items():
            if p not in player_overrides_applied:
                if p in self.rating_engine.players: 
                    self.rating_engine.players[p]['rating'] = over['rating']
                    self.rating_engine.players[p]['rd'] = over['rd']
                    self.rating_engine.players[p]['vol'] = over['vol']

        logger.info(f"👤 Total Unique Players Loaded: {len(player_set)}")
        self._update_master_roster(); self._sync_to_firebase()

    def get_matches_by_week(self, season, week): return self.weekly_matches.get(season, {}).get(str(week), [])
    def get_all_players(self): return self.all_players
    def get_seasons(self): return self.seasons_list
    def get_divisions(self): return sorted(list(self.divisions_list))
    
    def get_roster_with_meta(self):
        roster = []
        for name, data in self.all_players.items():
            rat = self.rating_engine.get_rating(name); c = data['combined']; m = c['matches']; divs = {}; 
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
            def calc_summary(history): wins = sum(1 for m in history if m['result'] == "Win"); return {'wins': wins, 'losses': len(history) - wins, 'matches': len(history)}
            ranking_list.append({'name': player_name, 'rating_val': int(rat['rating']), 'sigma': int(rat['rd']), 'vol': rat.get('vol', 0.06), 'regular': calc_summary(reg_hist), 'fillin': calc_summary(fill_hist)})
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
        return {'name': player_name, 'rating': int(rat['rating']), 'rd': int(rat['rd']), 'vol': rat.get('vol', 0.06), 'combined': format_bucket(raw['combined']), 'peterman_id': peterman_id}

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

    def user_submit_report(self, match_id, p1, p2, date, reporter, problem, suggested_home, suggested_away):
        if not self.db: return False
        try:
            self.db.collection('match_reports').add({'match_id': match_id, 'p1': p1, 'p2': p2, 'date': date, 'reporter': reporter, 'problem': problem, 'suggested_s1': suggested_home, 'suggested_s2': suggested_away, 'timestamp': firestore.SERVER_TIMESTAMP, 'status': 'Pending'})
            return True
        except Exception as e: return False

    def admin_get_ranks(self):
        if not self.db: return {}
        try: docs = self.db.collection('player_ranks').stream(); return {d.to_dict().get('name'): d.to_dict().get('rank') for d in docs}
        except: return {}

    def admin_set_rank(self, player_name, rank):
        if not self.db: return False
        try: safe_id = re.sub(r'[^a-zA-Z0-9]', '_', player_name).lower(); self.db.collection('player_ranks').document(safe_id).set({'name': player_name, 'rank': int(rank)}); return True
        except: return False

    def admin_resolve_report(self, doc_id, action, admin_email="Unknown"):
        if not self.db: return False
        try:
            new_status = 'Resolved' if action == 'approve' else 'Dismissed'
            doc_ref = self.db.collection('match_reports').document(doc_id)
            if doc_ref.get().exists: doc_ref.update({'status': new_status}); self._log_audit(admin_email, 'RESOLVE_INBOX', f"Marked Match Report {doc_id} as {new_status}.", {}); return True
            fb_ref = self.db.collection('feedback').document(doc_id)
            if fb_ref.get().exists: fb_ref.update({'status': new_status}); self._log_audit(admin_email, 'RESOLVE_INBOX', f"Marked Feedback {doc_id} as {new_status}.", {}); return True
            return False
        except: return False
        
    def admin_get_reports(self):
        if not self.db: return []
        try: 
            reports = []; m_docs = self.db.collection('match_reports').stream()
            aest_tz = datetime.timezone(datetime.timedelta(hours=10))
            for d in m_docs:
                data = d.to_dict()
                if data.get('status', '').lower() == 'pending': 
                    ts = data.get('timestamp')
                    date_str = ts.astimezone(aest_tz).strftime('%d/%m/%Y') if hasattr(ts, 'astimezone') else data.get('date', 'Unknown Date')
                    reports.append({'id': d.id, 'type': 'MATCH_ERROR', 'reporter': data.get('reporter', 'Unknown'), 'date': date_str, 'title': f"{data.get('p1', '')} vs {data.get('p2', '')}", 'problem': data.get('problem', ''), 'suggested_s1': data.get('suggested_s1', ''), 'suggested_s2': data.get('suggested_s2', ''), 'match_id': data.get('match_id', ''), 'timestamp': ts})
            f_docs = self.db.collection('feedback').stream()
            for d in f_docs:
                data = d.to_dict()
                if data.get('status', '').lower() == 'new':
                    ts = data.get('timestamp')
                    date_str = ts.astimezone(aest_tz).strftime('%d/%m/%Y') if hasattr(ts, 'astimezone') else 'Recent'
                    reports.append({'id': d.id, 'type': 'FEEDBACK', 'reporter': data.get('contact', 'Anonymous'), 'date': date_str, 'title': f"Feedback: {data.get('type', 'General')}", 'problem': f"Context: {data.get('context', '')}\n\nMessage: {data.get('message', '')}", 'suggested_s1': '', 'suggested_s2': '', 'match_id': '', 'timestamp': ts})
            def safe_ts(x):
                ts = x.get('timestamp')
                if hasattr(ts, 'timestamp'): return ts.timestamp()
                if isinstance(ts, (int, float)): return ts
                return 0
            reports.sort(key=safe_ts, reverse=True)
            for r in reports:
                if 'timestamp' in r: r['timestamp'] = str(r['timestamp']) 
            return reports
        except: return []

    def admin_get_date_errors(self):
        results = []; seen = set()
        for p_name, data in self.all_players.items():
            for m in data.get('combined', {}).get('history', []):
                date_str = str(m.get('date', ''))
                if date_str == '01/01/1900' or date_str == '1900-01-01':
                    opp = str(m.get('opponent', '')); m_season = str(m.get('season', '')); m_div = str(m.get('division', '')); m_week = str(m.get('week', '')); match_id = str(m.get('match_id', ''))
                    players = sorted([p_name, opp]); match_hash = f"{date_str}_{m_season}_{m_week}_{players[0]}_{players[1]}"
                    if match_hash not in seen:
                        seen.add(match_hash); score = m.get('score', '0-0')
                        try: s1, s2 = map(int, score.split('-'))
                        except: s1, s2 = 0, 0
                        results.append({'id': match_hash, 'date': date_str, 'division': m_div, 'season': m_season, 'week': m_week, 'home_players': [p_name], 'away_players': [opp], 'score': score, 's1': s1, 's2': s2, 'match_id': match_id, 'sheet_name': m.get('sheet_name', 'Unknown'), 'row_index': m.get('row_index', '?')})
        return results

    def admin_search_history(self, query_text, season_filter=None, div_filter=None, week_filter=None, date_filter=None):
        q_lower = query_text.lower() if query_text else ""; results = []; seen = set()
        for p_name, data in self.all_players.items():
            for m in data.get('combined', {}).get('history', []):
                date = str(m.get('date', '')); opp = str(m.get('opponent', '')); m_season = str(m.get('season', '')); m_div = str(m.get('division', '')); m_week = str(m.get('week', '')); match_id = str(m.get('match_id', ''))
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
        results.sort(key=sort_key, reverse=True); return results[:150]

    def admin_get_pending_approvals(self):
        if not self.db: return []
        try:
            docs = self.db.collection('match_results').where('status', '==', 'pending').stream(); res = []
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
            res.sort(key=lambda x: x.get('ts_sort', 0), reverse=True); return res[:50]
        except: return []

    def admin_delete_match_result(self, doc_id):
        if not self.db: return False
        try: self.db.collection('match_results').document(doc_id).delete(); self.refresh_data(); return True
        except: return False

    def admin_update_historical_match(self, p1, p2, date, s1, s2, new_date=None, admin_email="Unknown"):
        if not self.db: return False
        try: 
            payload = {'p1': p1, 'p2': p2, 'date': date, 's1': int(s1), 's2': int(s2), 'timestamp': firestore.SERVER_TIMESTAMP}
            if new_date and new_date != date: payload['new_date'] = new_date
            res = self.db.collection('match_corrections').add(payload)
            self._log_audit(admin_email, 'UPDATE_MATCH', f"Overrode score for {p1} vs {p2} on {date} to {s1}-{s2}.", {"correction_id": res[1].id})
            self.refresh_data(); return True
        except: return False

    def admin_merge_players(self, bad_name, good_name, admin_email="Unknown"):
        if not self.db: return False
        batch = self.db.batch(); count = 0
        self._log_audit(admin_email, 'MERGE_PLAYER', f"Permanently merged profile '{bad_name}' into '{good_name}'.", {})
        home_games = self.db.collection('match_results').where('home_players', 'array_contains', bad_name).stream()
        for doc in home_games: d = doc.to_dict(); new_home = [good_name if p == bad_name else p for p in d.get('home_players', [])]; batch.update(doc.reference, {'home_players': new_home, 'home_team': good_name}); count += 1
        away_games = self.db.collection('match_results').where('away_players', 'array_contains', bad_name).stream()
        for doc in away_games: d = doc.to_dict(); new_away = [good_name if p == bad_name else p for p in d.get('away_players', [])]; batch.update(doc.reference, {'away_players': new_away, 'away_team': good_name}); count += 1
        safe_id_bad = re.sub(r'[^a-zA-Z0-9]', '_', bad_name).lower(); batch.delete(self.db.collection('players').document(safe_id_bad)); batch.delete(self.db.collection('player_profiles').document(safe_id_bad))
        if count > 0: batch.commit()
        if self.sheet_results:
            try: self.sheet_results.worksheet("Aliases").append_row([bad_name, good_name])
            except: pass
        self.refresh_data(); return True

    def admin_override_rating(self, player_id, new_rating, retroactive=True, admin_email="Unknown"):
        if not self.db: return False
        try:
            player_name = self.id_to_name.get(player_id)
            if not player_name: return False
            safe_id = re.sub(r'[^a-zA-Z0-9]', '_', player_name).lower(); date_to_use = "1900-01-01" if retroactive else datetime.datetime.now().strftime("%Y-%m-%d")
            self.db.collection('rating_overrides').document(safe_id).set({'name': player_name, 'rating': float(new_rating), 'rd': 75.0, 'vol': 0.06, 'date_str': date_to_use, 'timestamp': firestore.SERVER_TIMESTAMP}, merge=True)
            self._log_audit(admin_email, 'OVERRIDE_RATING', f"Set {player_name}'s seed rating to {new_rating} (Retroactive: {retroactive}).", {"name": player_name})
            self.refresh_data(); return True
        except: return False
        
    def admin_bulk_pull_ratings(self, admin_email="Unknown"):
        if not self.db or not self.sheet_results: return {"success": False, "error": "Database or Sheet offline"}
        try:
            ws = self.sheet_results.worksheet("Ratings crap")
            all_values = ws.get_all_values()
            
            total_count = 0
            batch_count = 0
            batch = self.db.batch()
            today_str = datetime.datetime.now().strftime("%Y-%m-%d")
            
            for row in all_values[1:]: 
                if len(row) >= 2:
                    name = self._clean_name(row[0])
                    rating_str = str(row[1]).strip() 
                    
                    if not name or not rating_str: continue
                    try:
                        rating_val = float(rating_str)
                    except ValueError:
                        continue 
                    
                    safe_id = re.sub(r'[^a-zA-Z0-9]', '_', name).lower()
                    
                    override_ref = self.db.collection('rating_overrides').document(safe_id)
                    batch.set(override_ref, {
                        'name': name,
                        'rating': rating_val,
                        'date_str': today_str,
                        'rd': 140.0,
                        'vol': 0.06,
                        'timestamp': firestore.SERVER_TIMESTAMP
                    }, merge=True)
                    
                    total_count += 1
                    batch_count += 1
                    if batch_count >= 400: 
                        batch.commit()
                        batch = self.db.batch()
                        batch_count = 0
            
            if batch_count > 0:
                batch.commit()
                
            self._log_audit(admin_email, 'BULK_IMPORT_RATINGS', f"Pulled {total_count} ratings from 'Ratings crap' sheet as of {today_str}.", {})
            
            return {"success": True, "count": total_count}
            
        except Exception as e:
            return {"success": False, "error": str(e)}
            
    def admin_force_finish_live(self, schedule_id, s1, s2, admin_email="Unknown"):
        if not self.db: return False
        try:
            doc_ref = self.db.collection('fixture_schedule').document(schedule_id); doc_snap = doc_ref.get()
            if not doc_snap.exists: return False
            data = doc_snap.to_dict()
            res = self.db.collection('match_results').add({
                'home_players': data.get('home_players', []), 'away_players': data.get('away_players', []),
                'home_team': data.get('home_team', ''), 'away_team': data.get('away_team', ''),
                'division': data.get('division', 'Unknown'), 'season': data.get('season', 'Unknown'),
                'date': data.get('date', datetime.datetime.now().strftime("%d/%m/%Y")), 'week': data.get('week', 'Unknown'),
                'live_home_sets': int(s1), 'live_away_sets': int(s2), 'game_scores_history': data.get('game_scores_history', ''),
                'richStats': data.get('richStats', {}), 'timestamp': firestore.SERVER_TIMESTAMP, 'status': 'approved'
            })
            doc_ref.update({'match_status': 'Scheduled', 'live_home_score': 0, 'live_away_score': 0, 'live_home_sets': 0, 'live_away_sets': 0, 'current_server': '', 'game_scores_history': '', 'momentum': '', 'richStats': None, 'serve_stats': None})
            self._log_audit(admin_email, 'FORCE_FINISH_LIVE', f"Force submitted live match {schedule_id} to history.", {"result_id": res[1].id, "schedule_id": schedule_id, "fixture_data": data})
            self.refresh_data(); return True
        except: return False

    def admin_wipe_live(self, schedule_id, admin_email="Unknown"):
        if not self.db: return False
        try:
            doc_ref = self.db.collection('fixture_schedule').document(schedule_id); data = doc_ref.get().to_dict()
            doc_ref.update({'match_status': 'Scheduled', 'live_home_score': 0, 'live_away_score': 0, 'live_home_sets': 0, 'live_away_sets': 0, 'current_server': '', 'game_scores_history': '', 'momentum': '', 'richStats': None, 'serve_stats': None})
            self._log_audit(admin_email, 'WIPE_LIVE', f"Wiped ghost match data for {schedule_id}.", {"schedule_id": schedule_id, "fixture_data": data}); return True
        except: return False

    def record_donation(self, intent_id, name, amount):
        if not self.db: return False
        try:
            doc_ref = self.db.collection('donations').document(intent_id)
            if doc_ref.get().exists: return True 
            doc_ref.set({'name': name if name and name.strip() else 'Anonymous', 'amount': float(amount), 'timestamp': firestore.SERVER_TIMESTAMP, 'month': datetime.datetime.now().strftime("%Y-%m")}); return True
        except: return False

    def get_top_donors(self, limit=5):
        if not self.db: return []
        try:
            docs = self.db.collection('donations').where('month', '==', datetime.datetime.now().strftime("%Y-%m")).stream()
            grouped_donors = {}; donors = []
            for d in docs:
                data = d.to_dict(); n = data.get('name', 'Anonymous').strip()
                if not n: n = 'Anonymous'
                amt = float(data.get('amount', 0))
                if n.lower() == 'anonymous': donors.append({'name': 'Anonymous', 'amount': amt})
                else: grouped_donors[n] = grouped_donors.get(n, 0) + amt
            for k, v in grouped_donors.items(): donors.append({'name': k, 'amount': v})
            donors.sort(key=lambda x: x['amount'], reverse=True); return donors[:limit]
        except: return []

    def get_notices(self):
        if not self.db: return []
        try:
            docs = self.db.collection('notices').order_by('timestamp', direction=firestore.Query.DESCENDING).stream()
            res = []
            aest_tz = datetime.timezone(datetime.timedelta(hours=10))
            for d in docs:
                data = d.to_dict(); data['id'] = d.id; ts = data.get('timestamp')
                data['date_str'] = ts.astimezone(aest_tz).strftime('%d/%m/%Y %I:%M %p') if ts else 'Recent'; data['timestamp'] = str(ts)
                res.append(data)
            return res
        except: return []

    def admin_add_notice(self, title, message, notice_type, admin_email="Unknown"):
        if not self.db: return False
        try:
            self.db.collection('notices').add({'title': title, 'message': message, 'type': notice_type, 'timestamp': firestore.SERVER_TIMESTAMP, 'author': admin_email})
            self._log_audit(admin_email, 'ADD_NOTICE', f"Posted notice: {title}", {}); return True
        except: return False

    def admin_delete_notice(self, notice_id, admin_email="Unknown"):
        if not self.db: return False
        try: self.db.collection('notices').document(notice_id).delete(); self._log_audit(admin_email, 'DELETE_NOTICE', f"Deleted notice ID: {notice_id}", {}); return True
        except: return False

    def admin_set_fixture_format(self, fixture_id, format_type, admin_email="Unknown"):
        if not self.db: return False
        try:
            self.db.collection('fixture_schedule').document(fixture_id).update({'format_override': format_type if format_type in ['2v2', '3v3'] else None})
            self._log_audit(admin_email, 'SET_FORMAT', f"Changed format override for fixture {fixture_id} to {format_type}", {}); return True
        except: return False

    def get_admin_messages(self):
        if not self.db: return []
        try:
            docs = self.db.collection('admin_messages').order_by('timestamp', direction=firestore.Query.DESCENDING).limit(50).stream(); res = []
            aest_tz = datetime.timezone(datetime.timedelta(hours=10))
            for d in docs:
                data = d.to_dict(); data['id'] = d.id; ts = data.get('timestamp')
                data['time_str'] = ts.astimezone(aest_tz).strftime('%d/%m/%Y %I:%M %p') if ts else 'Just now'; data['timestamp'] = str(ts); res.append(data)
            return res
        except: return []

    def add_admin_message(self, message, admin_email):
        if not self.db: return False
        try: self.db.collection('admin_messages').add({'message': message, 'author': admin_email, 'timestamp': firestore.SERVER_TIMESTAMP}); return True
        except: return False

    def get_contact_lists(self):
        if not self.sheet_results: return {"emails": "", "phones": "", "preview": [], "error": "No sheet connection"}
        try:
            ws = self.sheet_results.worksheet("Member info")
            raw_data = ws.get_all_values()
            
            if not raw_data or len(raw_data) < 2:
                return {"emails": "", "phones": "", "preview": [], "error": "Sheet appears to be empty."}
                
            headers = [str(h).lower().strip() for h in raw_data[0]]
            has_send_col = any('send' in h for h in headers)
            
            emails = []; phones = []; preview = []
            
            for row in raw_data[1:]:
                if not any(str(cell).strip() for cell in row): continue
                    
                email_val = ""; phone_val = ""; name_val = "Unknown Player"
                allow_sms = not has_send_col 
                
                for idx, cell_val in enumerate(row):
                    if idx >= len(headers): break
                    col_name = headers[idx]; val_str = str(cell_val).strip()
                    
                    if col_name in ['name', 'player', 'player name', 'full name', 'first name'] and name_val == "Unknown Player":
                        if val_str: name_val = val_str
                    if 'email' in col_name and not email_val: email_val = val_str
                    if ('phone' in col_name or 'mobile' in col_name or 'number' in col_name) and not phone_val: phone_val = val_str
                        
                    if 'send' in col_name:
                        if val_str.lower() in ['yes', 'y', 'true']: allow_sms = True
                        else: allow_sms = False
                            
                if email_val and '@' in email_val: emails.append(email_val)
                
                if phone_val and allow_sms:
                    clean_phone = re.sub(r'[^\d\+\s]', '', phone_val)
                    if len(clean_phone) >= 8: 
                        phones.append(clean_phone)
                        
                        main_div = "Unknown Div"
                        clean_name_key = self._clean_name(name_val)
                        if clean_name_key in self.all_players:
                            div_counts = {}
                            for h in self.all_players[clean_name_key].get('combined', {}).get('history', []):
                                div_counts[h['division']] = div_counts.get(h['division'], 0) + 1
                            if div_counts:
                                main_div = max(div_counts, key=div_counts.get)

                        preview.append({"name": name_val, "phone": clean_phone, "division": main_div})
                    
            return {"emails": ", ".join(list(set(emails))), "phones": ", ".join(list(set(phones))), "preview": preview}
            
        except Exception as e:
            logger.error(f"Error fetching contacts: {e}")
            return {"emails": "", "phones": "", "preview": [], "error": str(e)}

    def admin_send_sms_broadcast(self, message_body, target_phones=None, admin_email="Unknown"):
        contacts = self.get_contact_lists()
        raw_phones = contacts.get("phones", "")
        if not raw_phones: return {"success": False, "error": "No valid opted-in phone numbers found in the Google Sheet."}
        
        allowed_phone_list = [p.strip() for p in raw_phones.split(",") if p.strip()]
        
        if target_phones is not None:
            phone_list = [p for p in target_phones if p in allowed_phone_list]
        else:
            phone_list = allowed_phone_list
            
        if not phone_list: return {"success": False, "error": "No valid opted-in phone numbers matched your selection."}
            
        username = "jakobwill7@gmail.com"
        api_key = "76F26417-8DEB-E47E-8056-B86E519B4445"
        
        clean_body = re.sub(r'[^\x20-\x7E\n\r]+', '', message_body)
        final_message = f"GCTTA Update: {clean_body}\n\nView stats: gctta-stats.com.au\nReply STOP to opt out"
        
        messages = []
        for phone in phone_list:
            messages.append({
                "source": "gctta_admin",
                "from": "GCTTA-STATS",
                "body": final_message,
                "to": phone
            })
            
        payload = json.dumps({"messages": messages}).encode('utf-8')
        
        try:
            auth_str = f"{username}:{api_key}"
            auth_bytes = base64.b64encode(auth_str.encode('utf-8')).decode('utf-8')
            
            req = urllib.request.Request("https://rest.clicksend.com/v3/sms/send", data=payload)
            req.add_header("Content-Type", "application/json")
            req.add_header("Authorization", f"Basic {auth_bytes}")
            
            response = urllib.request.urlopen(req)
            res_data = json.loads(response.read().decode('utf-8'))
            
            if res_data.get('http_code') == 200:
                self._log_audit(admin_email, 'SMS_BROADCAST', f"Sent Mass SMS to {len(phone_list)} selected members via API.", {})
                return {"success": True, "message": f"Successfully sent SMS to {len(phone_list)} selected members!"}
            else:
                return {"success": False, "error": f"ClickSend API Error: {res_data}"}
        except Exception as e:
            logger.error(f"SMS Broadcast Failed: {e}")
            return {"success": False, "error": str(e)}
            
    def admin_glicko_math(self, p1, p2, s1, s2):
        r1 = self.rating_engine.get_rating(p1); r2 = self.rating_engine.get_rating(p2)
        
        dummy_1 = {'rating': r1['rating'], 'rd': r1['rd'], 'vol': 0}
        dummy_2 = {'rating': r2['rating'], 'rd': r2['rd'], 'vol': 0}
        
        E_1 = 1.0 / (1.0 + 10.0 ** ((dummy_2['rating'] - dummy_1['rating']) / 400.0))
        E_2 = 1.0 - E_1
        
        if int(s1) > int(s2):
            res = calculate_match(dummy_1, dummy_2, int(s1), int(s2))
            new_r1 = res['winner']['rating']; new_r2 = res['loser']['rating']
        elif int(s2) > int(s1):
            res = calculate_match(dummy_2, dummy_1, int(s2), int(s1))
            new_r2 = res['winner']['rating']; new_r1 = res['loser']['rating']
        else:
            new_r1 = r1['rating']; new_r2 = r2['rating']
            
        return {
            'p1': {'name': p1, 'old_rating': round(r1['rating'], 1), 'old_rd': round(r1['rd'], 1), 'expected_win_pct': round(E_1 * 100, 1), 'new_rating': round(new_r1, 1), 'delta': round(new_r1 - r1['rating'], 1)},
            'p2': {'name': p2, 'old_rating': round(r2['rating'], 1), 'old_rd': round(r2['rd'], 1), 'expected_win_pct': round(E_2 * 100, 1), 'new_rating': round(new_r2, 1), 'delta': round(new_r2 - r2['rating'], 1)}
        }