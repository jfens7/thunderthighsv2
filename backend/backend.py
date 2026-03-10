import gspread
from google.oauth2.service_account import Credentials
import firebase_admin
from firebase_admin import credentials, firestore, auth
import datetime, re, os, json, sys, logging, random, string, hashlib, urllib.request, base64

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path: sys.path.append(current_dir)

try: from sky_engine import SkyEngine
except ImportError:
    try: from backend.sky_engine import SkyEngine
    except: SkyEngine = None

DEFAULT_RATING = 1000.0
DEFAULT_RD = 300.0  

def calculate_match(w, l, s1, s2, k_win=1.0, k_loss=1.4, anti_riot=True):
    total_sets = s1 + s2
    if total_sets == 0: return {'winner': w, 'loser': l}
    w_score = 0.7 + 0.3 * ((s1 - s2) / total_sets); l_score = 1.0 - w_score
    E_w = 1.0 / (1.0 + 10.0 ** ((l['rating'] - w['rating']) / 400.0)); E_l = 1.0 - E_w
    K_w = max(30.0, w['rd'] * k_win); K_l = max(40.0, l['rd'] * k_loss)
    w_shift = K_w * (w_score - E_w); l_shift = K_l * (l_score - E_l)
    w_rd_shift = -4.0; l_rd_shift = -4.0
    if anti_riot:
        if w_shift < 0: w_shift = 0.0; w_rd_shift = 5.0 
        if l_shift > 0: l_shift = 0.0; l_rd_shift = 2.0 
    w['rating'] += w_shift; l['rating'] += l_shift
    w['rd'] = max(50.0, min(350.0, w['rd'] + w_rd_shift)); l['rd'] = max(50.0, min(350.0, l['rd'] + l_rd_shift))
    return {'winner': w, 'loser': l}

RESULTS_SPREADSHEET_ID = "1tpxuUCl8ddpnBBr69vc4P1foRCRKWpts5-HaFPYb4po" 
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
            self.players[name] = {'rating': r_val, 'rd': rd_val, 'vol': float(vol) if vol is not None else 0.06}
        except ValueError: pass
    def update_match(self, p1_name, p2_name, s1, s2, k_win=1.0, k_loss=1.4, anti_riot=True):
        p1_stats = self.get_rating(p1_name); p2_stats = self.get_rating(p2_name)
        r1_old = p1_stats['rating']; rd1_old = p1_stats['rd']; r2_old = p2_stats['rating']; rd2_old = p2_stats['rd']
        if s1 == s2: return {'p1_delta': 0, 'p2_delta': 0, 'p1_before': r1_old, 'p1_rd_before': rd1_old, 'p1_after': r1_old, 'p1_rd_after': rd1_old, 'p2_before': r2_old, 'p2_rd_before': rd2_old, 'p2_after': r2_old, 'p2_rd_after': rd2_old}
        if s1 > s2: res = calculate_match(p1_stats, p2_stats, s1, s2, k_win, k_loss, anti_riot); self.players[p1_name] = res['winner']; self.players[p2_name] = res['loser']
        else: res = calculate_match(p2_stats, p1_stats, s2, s1, k_win, k_loss, anti_riot); self.players[p2_name] = res['winner']; self.players[p1_name] = res['loser']
        return {'p1_delta': self.players[p1_name]['rating'] - r1_old, 'p2_delta': self.players[p2_name]['rating'] - r2_old, 'p1_before': r1_old, 'p1_rd_before': rd1_old, 'p1_after': self.players[p1_name]['rating'], 'p1_rd_after': self.players[p1_name]['rd'], 'p2_before': r2_old, 'p2_rd_before': rd2_old, 'p2_after': self.players[p2_name]['rating'], 'p2_rd_after': self.players[p2_name]['rd']}

class ThunderData:
    def __init__(self):
        self.scopes = ["https://www.googleapis.com/auth/spreadsheets"]; self.client = None; self.sheet_results = None; self.db = None 
        self.rating_engine = RatingEngine(); self.all_players = {}; self.season_stats = {}; self.seasons_list = ["Career"] 
        self.divisions_list = set(); self.date_lookup = {}; self.weekly_matches = {}; self.player_ids = {}; self.id_to_name = {}; self.alias_map = {}; self.date_to_week_map = {} 
        self.match_history_log = []; self.k_win = 1.0; self.k_loss = 1.4; self.chaos_config = {'active': False, 'weeks': [], 'approvals': [], 'req': 3}
        self._authenticate()

    def _authenticate(self):
        try:
            creds_json = os.environ.get("GOOGLE_CREDS_JSON")
            if creds_json: self.creds = Credentials.from_service_account_info(json.loads(creds_json), scopes=self.scopes)
            else:
                paths = ["credentials.json", "backend/credentials.json"]; found = next((p for p in paths if os.path.exists(p)), None)
                self.creds = Credentials.from_service_account_file(found, scopes=self.scopes) if found else None
            if self.creds:
                self.client = gspread.authorize(self.creds)
                try: self.sheet_results = self.client.open_by_key(RESULTS_SPREADSHEET_ID); logger.info("✅ Connected to Master")
                except: logger.error("❌ Master Sheet Fail")
        except: pass
        try:
            try: app = firebase_admin.get_app()
            except ValueError:
                cred_path = 'firebase_credentials.json'
                if not os.path.exists(cred_path) and os.path.exists('backend/firebase_credentials.json'): cred_path = 'backend/firebase_credentials.json'
                firebase_admin.initialize_app(credentials.Certificate(cred_path))
            self.db = firestore.client()
            if not self.db.collection('system_config').document('main').get().exists: self.db.collection('system_config').document('main').set({'tournament_mode_active': False})
        except: self.db = None

    def record_page_view(self, ip_address):
        if not self.db: return
        try: today_str = datetime.datetime.now().strftime("%Y-%m-%d"); self.db.collection('daily_traffic').document(today_str).set({'date': today_str, 'views': firestore.Increment(1), 'ips': firestore.ArrayUnion([ip_address])}, merge=True)
        except: pass

    def get_traffic_stats(self):
        if not self.db: return {'views': 0, 'uniques': 0}
        try:
            doc = self.db.collection('daily_traffic').document(datetime.datetime.now().strftime("%Y-%m-%d")).get()
            if doc.exists: data = doc.to_dict(); return {'views': data.get('views', 0), 'uniques': len(data.get('ips', []))}
            return {'views': 0, 'uniques': 0}
        except: return {'views': 0, 'uniques': 0}

    def verify_admin_token(self, token):
        if not self.db: return None
        try:
            decoded_token = auth.verify_id_token(token); email = decoded_token.get('email')
            if not email: return None
            if email.lower() == SUPER_ADMIN_EMAIL.lower():
                self.db.collection('admin_users').document(email.lower()).set({'email': email.lower(), 'role': 'super_admin'}, merge=True)
                return {'email': email.lower(), 'role': 'super_admin'}
            doc = self.db.collection('admin_users').document(email.lower()).get()
            if doc.exists: 
                data = doc.to_dict(); role = data.get('role', 'pending'); expires_at = data.get('expires_at')
                if role == 'temp_super_admin' and expires_at and datetime.datetime.now(datetime.timezone.utc) > expires_at:
                    role = 'admin'; self.db.collection('admin_users').document(email.lower()).update({'role': 'admin', 'expires_at': firestore.DELETE_FIELD})
                return {'email': email.lower(), 'role': role}
            else:
                self.db.collection('admin_users').document(email.lower()).set({'email': email.lower(), 'role': 'pending'})
                return {'email': email.lower(), 'role': 'pending'}
        except: return None

    def get_admin_users(self):
        if not self.db: return []
        try: 
            users = []
            for d in self.db.collection('admin_users').stream():
                data = d.to_dict()
                if data.get('expires_at'): data['expires_at_str'] = data['expires_at'].strftime('%Y-%m-%d %H:%M')
                users.append(data)
            return users
        except: return []

    def approve_admin(self, email, action):
        if not self.db: return False
        try:
            ref = self.db.collection('admin_users').document(email)
            if action == 'approve': ref.update({'role': 'admin'})
            elif action == 'temp_super': ref.update({'role': 'temp_super_admin', 'expires_at': datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=7)})
            elif action == 'revoke': ref.update({'role': 'pending', 'expires_at': firestore.DELETE_FIELD})
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
            logs = []; aest_tz = datetime.timezone(datetime.timedelta(hours=10))
            for d in self.db.collection('admin_audit_logs').order_by('timestamp', direction=firestore.Query.DESCENDING).limit(150).stream():
                data = d.to_dict(); data['id'] = d.id; ts = data.get('timestamp')
                data['time_str'] = ts.astimezone(aest_tz).strftime("%d/%m/%Y %I:%M:%S %p") if ts else "Unknown Time"; data['timestamp'] = str(ts); logs.append(data)
            return logs
        except: return []

    def undo_audit_action(self, log_id, super_admin_email):
        if not self.db: return False
        try:
            doc_ref = self.db.collection('admin_audit_logs').document(log_id); doc = doc_ref.get()
            if not doc.exists or doc.to_dict().get('status') == 'undone': return False
            data = doc.to_dict(); action = data.get('action'); payload = json.loads(data.get('undo_payload', '{}'))
            if action == 'OVERRIDE_RATING': self.db.collection('rating_overrides').document(re.sub(r'[^a-zA-Z0-9]', '_', payload.get('name')).lower()).delete()
            elif action in ['UPDATE_MATCH', 'ADD_MATCH']: 
                if payload.get('correction_id'): self.db.collection('match_corrections').document(payload.get('correction_id')).delete()
                if payload.get('result_id'): self.db.collection('match_results').document(payload.get('result_id')).delete()
            elif action == 'OVERRIDE_DELTAS': self.db.collection('match_delta_overrides').document(payload.get('match_id')).delete()
            elif action in ['FORCE_FINISH_LIVE', 'WIPE_LIVE']:
                if action == 'FORCE_FINISH_LIVE': self.db.collection('match_results').document(payload.get('result_id')).delete()
                if payload.get('schedule_id') and payload.get('fixture_data'): self.db.collection('fixture_schedule').document(payload.get('schedule_id')).set(payload.get('fixture_data'))
            elif action == 'BULK_DATE_FIX':
                if 'rule_id' in payload: self.db.collection('bulk_date_rules').document(payload['rule_id']).delete()
            doc_ref.update({'status': 'undone', 'undone_by': super_admin_email}); self.refresh_data()
            return True
        except: return False

    def _clean_name(self, name): return self.alias_map.get(" ".join(str(name).split()).lower(), " ".join(str(name).split()).title()) if name else ""
    def _get_val(self, row, keys, default=''):
        row_keys_norm = {k.strip().lower(): k for k in row.keys()}
        for k in keys:
            if k.strip().lower() in row_keys_norm: return row[row_keys_norm[k.strip().lower()]]
        return default
    def _parse_date(self, date_str):
        if not date_str: return None
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d"):
            try: return datetime.datetime.strptime(str(date_str).strip(), fmt).date()
            except: continue
        return None
    def _generate_player_id(self): return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6))
    def _slugify(self, text): return re.sub(r'[^a-z0-9]', '', str(text).lower())
    def _extract_week(self, text): match = re.search(r'\d+', str(text)); return match.group() if match else str(text).strip().lower()

    def admin_bulk_fix_date(self, season, division, week, new_date, admin_email="Unknown"):
        if not self.db: return False
        try:
            rule_id = f"{self._slugify(season)}_{self._slugify(division)}_{self._extract_week(week)}"
            self.db.collection('bulk_date_rules').document(rule_id).set({'season': season, 'division': division, 'week': week, 'date': new_date, 'author': admin_email, 'timestamp': firestore.SERVER_TIMESTAMP})
            self._log_audit(admin_email, 'BULK_DATE_FIX', f"Smart-mapped matches for {season} {division} Week {week} to {new_date}.", {"rule_id": rule_id}); self.refresh_data()
            return True
        except: return False

    def admin_override_match_deltas(self, match_id, p1_delta, p2_delta, admin_email="Unknown"):
        if not self.db: return False
        try:
            self.db.collection('match_delta_overrides').document(match_id).set({'match_id': match_id, 'p1_delta': float(p1_delta), 'p2_delta': float(p2_delta), 'author': admin_email, 'timestamp': firestore.SERVER_TIMESTAMP}, merge=True)
            self._log_audit(admin_email, 'OVERRIDE_DELTAS', f"Set point changes for match {match_id}: P1 {p1_delta}, P2 {p2_delta}", {"match_id": match_id}); self.refresh_data()
            return True
        except: return False
        
    def admin_add_manual_match(self, p1, p2, date_str, game_scores, admin_email="Unknown"):
        if not self.db: return False
        try:
            s1 = 0; s2 = 0
            for game in game_scores.split(','):
                pts = game.strip().split('-')
                if len(pts) == 2:
                    try:
                        if int(pts[0]) > int(pts[1]): s1 += 1
                        else: s2 += 1
                    except: pass
            res = self.db.collection('match_results').add({'home_players': [p1], 'away_players': [p2], 'date': date_str, 'live_home_sets': s1, 'live_away_sets': s2, 'game_scores_history': game_scores, 'status': 'approved', 'manual_override': True, 'timestamp': firestore.SERVER_TIMESTAMP, 'author': admin_email})
            self._log_audit(admin_email, 'ADD_MATCH', f"Manually authored match: {p1} vs {p2} ({s1}-{s2}).", {"result_id": res[1].id}); self.refresh_data() 
            return True
        except: return False

    def admin_get_player_directory(self): return [{"name": name, "id": pid, "label": f"{name} (ID: {pid})"} for name, pid in self.player_ids.items()]

    def refresh_data(self):
        logger.info("⚡️ Fetching and Processing Data...")
        self.all_players = {}; self.season_stats = {}; self.seasons_list = ["Career"]
        self.divisions_list = set(); self.weekly_matches = {}; self.rating_engine = RatingEngine(); self.match_history_log = []
        
        if self.db:
            try:
                conf = self.db.collection('system_config').document('main').get().to_dict() or {}
                self.k_win = float(conf.get('k_win_scale', 1.0)); self.k_loss = float(conf.get('k_loss_scale', 1.4))
                admin_docs = list(self.db.collection('admin_users').stream()); active_admins = [d for d in admin_docs if d.to_dict().get('role') in ['admin', 'super_admin', 'temp_super_admin']]
                admin_count = len(active_admins); req_approvals = min(3, admin_count) if admin_count > 0 else 1
                chaos_doc = self.db.collection('system_config').document('chaos_mode').get()
                if chaos_doc.exists:
                    c_data = chaos_doc.to_dict()
                    self.chaos_config = {'weeks': c_data.get('weeks', []), 'approvals': c_data.get('approvals', []), 'req': req_approvals, 'active': len(c_data.get('approvals', [])) >= req_approvals}
                else: self.chaos_config = {'weeks': [], 'approvals': [], 'req': req_approvals, 'active': False}
            except: pass

        self.date_lookup = {}; self.date_to_week_map = {}
        if self.sheet_results: 
            try:
                for row in self.sheet_results.worksheet("Calculated_Dates").get_all_records():
                    s = self._slugify(row.get('Season', '')); d = self._slugify(row.get('Division', '')); w = self._extract_week(row.get('Week', '')); parsed = self._parse_date(str(row.get('Date','')))
                    if parsed: self.date_lookup[f"{s}|{d}|{w}"] = parsed; self.date_to_week_map[f"{s}|{parsed.strftime('%Y-%m-%d')}"] = w
            except: pass
            self.alias_map = {}
            try:
                for row in self.sheet_results.worksheet("Aliases").get_all_records():
                    bad = str(row.get('Bad Name')).strip().lower(); good = str(row.get('Good Name')).strip()
                    if bad and good: self.alias_map[bad] = good
            except: pass
            try:
                ws = self.sheet_results.worksheet("Ratings base"); rows = ws.get_all_values()
                if rows:
                    headers = [str(h).lower().strip() for h in rows[0]]; p_idx = headers.index('player') if 'player' in headers else 0
                    for row in rows[1:]:
                        if len(row) <= p_idx: continue
                        name = self._clean_name(row[p_idx])
                        if name and name not in self.all_players: self.all_players[name] = {'regular': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'fillin': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'combined': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}}
            except: pass

        raw_match_queue = []; delta_overrides_dict = {}
        if self.db:
            try:
                for doc in self.db.collection('match_delta_overrides').stream():
                    d = doc.to_dict(); delta_overrides_dict[doc.id] = {'p1_delta': float(d.get('p1_delta', 0)), 'p2_delta': float(d.get('p2_delta', 0))}
            except: pass
            
        if self.sheet_results:
            for worksheet in self.sheet_results.worksheets():
                title = worksheet.title
                if "season" not in title.lower(): continue
                season_name = re.sub(r'(?i)^season\s*:\s*', '', title).strip()
                if season_name not in self.seasons_list: self.seasons_list.append(season_name)
                if season_name not in self.season_stats: self.season_stats[season_name] = {}
                if season_name not in self.weekly_matches: self.weekly_matches[season_name] = {}
                try:
                    all_values = worksheet.get_all_values()
                    if not all_values: continue
                    raw_headers = all_values[0]; clean_headers = []; counts = {'name': 0, 'sets': 0, 'ps': 0, 'player': 0}
                    for h in raw_headers:
                        h_str = str(h).strip(); h_lower = h_str.lower(); key = None
                        if h_lower == 'name': key = 'name'
                        elif h_lower in ['sets', 's']: key = 'sets'
                        elif h_lower in ['pos', 'ps']: key = 'pos'
                        elif h_lower == 'player': key = 'player'
                        if key: counts[key] += 1; clean_headers.append(f"{h_str} {counts[key]}")
                        else: clean_headers.append(h_str)
                    
                    for i, r in enumerate(all_values[1:]):
                        if len(r) < len(clean_headers): r = r + [''] * (len(clean_headers) - len(r))
                        row = {header: r[j] for j, header in enumerate(clean_headers)}
                        if any("doubles" in str(v).lower() for v in row.values()): continue
                        p1 = self._clean_name(self._get_val(row, ['Name 1', 'Player 1', 'Name'])); p2 = self._clean_name(self._get_val(row, ['Name 2', 'Player 2']))
                        if not p1 or not p2: continue
                        div = str(self._get_val(row, ['Division', 'Div'], 'Unknown')).strip(); self.divisions_list.add(div)
                        p1_fill = "S" in str(self._get_val(row, ['PS 1', 'Pos 1', 'Pos'])).upper(); p2_fill = "S" in str(self._get_val(row, ['PS 2', 'Pos 2'])).upper()
                        round_val = self._get_val(row, ['Round', 'Rd', 'Week']); week_num = self._extract_week(round_val) if round_val else "unknown"
                        raw_date = self._get_val(row, ['Date', 'Match Date']); parsed_date = self._parse_date(raw_date)
                        if (not parsed_date) and str(week_num) != "unknown": parsed_date = self.date_lookup.get(f"{self._slugify(season_name)}|{self._slugify(div)}|{week_num}")
                        if not parsed_date: parsed_date = datetime.date(1900, 1, 1)
                        try: s1 = int(self._get_val(row, ['Sets 1', 'S1', 'Sets'])); s2 = int(self._get_val(row, ['Sets 2', 'S2']))
                        except: continue
                        raw_match_queue.append({'p1': p1, 'p2': p2, 's1': s1, 's2': s2, 'date': parsed_date, 'season': season_name, 'week': week_num, 'div': div, 'p1_fill': p1_fill, 'p2_fill': p2_fill, 'game_history': '', 'rich_stats': None, 'manual_override': False, 'sheet_name': worksheet.title, 'row_index': str(i + 2), 'source': 'Spreadsheet'})
                except: pass
                
        if self.db:
            try:
                for doc in self.db.collection('match_results').stream():
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
                    if str(week_val) == "unknown" and parsed_date: week_val = self.date_to_week_map.get(f"{self._slugify(season_name)}|{parsed_date.strftime('%Y-%m-%d')}", "unknown")
                    rich = d.get('richStats', {}); rich['total_duration'] = d.get('total_duration', '00:00'); rich['play_duration'] = d.get('play_duration', '00:00'); rich['set_scores'] = d.get('set_scores', [])
                    raw_match_queue.append({'p1': p1, 'p2': p2, 's1': s1, 's2': s2, 'date': parsed_date, 'season': season_name, 'week': week_val, 'div': d.get('division', 'Unknown'), 'p1_fill': False, 'p2_fill': False, 'game_history': d.get('game_scores_history', ''), 'rich_stats': rich, 'manual_override': d.get('manual_override', False), 'sheet_name': 'Live Match Data', 'row_index': 'Firebase', 'source': 'Admin/iPad'})
            except: pass

        corrections = {}
        if self.db:
            try:
                for doc in self.db.collection('match_corrections').stream():
                    c = doc.to_dict(); c_p1 = self._clean_name(c.get('p1', '')); c_p2 = self._clean_name(c.get('p2', '')); d_str = str(c.get('date', ''))
                    players = sorted([c_p1, c_p2]); key = f"{d_str}_{players[0]}_{players[1]}"
                    corrections[key] = {'s1': c.get('s1', 0), 's2': c.get('s2', 0), 'c_p1': c_p1, 'new_date': c.get('new_date')}
            except: pass

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

        groups = {}
        for m in raw_match_queue:
            players = sorted([m['p1'], m['p2']]); date_key = m['date'].strftime("%Y%m%d") if m['date'] else "nodate"
            match_key = f"{date_key}_{players[0]}_{players[1]}"
            if match_key not in groups: groups[match_key] = []
            groups[match_key].append(m)
        
        cleaned_matches = []
        for key, group in groups.items():
            if len(group) == 1: cleaned_matches.append(group[0]); continue
            firebase_m = [m for m in group if m.get('rich_stats') is not None]; sheet_m = [m for m in group if m.get('rich_stats') is None]; merged = []
            for fm in firebase_m: merged.append(fm); sheet_m.pop(0) if sheet_m else None
            for sm in sheet_m: merged.append(sm)
            cleaned_matches.extend(merged)
        
        cleaned_matches.sort(key=lambda x: x['date']); overrides_dict = {}
        if self.db:
            try:
                for doc in self.db.collection('rating_overrides').stream():
                    d = doc.to_dict(); overrides_dict[d.get('name')] = {'rating': float(d.get('rating', 1500)), 'rd': float(d.get('rd', 75.0)), 'vol': float(d.get('vol', 0.06)), 'date': d.get('date_str', '1900-01-01')}
            except: pass

        player_set = set(); player_overrides_applied = set()
        for m in cleaned_matches:
            players = sorted([m['p1'], m['p2']]); d_str = m['date'].strftime("%d/%m/%Y") if m['date'] else "nodate"; d_str_fmt = m['date'].strftime("%Y-%m-%d") if m['date'] else "1900-01-01"
            raw_id_string = f"{d_str}_{players[0]}_{players[1]}_{m['s1']}_{m['s2']}"; match_id = hashlib.md5(raw_id_string.encode()).hexdigest()[:6].upper()
            
            for p in [m['p1'], m['p2']]:
                if p in overrides_dict and p not in player_overrides_applied:
                    if d_str_fmt >= overrides_dict[p]['date']:
                        if p not in self.rating_engine.players: self.rating_engine.get_rating(p)
                        self.rating_engine.players[p]['rating'] = overrides_dict[p]['rating']; self.rating_engine.players[p]['rd'] = overrides_dict[p]['rd']; self.rating_engine.players[p]['vol'] = overrides_dict[p]['vol']
                        player_overrides_applied.add(p)

            deltas = {'p1_delta': 0, 'p2_delta': 0}
            if m['date'] > RATING_START_DATE: 
                if match_id in delta_overrides_dict:
                    p1_d = delta_overrides_dict[match_id]['p1_delta']; p2_d = delta_overrides_dict[match_id]['p2_delta']
                    p1_stats = self.rating_engine.get_rating(m['p1']); p2_stats = self.rating_engine.get_rating(m['p2'])
                    p1_before = p1_stats['rating']; p1_rd_before = p1_stats['rd']; p2_before = p2_stats['rating']; p2_rd_before = p2_stats['rd']
                    self.rating_engine.players[m['p1']]['rating'] += p1_d; self.rating_engine.players[m['p2']]['rating'] += p2_d
                    self.rating_engine.players[m['p1']]['rd'] = max(50.0, self.rating_engine.players[m['p1']]['rd'] - 4.0); self.rating_engine.players[m['p2']]['rd'] = max(50.0, self.rating_engine.players[m['p2']]['rd'] - 4.0)
                    deltas = {'p1_delta': p1_d, 'p2_delta': p2_d, 'p1_before': p1_before, 'p1_rd_before': p1_rd_before, 'p1_after': self.rating_engine.players[m['p1']]['rating'], 'p1_rd_after': self.rating_engine.players[m['p1']]['rd'], 'p2_before': p2_before, 'p2_rd_before': p2_rd_before, 'p2_after': self.rating_engine.players[m['p2']]['rating'], 'p2_rd_after': self.rating_engine.players[m['p2']]['rd']}
                else:
                    m_week = str(m.get('week', '')).lower()
                    is_chaos = self.chaos_config['active'] and m_week in [w.lower() for w in self.chaos_config['weeks']]
                    deltas = self.rating_engine.update_match(m['p1'], m['p2'], m['s1'], m['s2'], self.k_win, self.k_loss, not is_chaos)
            
            p1_delta = deltas.get('p1_delta', 0); p2_delta = deltas.get('p2_delta', 0)
            match_hash = f"{d_str}_{m['season']}_{m['week']}_{players[0]}_{players[1]}_{m['s1']}-{m['s2']}"
            record = {'id': match_hash, 'match_id': match_id, 'date': d_str, 'season': m['season'], 'week': m['week'], 'division': m['div'], 'p1': m['p1'], 'p2': m['p2'], 'home_players': [m['p1']], 'away_players': [m['p2']], 's1': m['s1'], 's2': m['s2'], 'score': f"{m['s1']}-{m['s2']}", 'p1_before': deltas.get('p1_before', self.rating_engine.get_rating(m['p1'])['rating']), 'p1_rd_before': deltas.get('p1_rd_before', self.rating_engine.get_rating(m['p1'])['rd']), 'p1_after': deltas.get('p1_after', self.rating_engine.get_rating(m['p1'])['rating']), 'p1_rd_after': deltas.get('p1_rd_after', self.rating_engine.get_rating(m['p1'])['rd']), 'p1_delta': p1_delta, 'p2_before': deltas.get('p2_before', self.rating_engine.get_rating(m['p2'])['rating']), 'p2_rd_before': deltas.get('p2_rd_before', self.rating_engine.get_rating(m['p2'])['rd']), 'p2_after': deltas.get('p2_after', self.rating_engine.get_rating(m['p2'])['rating']), 'p2_rd_after': deltas.get('p2_rd_after', self.rating_engine.get_rating(m['p2'])['rd']), 'p2_delta': p2_delta, 'rich_stats': m.get('rich_stats', {}), 'game_history': m.get('game_history', ''), 'sheet_name': m.get('sheet_name', 'Unknown'), 'row_index': m.get('row_index', '?'), 'source': m.get('source', 'Unknown')}
            self.match_history_log.append(record)
            
            def add_stats(p, sets_for, sets_against, is_p1, opp, fill, delta):
                if p not in self.all_players: self.all_players[p] = {'regular': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'fillin': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'combined': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}}
                if m['season'] not in self.season_stats: self.season_stats[m['season']] = {}
                if p not in self.season_stats[m['season']]: self.season_stats[m['season']][p] = {'regular': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'fillin': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}, 'combined': {'matches':0,'wins':0,'losses':0,'sets_won':0,'sets_lost':0,'history':[]}}
                result = "Win" if sets_for > sets_against else "Loss"
                h_rec = {'season': m['season'], 'week': m['week'], 'date': d_str, 'opponent': opp, 'result': result, 'score': f"{sets_for}-{sets_against}", 'type': 'Fill-in' if fill else 'Regular', 'division': m['div'], 'details': m.get('game_history', ''), 'rich_stats': m.get('rich_stats'), 'match_id': match_id, 'delta': delta, 'sheet_name': m.get('sheet_name', 'Unknown'), 'row_index': m.get('row_index', '?')}
                for s_dict in [self.all_players[p], self.season_stats[m['season']][p]]:
                    buckets = [s_dict['combined'], s_dict['fillin'] if fill else s_dict['regular']]
                    for b in buckets:
                        b['matches'] += 1; b['sets_won'] += sets_for; b['sets_lost'] += sets_against
                        if result == "Win": b['wins'] += 1
                        else: b['losses'] += 1
                        b['history'].append(h_rec)
            add_stats(m['p1'], m['s1'], m['s2'], True, m['p2'], m['p1_fill'], p1_delta); add_stats(m['p2'], m['s2'], m['s1'], False, m['p1'], m['p2_fill'], p2_delta); player_set.add(m['p1']); player_set.add(m['p2'])

            if str(m['week']) != "unknown":
                wk = str(m['week'])
                if wk not in self.weekly_matches[m['season']]: self.weekly_matches[m['season']][wk] = []
                self.weekly_matches[m['season']][wk].append({'p1': m['p1'], 'p2': m['p2'], 'score': f"{m['s1']}-{m['s2']}", 'division': m['div'], 'date': d_str})
        
        try:
            self.player_ids = {}; self.id_to_name = {}; ws = self.sheet_results.worksheet("Players"); all_values = ws.get_all_values()
            headers = [str(h).lower().strip() for h in all_values[0]]; name_col = headers.index("player name") if "player name" in headers else 0; id_col = headers.index("player id") if "player id" in headers else 1; existing_names = {}
            for i, row in enumerate(all_values[1:], start=2): 
                if not row: continue
                p_name = str(row[name_col]).strip() if len(row) > name_col else ""; clean_n = self._clean_name(p_name); p_id = str(row[id_col]).strip() if len(row) > id_col else ""
                if clean_n: self.player_ids[clean_n] = p_id; existing_names[clean_n.lower()] = True
            for p_name in player_set:
                if p_name.lower() not in existing_names: new_id = self._generate_player_id(); ws.append_row([p_name, new_id, datetime.date.today().strftime("%Y-%m-%d"), "Active"]); self.player_ids[p_name] = new_id
            self.id_to_name = {v: k for k, v in self.player_ids.items()}
        except: pass

        if self.db:
            try:
                batch = self.db.batch(); batch_count = 0
                for player_name, stats in self.all_players.items():
                    rat = self.rating_engine.get_rating(player_name); safe_id = re.sub(r'[^a-zA-Z0-9]', '_', player_name).lower()
                    batch.set(self.db.collection('player_profiles').document(safe_id), {'name': player_name, 'rating': int(rat['rating']), 'wins': stats['combined']['wins'], 'losses': stats['combined']['losses'], 'matches_played': stats['combined']['matches'], 'last_updated': datetime.datetime.now()}, merge=True)
                    batch_count += 1
                    if batch_count >= 400: batch.commit(); batch = self.db.batch(); batch_count = 0
                if batch_count > 0: batch.commit()
            except: pass

    def get_matches_by_week(self, season, week): return self.weekly_matches.get(season, {}).get(str(week), [])
    def get_all_players(self): return self.all_players
    def get_seasons(self): return self.seasons_list
    def get_divisions(self): return sorted(list(self.divisions_list))
    
    def get_division_rankings(self, season, division, max_week=None):
        if season not in self.season_stats: return []
        ranking_list = []
        for player_name, stats in self.season_stats[season].items():
            rat = self.rating_engine.get_rating(player_name)
            reg_hist = [m for m in stats['regular']['history'] if m['division'] == division]; fill_hist = [m for m in stats['fillin']['history'] if m['division'] == division]
            if not reg_hist and not fill_hist: continue 
            def calc_summary(history): return {'wins': sum(1 for m in history if m['result'] == "Win"), 'losses': len(history) - sum(1 for m in history if m['result'] == "Win"), 'matches': len(history)}
            ranking_list.append({'name': player_name, 'rating_val': int(rat['rating']), 'sigma': int(rat['rd']), 'vol': rat.get('vol', 0.06), 'regular': calc_summary(reg_hist), 'fillin': calc_summary(fill_hist)})
        return ranking_list
        
    def get_player_stats(self, player_name, season="Career", division="All", week="All", start_date=None, end_date=None):
        data = self.all_players if season == "Career" else self.season_stats.get(season, {})
        if player_name not in data: return None
        raw = data[player_name]; rat = self.rating_engine.get_rating(player_name)
        def format_bucket(stats):
            hist = stats['history']
            if division != "All": hist = [m for m in hist if m.get('division') == division]
            wins = sum(1 for m in hist if m['result'] == "Win"); win_rate = round((wins / len(hist)) * 100, 1) if hist else 0
            disp_hist = list(hist); disp_hist.reverse()
            return {'matches': len(hist), 'wins': wins, 'losses': len(hist)-wins, 'win_rate': f"{win_rate}%", 'match_history': disp_hist}
        return {'name': player_name, 'rating': int(rat['rating']), 'rd': int(rat['rd']), 'vol': rat.get('vol', 0.06), 'combined': format_bucket(raw['combined']), 'peterman_id': self.all_players.get(player_name, {}).get('peterman_id', '')}

    def admin_get_teams(self):
        if not self.db: return []
        try: return [{"id": d.id, **d.to_dict()} for d in self.db.collection('teams').stream()]
        except: return []

    def admin_update_team(self, team_id, players_list, admin_email="Unknown"):
        if not self.db: return False
        try: 
            self.db.collection('teams').document(team_id).set({'players': players_list}, merge=True)
            self._log_audit(admin_email, 'UPDATE_TEAM', f"Updated roster for {team_id}", {}); return True
        except: return False

    def admin_upload_pdf_schedule(self, season, division, file_stream, admin_email="Unknown"):
        if not self.db: return {"success": False, "error": "DB Offline"}
        try:
            import pdfplumber; import re
            matches_found = []; teams = {}
            with pdfplumber.open(file_stream) as pdf:
                for page in pdf.pages:
                    for table in page.extract_tables():
                        if not table or len(table) < 2: continue
                        headers = [str(x).lower().replace('\n', ' ') for x in table[0] if x]
                        if any("team name" in h for h in headers) or any("player" in h for h in headers):
                            for row in table[1:]:
                                if not row or not row[0]: continue
                                nums = str(row[0]).split('\n'); names = str(row[1]).split('\n') if len(row) > 1 else []
                                p1_block = str(row[2]).split('\n') if len(row) > 2 else []; p2_block = str(row[3]).split('\n') if len(row) > 3 else []; p3_block = str(row[4]).split('\n') if len(row) > 4 else []
                                def clean_p(block, idx):
                                    text_lines = [line.strip() for line in block if re.search(r'[A-Za-z]', line)]
                                    if idx < len(text_lines): return re.sub(r'[^A-Za-z\s-]', '', re.sub(r'\s+C$', '', text_lines[idx]).strip()).strip()
                                    return ""
                                for i in range(len(nums)):
                                    num = nums[i].strip()
                                    if not num.isdigit(): continue
                                    t_name = names[i].strip() if i < len(names) else f"Team {num}"; t_name = re.sub(r'[^A-Za-z0-9\s]', '', t_name).strip()
                                    players = [clean_p(p1_block, i), clean_p(p2_block, i), clean_p(p3_block, i)]; players = [p.title() for p in players if len(p) > 2]
                                    teams[num] = {"name": t_name, "players": players}
                        if any("date" in h for h in headers) or any("match" in h for h in headers):
                            for row in table[1:]:
                                if not row: continue
                                date_val_1 = str(row[0]).replace('\n', ' ').strip()
                                if date_val_1 and len(date_val_1) >= 4 and not date_val_1.isdigit():
                                    for cell in row[1:5]: 
                                        if not cell: continue
                                        match = re.search(r'(\d+)\s*vs\s*(\d+)', str(cell).replace('\n', ' '), re.IGNORECASE)
                                        if match: matches_found.append({'date_text': date_val_1, 't1': match.group(1), 't2': match.group(2)})
                                if len(row) > 6:
                                    date_val_2 = str(row[6]).replace('\n', ' ').strip() 
                                    if date_val_2 and len(date_val_2) >= 4 and not date_val_2.isdigit():
                                        for cell in row[7:]:
                                            if not cell: continue
                                            match = re.search(r'(\d+)\s*vs\s*(\d+)', str(cell).replace('\n', ' '), re.IGNORECASE)
                                            if match: matches_found.append({'date_text': date_val_2, 't1': match.group(1), 't2': match.group(2)})
            if not matches_found: return {"success": False, "error": "No schedule detected in the PDF."}
            batch = self.db.batch()
            for m in matches_found:
                home = teams.get(m['t1'], {"name": f"Team {m['t1']}", "players": []}); away = teams.get(m['t2'], {"name": f"Team {m['t2']}", "players": []})
                batch.set(self.db.collection('upcoming_schedule').document(), {'season': season, 'division': division, 'date_text': m['date_text'], 'home_team': home['name'], 'away_team': away['name'], 'home_players': home['players'], 'away_players': away['players']})
            for t_num, t_data in teams.items():
                team_doc_id = f"{self._slugify(season)}_{self._slugify(division)}_{self._slugify(t_data['name'])}"
                batch.set(self.db.collection('teams').document(team_doc_id), {'season': season, 'division': division, 'team_name': t_data['name'], 'players': t_data['players'], 'timestamp': firestore.SERVER_TIMESTAMP}, merge=True)
            batch.commit()
            self._log_audit(admin_email, 'UPLOAD_SCHEDULE', f"Parsed PDF for {season} {division}. Extracted {len(teams)} teams and {len(matches_found)} matchups.", {})
            return {"success": True, "matches_found": len(matches_found), "teams_found": len(teams)}
        except Exception as e: return {"success": False, "error": f"Failed to read PDF: {str(e)}"}

    def get_player_upcoming_schedule(self, player_name):
        if not self.db: return []
        try:
            player_clean = self._clean_name(player_name).lower(); matches = []
            for d in self.db.collection('upcoming_schedule').stream():
                m = d.to_dict(); home_p = [self._clean_name(p).lower() for p in m.get('home_players', [])]; away_p = [self._clean_name(p).lower() for p in m.get('away_players', [])]
                is_playing = False
                for hp in home_p:
                    if player_clean in hp or hp in player_clean: is_playing = True
                for ap in away_p:
                    if player_clean in ap or ap in player_clean: is_playing = True
                if is_playing: matches.append(m)
            return matches
        except: return []

    def get_notices(self):
        if not self.db: return []
        try:
            res = []; aest_tz = datetime.timezone(datetime.timedelta(hours=10))
            for d in self.db.collection('notices').order_by('timestamp', direction=firestore.Query.DESCENDING).stream():
                data = d.to_dict(); data['id'] = d.id; ts = data.get('timestamp'); data['date_str'] = ts.astimezone(aest_tz).strftime('%d/%m/%Y %I:%M %p') if ts else 'Recent'; data['timestamp'] = str(ts); res.append(data)
            return res
        except: return []

    def admin_add_notice(self, title, message, notice_type, admin_email="Unknown"):
        if not self.db: return False
        try: self.db.collection('notices').add({'title': title, 'message': message, 'type': notice_type, 'timestamp': firestore.SERVER_TIMESTAMP, 'author': admin_email}); self._log_audit(admin_email, 'ADD_NOTICE', f"Posted notice: {title}", {}); return True
        except: return False

    def admin_delete_notice(self, notice_id, admin_email="Unknown"):
        if not self.db: return False
        try: self.db.collection('notices').document(notice_id).delete(); self._log_audit(admin_email, 'DELETE_NOTICE', f"Deleted notice ID: {notice_id}", {}); return True
        except: return False

    def get_community_feed(self):
        if not self.db: return []
        try:
            posts = []; aest_tz = datetime.timezone(datetime.timedelta(hours=10))
            for d in self.db.collection('community_posts').order_by('timestamp', direction=firestore.Query.DESCENDING).limit(50).stream():
                data = d.to_dict(); data['id'] = d.id; ts = data.get('timestamp'); data['time_str'] = ts.astimezone(aest_tz).strftime('%d/%m/%y %I:%M %p') if ts else 'Just now'; data['timestamp'] = str(ts); posts.append(data)
            return posts
        except: return []

    def create_community_post(self, author_uid, author_name, content, post_type="General", image_url=None, poll_options=None):
        if not self.db: return False
        try:
            payload = {'author_uid': author_uid, 'author_name': author_name, 'content': content, 'type': post_type, 'image_url': image_url, 'upvotes': [], 'comments': [], 'timestamp': firestore.SERVER_TIMESTAMP}
            if poll_options: payload['poll'] = {opt: [] for opt in poll_options}
            self.db.collection('community_posts').add(payload); return True
        except: return False

    def vote_community_poll(self, post_id, option_text, voter_uid):
        if not self.db: return False
        try:
            doc_ref = self.db.collection('community_posts').document(post_id); doc = doc_ref.get()
            if not doc.exists: return False
            poll = doc.to_dict().get('poll', {})
            for opt, voters in poll.items():
                if voter_uid in voters: voters.remove(voter_uid)
            if option_text in poll: poll[option_text].append(voter_uid)
            doc_ref.update({'poll': poll}); return True
        except: return False

    def add_community_comment(self, post_id, author_name, content):
        if not self.db: return False
        try:
            self.db.collection('community_posts').document(post_id).update({'comments': firestore.ArrayUnion([{'author': author_name, 'content': content, 'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat()}])}); return True
        except: return False

    def toggle_post_upvote(self, post_id, uid):
        if not self.db: return False
        try:
            doc_ref = self.db.collection('community_posts').document(post_id); doc = doc_ref.get()
            if not doc.exists: return False
            upvotes = doc.to_dict().get('upvotes', [])
            if uid in upvotes: upvotes.remove(uid)
            else: upvotes.append(uid)
            doc_ref.update({'upvotes': upvotes}); return True
        except: return False

    def admin_delete_community_post(self, post_id, admin_email="Unknown"):
        if not self.db: return False
        try: self.db.collection('community_posts').document(post_id).delete(); self._log_audit(admin_email, 'DELETE_POST', f"Deleted community post {post_id}", {}); return True
        except: return False

    def get_contact_lists(self):
        if not self.sheet_results: return {"emails": "", "phones": "", "preview": [], "error": "No sheet connection"}
        try:
            ws = self.sheet_results.worksheet("Member info"); raw_data = ws.get_all_values()
            if not raw_data or len(raw_data) < 2: return {"emails": "", "phones": "", "preview": [], "error": "Sheet appears to be empty."}
            headers = [str(h).lower().strip() for h in raw_data[0]]; has_send_col = any('send' in h for h in headers)
            emails = []; phones = []; preview = []
            for row in raw_data[1:]:
                if not any(str(cell).strip() for cell in row): continue
                email_val = ""; phone_val = ""; name_val = "Unknown Player"; allow_sms = not has_send_col 
                for idx, cell_val in enumerate(row):
                    if idx >= len(headers): break
                    col_name = headers[idx]; val_str = str(cell_val).strip()
                    if col_name in ['name', 'player', 'player name', 'full name', 'first name'] and name_val == "Unknown Player": name_val = val_str
                    if 'email' in col_name and not email_val: email_val = val_str
                    if ('phone' in col_name or 'mobile' in col_name or 'number' in col_name) and not phone_val: phone_val = val_str
                    if 'send' in col_name: allow_sms = val_str.lower() in ['yes', 'y', 'true']
                if email_val and '@' in email_val: emails.append(email_val)
                if phone_val and allow_sms:
                    clean_phone = re.sub(r'[^\d\+\s]', '', phone_val)
                    if len(clean_phone) >= 8: 
                        phones.append(clean_phone); main_div = "Unknown Div"; clean_name_key = self._clean_name(name_val)
                        if clean_name_key in self.all_players:
                            div_counts = {}
                            for h in self.all_players[clean_name_key].get('combined', {}).get('history', []): div_counts[h['division']] = div_counts.get(h['division'], 0) + 1
                            if div_counts: main_div = max(div_counts, key=div_counts.get)
                        preview.append({"name": name_val, "phone": clean_phone, "division": main_div})
            return {"emails": ", ".join(list(set(emails))), "phones": ", ".join(list(set(phones))), "preview": preview}
        except Exception as e: return {"emails": "", "phones": "", "preview": [], "error": str(e)}

    def admin_send_sms_broadcast(self, message_body, target_phones=None, admin_email="Unknown"):
        contacts = self.get_contact_lists(); raw_phones = contacts.get("phones", "")
        if not raw_phones: return {"success": False, "error": "No valid opted-in phone numbers found in the Google Sheet."}
        allowed_phone_list = [p.strip() for p in raw_phones.split(",") if p.strip()]
        if target_phones is not None: phone_list = [p for p in target_phones if p in allowed_phone_list]
        else: phone_list = allowed_phone_list
        if not phone_list: return {"success": False, "error": "No valid opted-in phone numbers matched your selection."}
        username = "jakobwill7@gmail.com"; api_key = "76F26417-8DEB-E47E-8056-B86E519B4445"
        clean_body = re.sub(r'[^\x20-\x7E\n\r]+', '', message_body); final_message = f"GCTTA Update: {clean_body}\n\nView stats: gctta-stats.com.au\nReply STOP to opt out"
        messages = [{"source": "gctta_admin", "from": "GCTTA-STATS", "body": final_message, "to": phone} for phone in phone_list]
        payload = json.dumps({"messages": messages}).encode('utf-8')
        try:
            auth_str = f"{username}:{api_key}"; auth_bytes = base64.b64encode(auth_str.encode('utf-8')).decode('utf-8')
            req = urllib.request.Request("https://rest.clicksend.com/v3/sms/send", data=payload); req.add_header("Content-Type", "application/json"); req.add_header("Authorization", f"Basic {auth_bytes}")
            response = urllib.request.urlopen(req); res_data = json.loads(response.read().decode('utf-8'))
            if res_data.get('http_code') == 200:
                self._log_audit(admin_email, 'SMS_BROADCAST', f"Sent Mass SMS to {len(phone_list)} selected members via API.", {})
                return {"success": True, "message": f"Successfully sent SMS to {len(phone_list)} selected members!"}
            else: return {"success": False, "error": f"ClickSend API Error: {res_data}"}
        except Exception as e: return {"success": False, "error": str(e)}

    def get_sms_inbox(self):
        if not self.db: return []
        try:
            res = []; aest_tz = datetime.timezone(datetime.timedelta(hours=10))
            for d in self.db.collection('sms_replies').order_by('timestamp', direction=firestore.Query.DESCENDING).limit(50).stream():
                data = d.to_dict(); data['id'] = d.id; ts = data.get('timestamp'); data['time_str'] = ts.astimezone(aest_tz).strftime('%d/%m/%Y %I:%M %p') if ts else 'Just now'; res.append(data)
            return res
        except: return []

    def admin_get_all_donations(self):
        if not self.db: return []
        try:
            res = []; aest_tz = datetime.timezone(datetime.timedelta(hours=10))
            for d in self.db.collection('donations').order_by('timestamp', direction=firestore.Query.DESCENDING).stream():
                data = d.to_dict(); data['id'] = d.id; ts = data.get('timestamp'); data['time_str'] = ts.astimezone(aest_tz).strftime('%d/%m/%Y %I:%M %p') if ts else 'Unknown'; res.append(data)
            return res
        except: return []

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
            grouped_donors = {}; donors = []
            for d in self.db.collection('donations').stream():
                data = d.to_dict(); n = data.get('name', 'Anonymous').strip()
                if not n: n = 'Anonymous'
                amt = float(data.get('amount', 0))
                if n.lower() == 'anonymous': donors.append({'name': 'Anonymous', 'amount': amt})
                else: grouped_donors[n] = grouped_donors.get(n, 0) + amt
            for k, v in grouped_donors.items(): donors.append({'name': k, 'amount': v})
            donors.sort(key=lambda x: x['amount'], reverse=True); return donors[:limit]
        except: return []