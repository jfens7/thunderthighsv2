# backend/backend.py
import firebase_admin
from firebase_admin import credentials, firestore, auth
import datetime, re, os, json, sys, logging, random, string, hashlib, threading

from backend.glicko import RatingEngine
from backend.rc_scraper import RatingsCentralScraper
from backend.sheets_sync import SheetsSyncEngine
from backend.league_engine import LeagueEngineMixin
from backend.comms_engine import CommsEngineMixin

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SUPER_ADMIN_EMAIL = "jakobwill7@gmail.com"

class ThunderData(LeagueEngineMixin, CommsEngineMixin):
    def __init__(self):
        self.db = None
        self.all_players = {}
        self.season_stats = {}
        self.seasons_list = ["Career"]
        self.divisions_list = set()
        self.date_lookup = {}
        self.weekly_matches = {}
        self.player_ids = {}
        self.id_to_name = {}
        self.alias_map = {}
        self.date_to_week_map = {}
        self.match_history_log = []
        self.k_win = 1.0
        self.k_loss = 1.4
        self.chaos_config = {'active': False, 'weeks': [], 'approvals': [], 'req': 3}
        
        self.rating_engine = RatingEngine()
        self.rc_scraper = RatingsCentralScraper()
        
        self._init_firebase()
        
        self.sync_engine = SheetsSyncEngine(self)
        self.refresh_data()

    def _init_firebase(self):
        try:
            if not len(firebase_admin._apps):
                cred_path = 'firebase_credentials.json'
                if not os.path.exists(cred_path) and os.path.exists('backend/firebase_credentials.json'): 
                    cred_path = 'backend/firebase_credentials.json'
                firebase_admin.initialize_app(credentials.Certificate(cred_path))
            self.db = firestore.client()
            if not self.db.collection('system_config').document('main').get().exists: 
                self.db.collection('system_config').document('main').set({'tournament_mode_active': False})
        except Exception as e: 
            logger.error(f"Firebase Init Error: {e}")
            self.db = None

    def refresh_data(self):
        if hasattr(self, 'sync_engine') and self.sync_engine:
            self.sync_engine.run_sync()

    # --- Core Utilities ---
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

    # --- System & Auth ---
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
            doc_ref = self.db.collection('admin_audit_logs').document(log_id)
            doc = doc_ref.get()
            if not doc.exists or doc.to_dict().get('status') == 'undone': return False
            data = doc.to_dict()
            action = data.get('action')
            payload = json.loads(data.get('undo_payload', '{}'))
            
            if action == 'OVERRIDE_RATING': self.db.collection('rating_overrides').document(re.sub(r'[^a-zA-Z0-9]', '_', payload.get('name', '')).lower()).delete()
            elif action in ['UPDATE_MATCH', 'ADD_MATCH']: 
                if payload.get('correction_id'): self.db.collection('match_corrections').document(payload.get('correction_id')).delete()
                if payload.get('result_id'): self.db.collection('match_results').document(payload.get('result_id')).delete()
            elif action == 'OVERRIDE_DELTAS': self.db.collection('match_delta_overrides').document(payload.get('match_id')).delete()
            elif action in ['FORCE_FINISH_LIVE', 'WIPE_LIVE']:
                if action == 'FORCE_FINISH_LIVE' and payload.get('result_id'): self.db.collection('match_results').document(payload.get('result_id')).delete()
                if payload.get('schedule_id') and payload.get('fixture_data'): self.db.collection('fixture_schedule').document(payload.get('schedule_id')).set(payload.get('fixture_data'))
            elif action == 'BULK_DATE_FIX':
                if 'rule_id' in payload: self.db.collection('bulk_date_rules').document(payload['rule_id']).delete()
            elif action == 'UPDATE_MATH':
                self.db.collection('system_config').document('main').set({'k_win_scale': float(payload.get('old_k_win', 1.0)), 'k_loss_scale': float(payload.get('old_k_loss', 1.4))}, merge=True)
                self.k_win = float(payload.get('old_k_win', 1.0)); self.k_loss = float(payload.get('old_k_loss', 1.4))
                
            doc_ref.update({'status': 'undone', 'undone_by': super_admin_email})
            self.refresh_data()
            return True
        except: return False

    # --- Ratings Central Engine ---
    def search_ratings_central_by_name(self, player_name, target_club=None): return self.rc_scraper.search_by_name(player_name, target_club)

    def trigger_background_rc_scrape(self, player_name, rc_id, admin_email):
        def scrape_task():
            stats = self.rc_scraper.deep_scrape_profile(rc_id)
            if stats and self.db:
                safe_id = re.sub(r'[^a-zA-Z0-9]', '_', player_name).lower()
                self.db.collection('player_profiles').document(safe_id).set(stats, merge=True)
                self._log_audit(admin_email, 'RC_SYNC', f"Deep Scraped RC Data for {player_name}", {})
        threading.Thread(target=scrape_task).start()
        return True

    def admin_get_player_profile(self, player_name):
        if not self.db: return {}
        try:
            doc = self.db.collection('player_profiles').document(re.sub(r'[^a-zA-Z0-9]', '_', player_name).lower()).get()
            data = doc.to_dict() if doc.exists else {}
            data['gctta_sd'] = self.rating_engine.get_rating(player_name).get('rd', 300.0) 
            return data
        except: return {}

    def admin_update_player_profile(self, player_name, ratings_central_id, admin_email="Unknown"):
        if not self.db: return False
        try:
            self.db.collection('player_profiles').document(re.sub(r'[^a-zA-Z0-9]', '_', player_name).lower()).set({'name': player_name, 'ratings_central_id': ratings_central_id}, merge=True)
            self._log_audit(admin_email, 'UPDATE_PROFILE', f"Updated {player_name} Profile (RC: {ratings_central_id})", {})
            return True
        except: return False

    # --- Accounts & Linking ---
    def register_player_account(self, name, dob, email, uid, estimated_rating, club="Unknown"):
        if not self.db: return {"success": False, "error": "DB Offline"}
        try:
            est_val = str(estimated_rating).strip()
            suggested_rc_id = est_val if est_val.isdigit() and int(est_val) > 5000 else ""
            self.db.collection('pending_accounts').document(uid).set({'name': name, 'dob': dob, 'email': email, 'uid': uid, 'estimated_rating': 1500.0 if suggested_rc_id else float(estimated_rating or 1500.0), 'suggested_rc_id': suggested_rc_id, 'club': club, 'rc_sd': 150.0, 'status': 'pending', 'timestamp': firestore.SERVER_TIMESTAMP})
            return {"success": True}
        except Exception as e: return {"success": False, "error": str(e)}

    def admin_get_pending_accounts(self):
        if not self.db: return []
        try: return [{"id": d.id, **d.to_dict()} for d in self.db.collection('pending_accounts').where('status', '==', 'pending').stream()]
        except: return []

    def admin_link_player_account(self, uid, official_player_name, rc_id=None, admin_email="Unknown"):
        if not self.db: return False
        try:
            doc = self.db.collection('pending_accounts').document(uid).get()
            if not doc.exists: return False
            data = doc.to_dict(); data['linked_player_name'] = official_player_name; data['status'] = 'approved'; data['approved_by'] = admin_email
            self.db.collection('verified_users').document(uid).set(data)
            self.db.collection('pending_accounts').document(uid).delete()
            update_payload = {'last_updated': firestore.SERVER_TIMESTAMP}
            if rc_id: update_payload['ratings_central_id'] = rc_id
            self.db.collection('player_profiles').document(re.sub(r'[^a-zA-Z0-9]', '_', official_player_name).lower()).set(update_payload, merge=True)
            self._log_audit(admin_email, 'APPROVE_ACCOUNT', f"Linked UID {uid} to {official_player_name}", {})
            return True
        except: return False

    # --- Match Overrides & Stats ---
    def admin_override_rating(self, player_id, new_rating, sd_override=None, retroactive=True, admin_email="Unknown"):
        if not self.db: return False
        try:
            final_sd = float(sd_override) if sd_override else 65.0; player_name = self.id_to_name.get(player_id)
            if not player_name:
                for n, i in self.player_ids.items():
                    if i == player_id: player_name = n; break
            if not player_name: return False

            payload = {'name': player_name, 'rating': float(new_rating), 'rd': final_sd, 'vol': 0.06, 'date_str': '1900-01-01' if retroactive else datetime.datetime.now().strftime("%Y-%m-%d"), 'timestamp': firestore.SERVER_TIMESTAMP, 'author': admin_email}
            self.db.collection('rating_overrides').document(player_name).set(payload)
            self._log_audit(admin_email, 'OVERRIDE_RATING', f"Forced {player_name} to Rating: {new_rating}, SD: {final_sd}", {"name": player_name})
            self.refresh_data()
            return True
        except: return False

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
    def admin_get_chaos_config(self): return self.chaos_config

    def admin_vote_chaos(self, weeks, admin_email="Unknown"):
        if not self.db: return False
        try:
            doc = self.db.collection('system_config').document('chaos_mode').get()
            data = doc.to_dict() if doc.exists else {'weeks': [], 'approvals': []}
            data['weeks'] = weeks
            if admin_email not in data['approvals']: data['approvals'].append(admin_email)
            self.db.collection('system_config').document('chaos_mode').set(data, merge=True)
            self._log_audit(admin_email, 'CHAOS_VOTE', f"Voted to activate chaos mode for weeks: {weeks}", {}); self.refresh_data()
            return True
        except: return False

    def admin_clear_chaos(self, admin_email="Unknown"):
        if not self.db: return False
        try:
            self.db.collection('system_config').document('chaos_mode').set({'weeks': [], 'approvals': []})
            self._log_audit(admin_email, 'CHAOS_CLEAR', "Cleared chaos mode settings", {}); self.refresh_data()
            return True
        except: return False


    # ==========================================
    # BULLETPROOF DATA GETTERS (DYNAMIC EXTRACTION)
    # Automatically extracts seasons/divisions directly from the raw data
    # so they can NEVER be empty, regardless of what the Sync Engine names them.
    # ==========================================
    
    def get_seasons(self): 
        seasons = set()
        if hasattr(self, 'seasons_list') and self.seasons_list: seasons.update(self.seasons_list)
        if hasattr(self, 'seasons') and self.seasons: seasons.update(self.seasons)
        if hasattr(self, 'weekly_matches') and isinstance(self.weekly_matches, dict): seasons.update(self.weekly_matches.keys())
        if hasattr(self, 'season_stats') and isinstance(self.season_stats, dict): seasons.update(self.season_stats.keys())
        
        clean_seasons = sorted([str(s) for s in seasons if str(s) != "Career"], reverse=True)
        return clean_seasons if clean_seasons else ["Summer 2026"] # Failsafe
        
    def get_divisions(self): 
        divs = set()
        if hasattr(self, 'divisions_list') and self.divisions_list: divs.update(self.divisions_list)
        if hasattr(self, 'divisions') and self.divisions: divs.update(self.divisions)
        
        # Deep extract just to be 100% sure
        if hasattr(self, 'weekly_matches') and isinstance(self.weekly_matches, dict):
            for s_data in self.weekly_matches.values():
                if isinstance(s_data, dict):
                    for w_data in s_data.values():
                        if isinstance(w_data, list):
                            for m in w_data:
                                if 'division' in m: divs.add(m['division'])
                                
        clean_divs = sorted(list(divs))
        return clean_divs if clean_divs else ["Division 1"] # Failsafe
        
    def get_all_players(self): 
        return getattr(self, 'all_players', getattr(self, 'players', {}))
        
    def get_matches_by_week(self, season, week): 
        matches = getattr(self, 'weekly_matches', getattr(self, 'matches_by_week', getattr(self, 'weekly_results', {})))
        if isinstance(matches, dict):
            s_data = matches.get(season, {})
            if isinstance(s_data, dict): return s_data.get(str(week), [])
        return []

    def get_division_rankings(self, season, division, max_week=None):
        stats_dict = getattr(self, 'season_stats', getattr(self, 'stats_by_season', {}))
        if season not in stats_dict: return []
        
        ranking_list = []
        for player_name, stats in stats_dict[season].items():
            rat = self.rating_engine.get_rating(player_name)
            reg_hist = [m for m in stats.get('regular', {}).get('history', []) if m.get('division') == division]
            fill_hist = [m for m in stats.get('fillin', {}).get('history', []) if m.get('division') == division]
            if not reg_hist and not fill_hist: continue 
            
            def calc_summary(history): return {'wins': sum(1 for m in history if m.get('result') == "Win"), 'losses': len(history) - sum(1 for m in history if m.get('result') == "Win"), 'matches': len(history)}
            
            ranking_list.append({
                'name': player_name, 
                'rating_val': int(rat.get('rating', 1500)), 
                'sigma': int(rat.get('rd', 350)), 
                'vol': rat.get('vol', 0.06), 
                'regular': calc_summary(reg_hist), 
                'fillin': calc_summary(fill_hist)
            })
        return ranking_list
        
    def get_player_stats(self, player_name, season="Career", division="All", week="All", start_date=None, end_date=None):
        players_data = self.get_all_players()
        season_data = getattr(self, 'season_stats', getattr(self, 'stats_by_season', {}))
        data = players_data if season == "Career" else season_data.get(season, {})
        
        if player_name not in data: return None
        raw = data[player_name]
        rat = self.rating_engine.get_rating(player_name)
        
        def format_bucket(stats):
            if not stats: return {'matches': 0, 'wins': 0, 'losses': 0, 'win_rate': "0%", 'match_history': []}
            hist = stats.get('history', [])
            if division != "All": hist = [m for m in hist if m.get('division') == division]
            wins = sum(1 for m in hist if m.get('result') == "Win")
            win_rate = round((wins / len(hist)) * 100, 1) if hist else 0
            disp_hist = list(hist)
            disp_hist.reverse()
            return {'matches': len(hist), 'wins': wins, 'losses': len(hist)-wins, 'win_rate': f"{win_rate}%", 'match_history': disp_hist}
            
        return {
            'name': player_name, 
            'rating': int(rat.get('rating', 1500)), 
            'rd': int(rat.get('rd', 350)), 
            'vol': rat.get('vol', 0.06), 
            'combined': format_bucket(raw.get('combined', {})), 
            'regular': format_bucket(raw.get('regular', {})), 
            'fillin': format_bucket(raw.get('fillin', {})), 
            'peterman_id': players_data.get(player_name, {}).get('peterman_id', '')
        }

    def admin_glicko_math(self, p1, p2, s1, s2):
        p1_clean = self._clean_name(p1); p2_clean = self._clean_name(p2)
        try: s1 = int(s1); s2 = int(s2)
        except: return {"error": "Invalid scores"}

        r_eng = RatingEngine() 
        p1_stats = self.rating_engine.get_rating(p1_clean).copy(); p2_stats = self.rating_engine.get_rating(p2_clean).copy()
        r_eng.players[p1_clean] = p1_stats; r_eng.players[p2_clean] = p2_stats
        res = r_eng.update_match(p1_clean, p2_clean, s1, s2, '', self.k_win, self.k_loss, True)

        return {"p1": {"name": p1_clean, "old_rating": res['p1_before'], "new_rating": res['p1_after'], "delta": res['p1_delta'], "old_rd": res['p1_rd_before'], "new_rd": res['p1_rd_after'], "rd_delta": res['p1_rd_after'] - res['p1_rd_before']}, "p2": {"name": p2_clean, "old_rating": res['p2_before'], "new_rating": res['p2_after'], "delta": res['p2_delta'], "old_rd": res['p2_rd_before'], "new_rd": res['p2_rd_after'], "rd_delta": res['p2_rd_after'] - res['p2_rd_before']}}
        
    def admin_set_rating_scales(self, k_win, k_loss, admin_email="Unknown"):
        if not self.db: return False
        try:
            doc = self.db.collection('system_config').document('main').get(); old_k_win = 1.0; old_k_loss = 1.4
            if doc.exists:
                data = doc.to_dict()
                old_k_win = data.get('k_win_scale', 1.0); old_k_loss = data.get('k_loss_scale', 1.4)

            self.db.collection('system_config').document('main').set({'k_win_scale': float(k_win), 'k_loss_scale': float(k_loss)}, merge=True)
            self.k_win = float(k_win); self.k_loss = float(k_loss)
            self._log_audit(admin_email, 'UPDATE_MATH', f"Set K-Win to {k_win}, K-Loss to {k_loss}", {"old_k_win": old_k_win, "old_k_loss": old_k_loss}); return True
        except: return False

    def get_head_to_head(self, p1, p2):
        if not p1 or not p2: return {"error": "Missing players"}
        p1_clean = self._clean_name(p1); p2_clean = self._clean_name(p2); matches = []; p1_wins = 0; p2_wins = 0

        for m in getattr(self, 'match_history_log', getattr(self, 'match_history', [])):
            if (m['p1'] == p1_clean and m['p2'] == p2_clean) or (m['p1'] == p2_clean and m['p2'] == p1_clean):
                matches.append(m)
                if m['p1'] == p1_clean:
                    if m['s1'] > m['s2']: p1_wins += 1
                    elif m['s2'] > m['s1']: p2_wins += 1
                else:
                    if m['s1'] > m['s2']: p2_wins += 1
                    elif m['s2'] > m['s1']: p1_wins += 1

        matches.sort(key=lambda x: datetime.datetime.strptime(x['date'], "%d/%m/%Y") if x['date'] != "nodate" else datetime.datetime.min, reverse=True)
        return {"p1": p1_clean, "p2": p2_clean, "p1_wins": p1_wins, "p2_wins": p2_wins, "total_matches": len(matches), "history": matches}