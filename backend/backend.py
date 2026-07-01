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

    def record_page_view(self, ip_address, user_agent=""):
        if not self.db: return
        try:
            now = datetime.datetime.utcnow() + datetime.timedelta(hours=10)
            today_str = now.strftime("%Y-%m-%d")
            time_str = now.strftime("%I:%M %p")
            
            doc_ref = self.db.collection('daily_traffic').document(today_str)
            doc = doc_ref.get()

            ua_lower = user_agent.lower()
            is_bot = any(bot in ua_lower for bot in ['bot', 'crawl', 'spider', 'slurp', 'inspect', 'lighthouse', 'headless'])
            
            device_name = "Unknown Device"
            if is_bot: device_name = "Search Bot / Crawler"
            elif "iphone" in ua_lower: device_name = "iPhone"
            elif "ipad" in ua_lower: device_name = "iPad"
            elif "android" in ua_lower: device_name = "Android Device"
            elif "macintosh" in ua_lower or "mac os x" in ua_lower: device_name = "Mac"
            elif "windows" in ua_lower: device_name = "Windows PC"
            elif "linux" in ua_lower: device_name = "Linux PC"

            log_entry = { "ip": ip_address, "device": device_name, "is_bot": is_bot, "time": time_str }

            if doc.exists:
                data = doc.to_dict()
                ips = data.get('ips', [])
                bot_ips = data.get('bot_ips', [])
                logs = data.get('visitor_logs', [])

                update_data = {'views': firestore.Increment(1)}
                
                if is_bot and ip_address not in bot_ips:
                    bot_ips.append(ip_address)
                    logs.insert(0, log_entry) 
                    update_data['bot_ips'] = bot_ips
                    update_data['visitor_logs'] = logs[:100] 
                elif not is_bot and ip_address not in ips:
                    ips.append(ip_address)
                    logs.insert(0, log_entry)
                    update_data['ips'] = ips
                    update_data['visitor_logs'] = logs[:100]

                doc_ref.update(update_data)
            else:
                initial_data = {
                    'date': today_str,
                    'views': 1,
                    'ips': [] if is_bot else [ip_address],
                    'bot_ips': [ip_address] if is_bot else [],
                    'visitor_logs': [log_entry]
                }
                doc_ref.set(initial_data)
        except Exception as e:
            logger.error(f"Failed to record page view: {e}")

    def get_traffic_stats(self):
        if not self.db: return {'views': 0, 'uniques': 0, 'humans': 0, 'bots': 0, 'logs': []}
        try:
            now = datetime.datetime.utcnow() + datetime.timedelta(hours=10)
            today_str = now.strftime("%Y-%m-%d")
            
            doc = self.db.collection('daily_traffic').document(today_str).get()
            if doc.exists:
                data = doc.to_dict()
                real_humans = len(data.get('ips', []))
                bots = len(data.get('bot_ips', []))
                return {
                    'views': data.get('views', 0),
                    'uniques': real_humans + bots,
                    'humans': real_humans,
                    'bots': bots,
                    'logs': data.get('visitor_logs', [])
                }
            return {'views': 0, 'uniques': 0, 'humans': 0, 'bots': 0, 'logs': []}
        except:
            return {'views': 0, 'uniques': 0, 'humans': 0, 'bots': 0, 'logs': []}

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

    def search_ratings_central_by_name(self, player_name, target_club=None): 
        if not self.db: return []
        try:
            docs = self.db.collection('rc_directory').stream()
            results = []
            player_name_lower = player_name.lower()
            for d in docs:
                data = d.to_dict()
                if player_name_lower in data.get('search_name', ''):
                    results.append({"id": data['rc_id'], "name": data['name'], "rating": f"{data['rating']}±{data['sd']}", "location": f"{data.get('state', 'QLD')}, Australia", "club": data['club'], "recent_opponents": f"Updated: {data['last_updated']}"})
                    if len(results) >= 10: break
            return results
        except Exception as e:
            return []

    def trigger_background_rc_scrape(self, player_name, rc_id, admin_email):
        def scrape_task():
            stats = self.rc_scraper.deep_scrape_profile(rc_id)
            if stats and self.db:
                safe_id = re.sub(r'[^a-zA-Z0-9]', '_', player_name).lower()
                stats['last_rc_sync'] = firestore.SERVER_TIMESTAMP
                self.db.collection('player_profiles').document(safe_id).set(stats, merge=True)
                self._log_audit(admin_email, 'RC_SYNC', f"Deep Scraped RC Data for {player_name}", {})
        threading.Thread(target=scrape_task).start()
        return True

    def auto_update_stale_rc_profiles(self):
        if not self.db: return
        try:
            now = datetime.datetime.now(datetime.timezone.utc)
            profiles = self.db.collection('player_profiles').where('ratings_central_id', '!=', '').stream()
            for doc in profiles:
                data = doc.to_dict()
                rc_id = data.get('ratings_central_id')
                if not rc_id: continue
                last_sync = data.get('last_rc_sync')
                needs_update = False
                if not last_sync: needs_update = True
                else:
                    try:
                        if (now - last_sync).days >= 4: needs_update = True
                    except: needs_update = True
                if needs_update:
                    logger.info(f"♻️ Auto-updating stale RC profile for: {data.get('name')}")
                    self.trigger_background_rc_scrape(data.get('name'), rc_id, "SYSTEM_AUTO_UPDATER")
        except Exception as e:
            logger.error(f"RC Auto-Update Error: {e}")

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
            self.trigger_background_rc_scrape(player_name, ratings_central_id, admin_email)
            return True
        except: return False

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

    def admin_override_rating(self, player_id, new_rating, sd_override=None, retroactive=True, admin_email="Unknown"):
        if not self.db: return False
        try:
            final_sd = float(sd_override) if sd_override else 65.0; player_name = self.id_to_name.get(player_id)
            if not player_name:
                for n, i in self.player_ids.items():
                    if i == player_id: player_name = n; break
            if not player_name: return False

            payload = {'name': player_name, 'rating': float(new_rating), 'rd': final_sd, 'vol': 0.06, 'date_str': '1900-01-01' if retroactive else datetime.datetime.now().strftime("%Y-%m-%d"), 'timestamp': firestore.SERVER_TIMESTAMP, 'author': admin_email, 'retroactive': retroactive}
            self.db.collection('rating_overrides').document(player_name).set(payload)
            
            # --- NEW: Persist straight to the player profile object ---
            safe_id = re.sub(r'[^a-zA-Z0-9]', '_', player_name).lower()
            self.db.collection('player_profiles').document(safe_id).set({
                'rating': float(new_rating),
                'sd': final_sd,
                'last_updated': firestore.SERVER_TIMESTAMP
            }, merge=True)
            
            # Immediately update the in-memory rating engine so it reflects in UI without a full sync
            if player_name not in self.rating_engine.players:
                self.rating_engine.get_rating(player_name)
            self.rating_engine.players[player_name]['rating'] = float(new_rating)
            self.rating_engine.players[player_name]['rd'] = final_sd

            self._log_audit(admin_email, 'OVERRIDE_RATING', f"Forced {player_name} to Rating: {new_rating}, SD: {final_sd}", {"name": player_name})
            
            # Only recalculate everyone if the override is retroactive (needs to replay matches)
            if retroactive:
                threading.Thread(target=self.refresh_data, daemon=True).start()
                
            return True
        except Exception as e:
            logger.error(f"Override Rating Error: {e}")
            return False

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
    
    def get_seasons(self): 
        seasons = set()
        if hasattr(self, 'seasons_list') and self.seasons_list: seasons.update(self.seasons_list)
        if hasattr(self, 'seasons') and self.seasons: seasons.update(self.seasons)
        if hasattr(self, 'weekly_matches') and isinstance(self.weekly_matches, dict): seasons.update(self.weekly_matches.keys())
        if hasattr(self, 'season_stats') and isinstance(self.season_stats, dict): seasons.update(self.season_stats.keys())
        clean_seasons = sorted([str(s) for s in seasons if str(s) != "Career"], reverse=True)
        return clean_seasons if clean_seasons else ["Winter 2026"] 
        
    def get_divisions(self): 
        divs = set()
        if hasattr(self, 'divisions_list') and self.divisions_list: divs.update(self.divisions_list)
        if hasattr(self, 'divisions') and self.divisions: divs.update(self.divisions)
        if hasattr(self, 'weekly_matches') and isinstance(self.weekly_matches, dict):
            for s_data in self.weekly_matches.values():
                if isinstance(s_data, dict):
                    for w_data in s_data.values():
                        if isinstance(w_data, list):
                            for m in w_data:
                                if 'division' in m: divs.add(m['division'])
        clean_divs = sorted(list(divs))
        return clean_divs if clean_divs else ["Division 1"] 
        
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
        
        if player_name not in data:
            if season != "Career" and player_name in players_data:
                raw = {'combined': {}, 'regular': {}, 'fillin': {}}
            else:
                return None
        else:
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
            
        def calculate_insights(hist):
            sweeps = []
            blowouts = []
            close_calls = []
            big_swings = []
            strong_starts = []
            merciless = []
            comebacks = []
            letdowns = []
            nail_biters = []
            
            for m in hist:
                try:
                    score = m.get('score', '')
                    if not score: continue
                    parts = score.split('-')
                    if len(parts) != 2: continue
                    sets_for = int(parts[0])
                    sets_against = int(parts[1])
                    result = m.get('result', '')
                    opp = m.get('opponent', 'Unknown')
                    delta = m.get('delta', 0)
                    
                    match_data = {
                        'opponent': opp,
                        'result': f"{result}: {score}",
                        'season': m.get('season', 'Career'),
                        'delta': delta,
                        'details': m.get('details', '')
                    }
                    
                    if result == "Win" and sets_against == 0:
                        sweeps.append(match_data)
                    if result == "Loss" and sets_for == 0:
                        blowouts.append(match_data)
                    if abs(sets_for - sets_against) == 1:
                        close_calls.append(match_data)
                    if abs(delta) >= 70:
                        big_swings.append(match_data)
                        
                    details = m.get('details', '')
                    if details:
                        games = [g.strip() for g in details.split(',') if g.strip()]
                        if games:
                            # Game 1 strong start
                            g1 = games[0].split('-')
                            if len(g1) == 2:
                                if int(g1[0]) > int(g1[1]):
                                    strong_starts.append(match_data)
                                    
                            # Merciless
                            has_zero = False
                            for g in games:
                                pts = g.split('-')
                                if len(pts) == 2 and (int(pts[0]) == 0 or int(pts[1]) == 0):
                                    has_zero = True
                            if has_zero:
                                merciless.append(match_data)
                                
                            # Nail Biters
                            last_g = games[-1].split('-')
                            if len(last_g) == 2:
                                p1_pts, p2_pts = int(last_g[0]), int(last_g[1])
                                if p1_pts >= 10 and p2_pts >= 10 and abs(p1_pts - p2_pts) <= 2:
                                    nail_biters.append(match_data)
                                elif (p1_pts == 11 and p2_pts == 9) or (p1_pts == 9 and p2_pts == 11):
                                    nail_biters.append(match_data)
                except Exception:
                    pass
            
            # Sort all lists by some logical criteria, or just reverse them to show newest first
            def format_insight(arr):
                arr.reverse()
                return arr
                
            return {
                'sweeps': format_insight(sweeps),
                'blowouts': format_insight(blowouts),
                'close_calls': format_insight(close_calls),
                'big_swings': format_insight(big_swings),
                'strong_starts': format_insight(strong_starts),
                'merciless': format_insight(merciless),
                'comebacks': format_insight(comebacks),
                'letdowns': format_insight(letdowns),
                'nail_biters': format_insight(nail_biters)
            }

        # User requested insights to ALWAYS be career history, even if a specific season is selected for match history
        career_raw = players_data.get(player_name, {})
        career_hist = career_raw.get('combined', {}).get('history', [])
        insights_data = calculate_insights(career_hist)
        
        # We also need the total career matches to return so the frontend can calculate percentages accurately
        total_career_matches = len(career_hist)
        total_career_wins = sum(1 for m in career_hist if m.get('result') == "Win")
        total_career_losses = total_career_matches - total_career_wins
        
        first_year = "unknown"
        if career_hist:
            oldest_match = career_hist[-1]  # or [0] depending on how it's sorted, let's just find min year
            first_year_candidates = []
            for m in career_hist:
                date_str = m.get('date', '')
                if date_str and '/' in date_str:
                    parts = date_str.split('/')
                    if len(parts) == 3:
                        first_year_candidates.append(parts[2])
                elif m.get('season'):
                    season = str(m.get('season'))
                    import re
                    match = re.search(r'\d{4}', season)
                    if match:
                        first_year_candidates.append(match.group(0))
            if first_year_candidates:
                first_year = min(first_year_candidates)

        return {
            'name': player_name, 
            'rating': int(rat.get('rating', 1500)), 
            'rd': int(rat.get('rd', 350)), 
            'vol': rat.get('vol', 0.06), 
            'combined': format_bucket(raw.get('combined', {})), 
            'regular': format_bucket(raw.get('regular', {})), 
            'fillin': format_bucket(raw.get('fillin', {})), 
            'insights': insights_data,
            'career_matches': total_career_matches,
            'career_wins': total_career_wins,
            'career_losses': total_career_losses,
            'first_year': first_year,
            'peterman_id': players_data.get(player_name, {}).get('peterman_id', '')
        }

    def simulate_match_public(self, p1, p2, s1, s2, custom_k_win=None, custom_k_loss=None):
        p1_clean = self._clean_name(p1)
        p2_clean = self._clean_name(p2)
        try: s1 = int(s1); s2 = int(s2)
        except: return {"error": "Invalid scores"}

        kw = float(custom_k_win) if custom_k_win else self.k_win
        kl = float(custom_k_loss) if custom_k_loss else self.k_loss

        temp_engine = RatingEngine() 
        p1_stats = self.rating_engine.get_rating(p1_clean).copy()
        p2_stats = self.rating_engine.get_rating(p2_clean).copy()
        temp_engine.players[p1_clean] = p1_stats
        temp_engine.players[p2_clean] = p2_stats
        res = temp_engine.update_match(p1_clean, p2_clean, s1, s2, '', kw, kl, True)

        return {
            "p1": {"name": p1_clean, "old_rating": res['p1_before'], "new_rating": res['p1_after'], "delta": res['p1_delta'], "old_rd": res['p1_rd_before'], "new_rd": res['p1_rd_after'], "rd_delta": res['p1_rd_after'] - res['p1_rd_before']}, 
            "p2": {"name": p2_clean, "old_rating": res['p2_before'], "new_rating": res['p2_after'], "delta": res['p2_delta'], "old_rd": res['p2_rd_before'], "new_rd": res['p2_rd_after'], "rd_delta": res['p2_rd_after'] - res['p2_rd_before']},
            "h2h": self.get_head_to_head(p1_clean, p2_clean)
        }

    def admin_glicko_math(self, p1, p2, s1, s2):
        return self.simulate_match_public(p1, p2, s1, s2)
        
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
        
        rat1 = self.rating_engine.get_rating(p1_clean)
        rat2 = self.rating_engine.get_rating(p2_clean)
        r1 = rat1.get('rating', 1500)
        r2 = rat2.get('rating', 1500)
        rd1 = rat1.get('rd', 350)
        rd2 = rat2.get('rd', 350)
        
        # Complex Odds Algorithm
        import math
        
        # 1. Rating & SD based probability (Glicko-style expectation)
        q = math.log(10) / 400
        rd_combined = math.sqrt(rd1**2 + rd2**2)
        g_rd = 1 / math.sqrt(1 + 3 * (q * rd_combined / math.pi)**2)
        rating_prob_p1 = 1 / (1 + 10 ** (g_rd * (r2 - r1) / 400))
        
        # 2. Match History and Closeness (Sets) with Recency Weighting
        total_m = len(matches)
        h2h_prob_p1 = 0.5
        if total_m > 0:
            weighted_wins_p1 = 0
            weighted_total = 0
            
            weighted_sets_p1 = 0
            weighted_sets_total = 0
            
            for i, m in enumerate(matches):
                # Decay factor: more recent matches (lower index) carry more weight
                weight = 0.85 ** i
                
                if m['p1'] == p1_clean:
                    s1_p1, s2_p2 = m['s1'], m['s2']
                else:
                    s1_p1, s2_p2 = m['s2'], m['s1']
                    
                if s1_p1 > s2_p2:
                    weighted_wins_p1 += weight
                weighted_total += weight
                
                weighted_sets_p1 += s1_p1 * weight
                weighted_sets_total += (s1_p1 + s2_p2) * weight
                    
            h2h_win_rate = weighted_wins_p1 / weighted_total if weighted_total > 0 else 0.5
            set_ratio = weighted_sets_p1 / weighted_sets_total if weighted_sets_total > 0 else 0.5
            
            # Blend pure win rate with set closeness
            h2h_prob_p1 = (h2h_win_rate * 0.7) + (set_ratio * 0.3)
            
        # 3. Blend them together based on sample size of head-to-head matches
        # The more they play, the more H2H matters (caps at 60% weight after 15+ matches)
        h2h_weight = min(total_m / 25.0, 0.6) 
        
        final_prob_p1 = (rating_prob_p1 * (1 - h2h_weight)) + (h2h_prob_p1 * h2h_weight)
        
        # Cap probabilities to avoid 1.00 odds
        final_prob_p1 = max(0.01, min(0.99, final_prob_p1))
        final_prob_p2 = 1 - final_prob_p1
        
        odds_p1 = round(1 / final_prob_p1, 2)
        odds_p2 = round(1 / final_prob_p2, 2)
        
        return {
            "p1": p1_clean, "p2": p2_clean, 
            "p1_wins": p1_wins, "p2_wins": p2_wins, 
            "total_matches": total_m, 
            "history": matches,
            "p1_rating": int(r1), "p1_rd": int(rd1),
            "p2_rating": int(r2), "p2_rd": int(rd2),
            "p1_odds": f"{odds_p1:.2f}", "p2_odds": f"{odds_p2:.2f}",
            "p1_prob": round(final_prob_p1 * 100, 1),
            "p2_prob": round(final_prob_p2 * 100, 1)
        }

    def get_rating_history(self, player_name):
        if not self.db: return []
        try:
            p_clean = self._clean_name(player_name)
            docs = self.db.collection('player_rating_history').where('player_name', '==', p_clean).limit(200).stream()
            history = []
            for d in docs:
                data = d.to_dict()
                date_val = data.get('date', 'Unknown')
                if hasattr(data.get('timestamp'), 'strftime'):
                    date_val = data.get('timestamp').strftime("%d/%m/%Y")
                history.append({
                    "date": date_val,
                    "rating": round(data.get('rating', 0), 1),
                    "sd": round(data.get('sd', 0), 1),
                    "rating_change": round(data.get('rating_change', 0), 1),
                    "sd_change": round(data.get('sd_change', 0), 1),
                    "opponent": data.get('opponent', ''),
                    "result_str": data.get('result_str', ''),
                    "is_decay": data.get('is_decay', False),
                    "_timestamp_val": data.get('timestamp').timestamp() if hasattr(data.get('timestamp'), 'timestamp') else 0
                })
            
            history.sort(key=lambda x: x.get('_timestamp_val', 0), reverse=True)
            
            if not history and hasattr(self, 'match_history_log'):
                # Fallback to generated match history log for players without specific history ledger items
                for m in reversed(self.match_history_log):
                    if p_clean in m['home_players'] or p_clean in m['away_players']:
                        is_home = p_clean in m['home_players']
                        history.append({
                            "date": m.get('date', 'Unknown'),
                            "rating": round(m.get('p1_after') if is_home else m.get('p2_after'), 1),
                            "sd": round(m.get('p1_rd_after') if is_home else m.get('p2_rd_after'), 1),
                            "rating_change": round(m.get('p1_delta') if is_home else m.get('p2_delta'), 1),
                            "sd_change": round((m.get('p1_rd_after') - m.get('p1_rd_before')) if is_home else (m.get('p2_rd_after') - m.get('p2_rd_before')), 1),
                            "opponent": m.get('p2') if is_home else m.get('p1'),
                            "result_str": m.get('score', ''),
                            "is_decay": False
                        })
                
            return history[:100]
        except Exception as e:
            logger.error(f"Error fetching rating history: {e}")
            return []

    def admin_recalculate_recent(self, admin_email="Unknown"):
        if not self.db: return {"success": False, "error": "DB Offline"}
        try:
            cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=21)
            
            # 1. Fetch matches ONLY from the last 21 days
            matches_ref = self.db.collection('match_results').where('status', '==', 'approved').stream()
            matches = []
            for m in matches_ref:
                data = m.to_dict()
                timestamp = data.get('timestamp')
                if timestamp and hasattr(timestamp, 'replace') and timestamp.replace(tzinfo=datetime.timezone.utc) >= cutoff:
                    data['id'] = m.id
                    date_obj = self._parse_date(data.get('date'))
                    data['_sort_date'] = date_obj if date_obj else datetime.date.min
                    matches.append(data)
            
            matches.sort(key=lambda x: (x['_sort_date'], x.get('timestamp')))
            
            # 2. Get Baselines from 21 days ago
            from backend.glicko import RatingEngine
            fresh_engine = RatingEngine()
            
            involved_players = set()
            for m in matches:
                involved_players.update(m.get('home_players', []))
                involved_players.update(m.get('away_players', []))
                
            for p in involved_players:
                p_clean = self._clean_name(p)
                hist_docs = self.db.collection('player_rating_history').where('player_name', '==', p_clean).where('timestamp', '<', cutoff).order_by('timestamp', direction=firestore.Query.DESCENDING).limit(1).stream()
                
                baseline_found = False
                for doc in hist_docs:
                    d = doc.to_dict()
                    fresh_engine.players[p_clean] = {'rating': d.get('rating', 1500.0), 'rd': d.get('sd', 350.0), 'vol': 0.06}
                    baseline_found = True
                if not baseline_found:
                    fresh_engine.players[p_clean] = {'rating': 1500.0, 'rd': 350.0, 'vol': 0.06}

            # 3. Wipe ONLY recent history ledger entries
            recent_hist = self.db.collection('player_rating_history').where('timestamp', '>=', cutoff).stream()
            batch = self.db.batch()
            deletes = 0
            for doc in recent_hist:
                batch.delete(doc.reference)
                deletes += 1
                if deletes >= 400:
                    batch.commit()
                    batch = self.db.batch()
                    deletes = 0
            if deletes > 0: batch.commit()

            # 4. Replay the last 3 weeks
            batch = self.db.batch()
            writes = 0
            for m in matches:
                p1 = self._clean_name(m.get('home_players', [''])[0])
                p2 = self._clean_name(m.get('away_players', [''])[0])
                s1 = m.get('live_home_sets', 0)
                s2 = m.get('live_away_sets', 0)
                
                if not p1 or not p2: continue
                res = fresh_engine.update_match(p1, p2, s1, s2, m['id'], self.k_win, self.k_loss, True)
                
                for p, s_after, rd_after, opp, s_str, r_delta, sd_delta in [
                    (p1, res['p1_after'], res['p1_rd_after'], p2, f"{s1}-{s2}", res['p1_delta'], res['p1_rd_after'] - res['p1_rd_before']),
                    (p2, res['p2_after'], res['p2_rd_after'], p1, f"{s2}-{s1}", res['p2_delta'], res['p2_rd_after'] - res['p2_rd_before'])
                ]:
                    ref = self.db.collection('player_rating_history').document()
                    batch.set(ref, {'player_name': p, 'rating': s_after, 'sd': rd_after, 'rating_change': r_delta, 'sd_change': sd_delta, 'opponent': opp, 'result_str': s_str, 'date': m.get('date'), 'timestamp': m.get('timestamp') or firestore.SERVER_TIMESTAMP, 'is_decay': False})
                    writes += 1
                    if writes >= 400:
                        batch.commit()
                        batch = self.db.batch()
                        writes = 0
            if writes > 0: batch.commit()
            
            self._log_audit(admin_email, 'RECALCULATE_RECENT', f"Rebuilt last 21 days ({len(matches)} matches).", {})
            threading.Thread(target=self.refresh_data, daemon=True).start()
            return {"success": True, "message": f"Successfully recalculated last 3 weeks."}
        except Exception as e: return {"success": False, "error": str(e)}

    def admin_recalculate_ratings(self, admin_email="Unknown"):
        if not self.db: return {"success": False, "error": "DB Offline"}
        try:
            matches_ref = self.db.collection('match_results').where('status', '==', 'approved').stream()
            matches = []
            for m in matches_ref:
                data = m.to_dict()
                data['id'] = m.id
                date_obj = self._parse_date(data.get('date'))
                data['_sort_date'] = date_obj if date_obj else datetime.date.min
                matches.append(data)
            
            matches.sort(key=lambda x: (x['_sort_date'], x.get('timestamp') or datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)))
            
            from backend.glicko import RatingEngine
            fresh_engine = RatingEngine()
            
            batch = self.db.batch()
            history_writes = 0
            
            # --- INACTIVITY CREEP TRACKING ---
            import math
            last_played = {}
            c_system = 15.6  # Glicko decay constant 
            grace_period_days = 14
            MAX_RD = 350.0
            
            # Wipe old history ledger before rebuilding
            old_hist = self.db.collection('player_rating_history').stream()
            for doc in old_hist:
                batch.delete(doc.reference)
                history_writes += 1
                if history_writes >= 400:
                    batch.commit()
                    batch = self.db.batch()
                    history_writes = 0
            
            for m in matches:
                p1 = m.get('home_players', [''])[0]
                p2 = m.get('away_players', [''])[0]
                s1 = m.get('live_home_sets', 0)
                s2 = m.get('live_away_sets', 0)
                
                if not p1 or not p2: continue
                p1_clean = self._clean_name(p1)
                p2_clean = self._clean_name(p2)
                match_date = m['_sort_date']
                
                # --- APPLY INACTIVITY PENALTY BEFORE MATCH ---
                for p_clean in [p1_clean, p2_clean]:
                    if p_clean in last_played:
                        days_missed = (match_date - last_played[p_clean]).days
                        if days_missed > grace_period_days:
                            weeks_missed = (days_missed - grace_period_days) / 7.0
                            old_stats = fresh_engine.get_rating(p_clean)
                            old_rd = old_stats.get('rd', 350.0)
                            
                            new_rd = min(math.sqrt(old_rd**2 + (c_system**2 * weeks_missed)), MAX_RD)
                            
                            if new_rd > old_rd + 1.0:
                                fresh_engine.players[p_clean]['rd'] = new_rd
                                decay_ref = self.db.collection('player_rating_history').document()
                                batch.set(decay_ref, {
                                    'player_name': p_clean, 'rating': old_stats['rating'], 'sd': new_rd,
                                    'rating_change': 0, 'sd_change': new_rd - old_rd,
                                    'opponent': 'Inactivity Decay', 'result_str': f'{days_missed} days missed',
                                    'date': match_date.strftime("%d/%m/%Y"),
                                    'timestamp': m.get('timestamp') or firestore.SERVER_TIMESTAMP,
                                    'is_decay': True
                                })
                                history_writes += 1

                res = fresh_engine.update_match(p1_clean, p2_clean, s1, s2, m['id'], self.k_win, self.k_loss, True)
                last_played[p1_clean] = match_date
                last_played[p2_clean] = match_date
                
                # --- RECORD MATCH LEDGER ---
                for p, s_after, rd_after, opp, s_str, r_delta, sd_delta in [
                    (p1_clean, res['p1_after'], res['p1_rd_after'], p2_clean, f"{s1}-{s2}", res['p1_delta'], res['p1_rd_after'] - res['p1_rd_before']),
                    (p2_clean, res['p2_after'], res['p2_rd_after'], p1_clean, f"{s2}-{s1}", res['p2_delta'], res['p2_rd_after'] - res['p2_rd_before'])
                ]:
                    h_ref = self.db.collection('player_rating_history').document()
                    batch.set(h_ref, {
                        'player_name': p, 'rating': s_after, 'sd': rd_after,
                        'rating_change': r_delta, 'sd_change': sd_delta,
                        'opponent': opp, 'result_str': s_str,
                        'date': m.get('date'), 'timestamp': m.get('timestamp') or firestore.SERVER_TIMESTAMP,
                        'is_decay': False
                    })
                    history_writes += 1
                    if history_writes >= 400:
                        batch.commit()
                        batch = self.db.batch()
                        history_writes = 0
            
            if history_writes > 0: batch.commit()
            
            self._log_audit(admin_email, 'RECALCULATE_ALL', f"Replayed and recalculated {len(matches)} matches with decay.", {})
            threading.Thread(target=self.refresh_data, daemon=True).start()
            return {"success": True, "message": f"Successfully recalculated {len(matches)} matches."}
        except Exception as e:
            logger.error(f"Recalc Error: {e}")
            return {"success": False, "error": str(e)}