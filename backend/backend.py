# backend/backend.py
import firebase_admin
from firebase_admin import credentials, firestore, auth
import datetime, re, os, json, sys, logging, random, string, hashlib, threading

from backend.glicko import RatingEngine, clamp_public_rating, parse_division_number, DIVISION_LABELS, VOLATILITY_GUIDE
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
        self.player_rating_history_memory = {}
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

    def refresh_data(self, commit_history=False):
        if hasattr(self, 'sync_engine') and self.sync_engine:
            try:
                self.sync_engine.run_sync(commit_history=commit_history)
            except TypeError:
                self.sync_engine.run_sync()

    def _clean_name(self, name):
        return self.alias_map.get(" ".join(str(name).split()).lower(), " ".join(str(name).split()).title()) if name else ""

    def _get_val(self, row, keys, default=''):
        row_keys_norm = {k.strip().lower(): k for k in row.keys()}
        for k in keys:
            if k.strip().lower() in row_keys_norm:
                return row[row_keys_norm[k.strip().lower()]]
        return default

    def _parse_date(self, date_str):
        if not date_str:
            return None
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d"):
            try:
                return datetime.datetime.strptime(str(date_str).strip(), fmt).date()
            except Exception:
                continue
        return None

    def _generate_player_id(self):
        return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(6))

    def _slugify(self, text):
        return re.sub(r'[^a-z0-9]', '', str(text).lower())

    def _extract_week(self, text):
        match = re.search(r'\d+', str(text))
        return match.group() if match else str(text).strip().lower()

    def _format_div_label(self, div_num):
        if div_num in DIVISION_LABELS:
            return DIVISION_LABELS[div_num]
        return "Division 3 (601–900)"

    def get_volatility_guide(self):
        return VOLATILITY_GUIDE

    # ==========================================
    # WEB TRAFFIC, BOT DETECTION & DEVICE ANALYTICS
    # ==========================================
    def record_page_view(self, ip_address, user_agent=""):
        if not self.db:
            return
        try:
            now = datetime.datetime.utcnow() + datetime.timedelta(hours=10)
            today_str = now.strftime("%Y-%m-%d")
            time_str = now.strftime("%I:%M %p")
            
            doc_ref = self.db.collection('daily_traffic').document(today_str)
            doc = doc_ref.get()

            ua_lower = user_agent.lower()
            is_bot = any(bot in ua_lower for bot in ['bot', 'crawl', 'spider', 'slurp', 'inspect', 'lighthouse', 'headless', 'python-requests', 'curl', ' render'])
            
            device_name = "Unknown Device"
            if is_bot:
                device_name = "Search Bot / Crawler"
            elif "iphone" in ua_lower:
                device_name = "iPhone (iOS)"
            elif "ipad" in ua_lower:
                device_name = "iPad (iPadOS)"
            elif "android" in ua_lower:
                device_name = "Android Mobile"
            elif "macintosh" in ua_lower or "mac os x" in ua_lower:
                device_name = "Mac (macOS)"
            elif "windows" in ua_lower:
                device_name = "Windows PC"
            elif "linux" in ua_lower:
                device_name = "Linux PC"

            log_entry = {"ip": ip_address, "device": device_name, "is_bot": is_bot, "time": time_str}

            if doc.exists:
                data = doc.to_dict()
                ips = data.get('ips', [])
                bot_ips = data.get('bot_ips', [])
                logs = data.get('visitor_logs', [])

                update_data = {'views': firestore.Increment(1)}
                
                if is_bot:
                    if ip_address not in bot_ips:
                        bot_ips.append(ip_address)
                        update_data['bot_ips'] = bot_ips
                    logs.insert(0, log_entry)
                    update_data['visitor_logs'] = logs[:150]
                else:
                    if ip_address not in ips:
                        ips.append(ip_address)
                        update_data['ips'] = ips
                    logs.insert(0, log_entry)
                    update_data['visitor_logs'] = logs[:150]

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
        if not self.db:
            return {'views': 0, 'uniques': 0, 'humans': 0, 'bots': 0, 'logs': [], 'device_breakdown': {}}
        try:
            now = datetime.datetime.utcnow() + datetime.timedelta(hours=10)
            today_str = now.strftime("%Y-%m-%d")
            
            doc = self.db.collection('daily_traffic').document(today_str).get()
            if doc.exists:
                data = doc.to_dict()
                real_humans = len(data.get('ips', []))
                bots = len(data.get('bot_ips', []))
                logs = data.get('visitor_logs', [])
                
                device_counts = {}
                for l in logs:
                    if not l.get('is_bot'):
                        dev = l.get('device', 'Unknown Device')
                        device_counts[dev] = device_counts.get(dev, 0) + 1

                return {
                    'views': data.get('views', 0),
                    'uniques': real_humans + bots,
                    'humans': real_humans,
                    'bots': bots,
                    'logs': logs,
                    'device_breakdown': device_counts
                }
            return {'views': 0, 'uniques': 0, 'humans': 0, 'bots': 0, 'logs': [], 'device_breakdown': {}}
        except Exception:
            return {'views': 0, 'uniques': 0, 'humans': 0, 'bots': 0, 'logs': [], 'device_breakdown': {}}

    # ==========================================
    # 3-TIER ROLE-BASED ACCESS CONTROL (RBAC)
    # ==========================================
    def _get_email_tier(self, email, role=None):
        email_clean = str(email or '').lower().strip()
        role_clean = str(role or '').lower().strip()
        if email_clean == SUPER_ADMIN_EMAIL.lower() or role_clean == 'god':
            return 1
        if email_clean.startswith('gctta') or 'gctta' in email_clean or role_clean in ['gm', 'general_manager', 'super_admin', 'temp_super_admin']:
            return 2
        if role_clean in ['admin', 'temp_admin', 'data_entry', 'moderator']:
            return 3
        return 99

    def verify_admin_token(self, token):
        if not self.db:
            return None
        try:
            decoded_token = auth.verify_id_token(token)
            email = decoded_token.get('email')
            if not email:
                return None
            email_lower = email.lower().strip()

            # Tier 1: God Mode (Jakob)
            if email_lower == SUPER_ADMIN_EMAIL.lower():
                self.db.collection('admin_users').document(email_lower).set({
                    'email': email_lower,
                    'role': 'god',
                    'tier': 1
                }, merge=True)
                return {'email': email_lower, 'role': 'god', 'tier': 1}

            doc_ref = self.db.collection('admin_users').document(email_lower)
            doc = doc_ref.get()

            # Tier 2 Auto-Recognition for permanent GCTTA General Manager account
            if 'gctta' in email_lower:
                if not doc.exists or doc.to_dict().get('role') == 'pending':
                    doc_ref.set({'email': email_lower, 'role': 'gm', 'tier': 2}, merge=True)
                    return {'email': email_lower, 'role': 'gm', 'tier': 2}

            if doc.exists:
                data = doc.to_dict()
                role = data.get('role', 'pending')
                expires_at = data.get('expires_at')

                if expires_at and datetime.datetime.now(datetime.timezone.utc) > expires_at:
                    if role == 'temp_super_admin':
                        role = 'admin'
                        doc_ref.update({'role': 'admin', 'expires_at': firestore.DELETE_FIELD})
                    elif role in ['temp_admin', 'data_entry']:
                        role = 'pending'
                        doc_ref.update({'role': 'pending', 'expires_at': firestore.DELETE_FIELD})

                return {'email': email_lower, 'role': role, 'tier': self._get_email_tier(email_lower, role)}
            else:
                doc_ref.set({'email': email_lower, 'role': 'pending', 'tier': 99})
                return {'email': email_lower, 'role': 'pending', 'tier': 99}
        except Exception as e:
            logger.error(f"verify_admin_token error: {e}")
            return None

    def get_admin_users(self):
        if not self.db:
            return []
        try: 
            users = []
            for d in self.db.collection('admin_users').stream():
                data = d.to_dict()
                if data.get('expires_at') and hasattr(data.get('expires_at'), 'strftime'):
                    data['expires_at_str'] = data['expires_at'].strftime('%Y-%m-%d %H:%M')
                data['tier'] = self._get_email_tier(data.get('email'), data.get('role'))
                users.append(data)
            return users
        except Exception:
            return []

    def approve_admin(self, email, action, days=7):
        if not self.db:
            return False
        try:
            email_lower = str(email or '').lower().strip()
            if email_lower == SUPER_ADMIN_EMAIL.lower():
                return False
            ref = self.db.collection('admin_users').document(email_lower)
            now_utc = datetime.datetime.now(datetime.timezone.utc)

            if action == 'approve':
                ref.set({'email': email_lower, 'role': 'admin', 'expires_at': firestore.DELETE_FIELD}, merge=True)
            elif action == 'make_gm':
                ref.set({'email': email_lower, 'role': 'gm', 'expires_at': firestore.DELETE_FIELD}, merge=True)
            elif action in ['temp_admin', 'temp_data_entry']:
                ref.set({'email': email_lower, 'role': 'temp_admin', 'expires_at': now_utc + datetime.timedelta(days=int(days))}, merge=True)
            elif action == 'temp_super':
                ref.set({'email': email_lower, 'role': 'temp_super_admin', 'expires_at': now_utc + datetime.timedelta(days=int(days))}, merge=True)
            elif action == 'revoke':
                ref.set({'email': email_lower, 'role': 'pending', 'expires_at': firestore.DELETE_FIELD}, merge=True)
            elif action == 'delete':
                ref.delete()
            return True
        except Exception:
            return False

    def _log_audit(self, admin_email, action_type, description, undo_payload):
        if not self.db:
            return
        try:
            self.db.collection('admin_audit_logs').add({
                'admin': admin_email,
                'action': action_type,
                'description': description,
                'undo_payload': json.dumps(undo_payload),
                'timestamp': firestore.SERVER_TIMESTAMP,
                'status': 'active'
            })
        except Exception:
            pass

    def get_audit_logs(self):
        if not self.db:
            return []
        try:
            logs = []
            aest_tz = datetime.timezone(datetime.timedelta(hours=10))
            for d in self.db.collection('admin_audit_logs').order_by('timestamp', direction=firestore.Query.DESCENDING).limit(150).stream():
                data = d.to_dict()
                data['id'] = d.id
                ts = data.get('timestamp')
                data['time_str'] = ts.astimezone(aest_tz).strftime("%d/%m/%Y %I:%M:%S %p") if ts and hasattr(ts, 'astimezone') else "Unknown Time"
                data['timestamp'] = str(ts)
                logs.append(data)
            return logs
        except Exception:
            return []

    def undo_audit_action(self, log_id, caller_admin_email):
        if not self.db:
            return False
        try:
            doc_ref = self.db.collection('admin_audit_logs').document(log_id)
            doc = doc_ref.get()
            if not doc.exists or doc.to_dict().get('status') == 'undone':
                return False
            data = doc.to_dict()

            caller_clean = str(caller_admin_email or '').lower().strip()
            author_clean = str(data.get('admin') or '').lower().strip()
            if caller_clean != SUPER_ADMIN_EMAIL.lower() and author_clean == SUPER_ADMIN_EMAIL.lower():
                logger.warning(f"RBAC Blocked: {caller_clean} attempted to undo God Mode action {log_id}")
                return False

            action = data.get('action')
            payload = json.loads(data.get('undo_payload', '{}'))
            
            if action == 'OVERRIDE_RATING':
                target_name = payload.get('name', '')
                self.db.collection('rating_overrides').document(target_name).delete()
                self.db.collection('rating_overrides').document(re.sub(r'[^a-zA-Z0-9]', '_', target_name).lower()).delete()
            elif action in ['UPDATE_MATCH', 'ADD_MATCH']: 
                if payload.get('correction_id'):
                    self.db.collection('match_corrections').document(payload.get('correction_id')).delete()
                if payload.get('result_id'):
                    self.db.collection('match_results').document(payload.get('result_id')).delete()
            elif action == 'OVERRIDE_DELTAS':
                self.db.collection('match_delta_overrides').document(payload.get('match_id')).delete()
            elif action in ['FORCE_FINISH_LIVE', 'WIPE_LIVE']:
                if action == 'FORCE_FINISH_LIVE' and payload.get('result_id'):
                    self.db.collection('match_results').document(payload.get('result_id')).delete()
                if payload.get('schedule_id') and payload.get('fixture_data'):
                    self.db.collection('fixture_schedule').document(payload.get('schedule_id')).set(payload.get('fixture_data'))
            elif action == 'BULK_DATE_FIX':
                if 'rule_id' in payload:
                    self.db.collection('bulk_date_rules').document(payload['rule_id']).delete()
            elif action == 'UPDATE_MATH':
                self.db.collection('system_config').document('main').set({
                    'k_win_scale': float(payload.get('old_k_win', 1.0)),
                    'k_loss_scale': float(payload.get('old_k_loss', 1.4))
                }, merge=True)
                self.k_win = float(payload.get('old_k_win', 1.0))
                self.k_loss = float(payload.get('old_k_loss', 1.4))
                
            doc_ref.update({'status': 'undone', 'undone_by': caller_admin_email})
            self.refresh_data()
            return True
        except Exception as e:
            logger.error(f"Undo Audit Error: {e}")
            return False

    # ==========================================
    # MATCH TIME MACHINE & SEARCH HISTORY
    # ==========================================
    def admin_search_history(self, query="", source_filter="All"):
        try:
            q_clean = str(query or "").lower().strip()
            matches = []
            history_source = getattr(self, 'match_history_log', getattr(self, 'match_history', []))

            for idx, m in enumerate(reversed(history_source)):
                p1 = str(m.get('p1', '')).lower()
                p2 = str(m.get('p2', '')).lower()
                match_id = str(m.get('match_id', m.get('id', f"m_{idx}"))).lower()
                date_str = str(m.get('date', '')).lower()

                if q_clean and (q_clean not in p1 and q_clean not in p2 and q_clean not in match_id and q_clean not in date_str):
                    continue

                source_type = m.get('source', 'Spreadsheet')
                if source_filter != "All":
                    if source_filter.lower() == "spreadsheet" and source_type.lower() != "spreadsheet":
                        continue
                    elif source_filter.lower() in ["live app", "app"] and source_type.lower() == "spreadsheet":
                        continue

                entry = dict(m)
                entry['id'] = m.get('id', m.get('match_id', f"hist_{idx}"))
                entry['match_id'] = m.get('match_id', entry['id'])
                entry['game_history'] = m.get('game_history', m.get('details', ''))
                matches.append(entry)

                if len(matches) >= 100:
                    break

            return matches
        except Exception as e:
            logger.error(f"admin_search_history error: {e}")
            return []

    def search_ratings_central_by_name(self, player_name, target_club=None): 
        if not self.db:
            return []
        try:
            docs = self.db.collection('rc_directory').stream()
            results = []
            player_name_lower = player_name.lower()
            for d in docs:
                data = d.to_dict()
                if player_name_lower in data.get('search_name', ''):
                    results.append({
                        "id": data['rc_id'],
                        "name": data['name'],
                        "rating": f"{data['rating']}±{data['sd']}",
                        "location": f"{data.get('state', 'QLD')}, Australia",
                        "club": data['club'],
                        "recent_opponents": f"Updated: {data['last_updated']}"
                    })
                    if len(results) >= 10:
                        break
            return results
        except Exception:
            return []

    def trigger_background_rc_scrape(self, player_name, rc_id, admin_email):
        def scrape_task():
            stats = self.rc_scraper.deep_scrape_profile(rc_id)
            if stats and self.db:
                safe_id = re.sub(r'[^a-zA-Z0-9]', '_', player_name).lower()
                stats['last_rc_sync'] = firestore.SERVER_TIMESTAMP
                self.db.collection('player_profiles').document(safe_id).set(stats, merge=True)
                self._log_audit(admin_email, 'RC_SYNC', f"Deep Scraped RC Data for {player_name}", {})
        threading.Thread(target=scrape_task, daemon=True).start()
        return True

    def auto_update_stale_rc_profiles(self):
        if not self.db:
            return
        try:
            now = datetime.datetime.now(datetime.timezone.utc)
            profiles = self.db.collection('player_profiles').where('ratings_central_id', '!=', '').stream()
            for doc in profiles:
                data = doc.to_dict()
                rc_id = data.get('ratings_central_id')
                if not rc_id:
                    continue
                last_sync = data.get('last_rc_sync')
                needs_update = False
                if not last_sync:
                    needs_update = True
                else:
                    try:
                        if (now - last_sync).days >= 4:
                            needs_update = True
                    except Exception:
                        needs_update = True
                if needs_update:
                    logger.info(f"♻️ Auto-updating stale RC profile for: {data.get('name')}")
                    self.trigger_background_rc_scrape(data.get('name'), rc_id, "SYSTEM_AUTO_UPDATER")
        except Exception as e:
            logger.error(f"RC Auto-Update Error: {e}")

    def admin_get_player_profile(self, player_name):
        if not self.db:
            return {}
        try:
            doc = self.db.collection('player_profiles').document(re.sub(r'[^a-zA-Z0-9]', '_', player_name).lower()).get()
            data = doc.to_dict() if doc.exists else {}
            rat = self.rating_engine.get_rating(player_name)
            data['gctta_sd'] = rat.get('rd', 300.0)
            data['public_rating'] = int(round(rat.get('public_rating', rat.get('rating', 1050.0))))
            data['true_rating'] = int(round(rat.get('true_rating', rat.get('rating', 1050.0))))
            return data
        except Exception:
            return {}

    def admin_update_player_profile(self, player_name, ratings_central_id, admin_email="Unknown"):
        if not self.db:
            return False
        try:
            self.db.collection('player_profiles').document(re.sub(r'[^a-zA-Z0-9]', '_', player_name).lower()).set({
                'name': player_name,
                'ratings_central_id': ratings_central_id
            }, merge=True)
            self._log_audit(admin_email, 'UPDATE_PROFILE', f"Updated {player_name} Profile (RC: {ratings_central_id})", {})
            if ratings_central_id:
                self.trigger_background_rc_scrape(player_name, ratings_central_id, admin_email)
            return True
        except Exception:
            return False

    def register_player_account(self, name, dob, email, uid, estimated_rating, club="Unknown"):
        if not self.db:
            return {"success": False, "error": "DB Offline"}
        try:
            est_val = str(estimated_rating).strip()
            suggested_rc_id = est_val if est_val.isdigit() and int(est_val) > 5000 else ""
            self.db.collection('pending_accounts').document(uid).set({
                'name': name,
                'dob': dob,
                'email': email,
                'uid': uid,
                'estimated_rating': 1500.0 if suggested_rc_id else float(estimated_rating or 1500.0),
                'suggested_rc_id': suggested_rc_id,
                'club': club,
                'rc_sd': 150.0,
                'status': 'pending',
                'timestamp': firestore.SERVER_TIMESTAMP
            })
            return {"success": True}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def admin_get_pending_accounts(self):
        if not self.db:
            return []
        try:
            return [{"id": d.id, **d.to_dict()} for d in self.db.collection('pending_accounts').where('status', '==', 'pending').stream()]
        except Exception:
            return []

    def admin_link_player_account(self, uid, official_player_name, rc_id=None, admin_email="Unknown"):
        if not self.db:
            return False
        try:
            doc = self.db.collection('pending_accounts').document(uid).get()
            if not doc.exists:
                return False
            data = doc.to_dict()
            data['linked_player_name'] = official_player_name
            data['status'] = 'approved'
            data['approved_by'] = admin_email
            self.db.collection('verified_users').document(uid).set(data)
            self.db.collection('pending_accounts').document(uid).delete()
            update_payload = {'last_updated': firestore.SERVER_TIMESTAMP}
            if rc_id:
                update_payload['ratings_central_id'] = rc_id
            self.db.collection('player_profiles').document(re.sub(r'[^a-zA-Z0-9]', '_', official_player_name).lower()).set(update_payload, merge=True)
            self._log_audit(admin_email, 'APPROVE_ACCOUNT', f"Linked UID {uid} to {official_player_name}", {})
            return True
        except Exception:
            return False

    def admin_override_rating(self, player_id, new_rating, sd_override=None, retroactive=True, admin_email="Unknown", effective_date=None):
        if not self.db:
            return False
        try:
            final_sd = float(sd_override) if sd_override else 65.0
            player_name = self.id_to_name.get(player_id)
            if not player_name:
                for n, i in self.player_ids.items():
                    if i == player_id or n.lower() == str(player_id).lower():
                        player_name = n
                        break
            if not player_name and player_id in self.all_players:
                player_name = player_id
            if not player_name:
                return False

            date_str = effective_date if effective_date else ('1900-01-01' if retroactive else datetime.datetime.now().strftime("%Y-%m-%d"))

            payload = {
                'name': player_name,
                'rating': float(new_rating),
                'rd': final_sd,
                'vol': 25.0,
                'date_str': date_str,
                'timestamp': firestore.SERVER_TIMESTAMP,
                'author': admin_email,
                'retroactive': retroactive
            }
            self.db.collection('rating_overrides').document(player_name).set(payload)
            
            safe_id = re.sub(r'[^a-zA-Z0-9]', '_', player_name).lower()
            self.db.collection('player_profiles').document(safe_id).set({
                'rating': float(new_rating),
                'sd': final_sd,
                'last_updated': firestore.SERVER_TIMESTAMP
            }, merge=True)
            
            p_obj = self.rating_engine.get_rating(player_name)
            p_obj['true_rating'] = float(new_rating)
            p_obj['public_rating'] = clamp_public_rating(float(new_rating), p_obj.get('home_div'))
            p_obj['rating'] = p_obj['public_rating']
            p_obj['rd'] = final_sd

            self._log_audit(admin_email, 'OVERRIDE_RATING', f"Forced {player_name} to True Rating: {new_rating}, SD: {final_sd}", {"name": player_name})
            
            if retroactive:
                threading.Thread(target=self.refresh_data, daemon=True).start()
                
            return True
        except Exception as e:
            logger.error(f"Override Rating Error: {e}")
            return False

    def admin_bulk_fix_date(self, season, division, week, new_date, admin_email="Unknown"):
        if not self.db:
            return False
        try:
            rule_id = f"{self._slugify(season)}_{self._slugify(division)}_{self._extract_week(week)}"
            self.db.collection('bulk_date_rules').document(rule_id).set({
                'season': season,
                'division': division,
                'week': week,
                'date': new_date,
                'author': admin_email,
                'timestamp': firestore.SERVER_TIMESTAMP
            })
            self._log_audit(admin_email, 'BULK_DATE_FIX', f"Smart-mapped matches for {season} {division} Week {week} to {new_date}.", {"rule_id": rule_id})
            self.refresh_data()
            return True
        except Exception:
            return False

    def admin_override_match_deltas(self, match_id, p1_delta, p2_delta, admin_email="Unknown"):
        if not self.db:
            return False
        try:
            self.db.collection('match_delta_overrides').document(match_id).set({
                'match_id': match_id,
                'p1_delta': float(p1_delta),
                'p2_delta': float(p2_delta),
                'author': admin_email,
                'timestamp': firestore.SERVER_TIMESTAMP
            }, merge=True)
            self._log_audit(admin_email, 'OVERRIDE_DELTAS', f"Set point changes for match {match_id}: P1 {p1_delta}, P2 {p2_delta}", {"match_id": match_id})
            self.refresh_data()
            return True
        except Exception:
            return False
        
    def admin_add_manual_match(self, p1, p2, date_str, game_scores, admin_email="Unknown"):
        if not self.db:
            return False
        try:
            s1 = 0
            s2 = 0
            for game in str(game_scores or '').split(','):
                pts = game.strip().split('-')
                if len(pts) == 2:
                    try:
                        if int(pts[0]) > int(pts[1]):
                            s1 += 1
                        else:
                            s2 += 1
                    except Exception:
                        pass
            res = self.db.collection('match_results').add({
                'home_players': [p1],
                'away_players': [p2],
                'date': date_str,
                'live_home_sets': s1,
                'live_away_sets': s2,
                'game_scores_history': game_scores,
                'status': 'approved',
                'manual_override': True,
                'timestamp': firestore.SERVER_TIMESTAMP,
                'author': admin_email
            })
            self._log_audit(admin_email, 'ADD_MATCH', f"Manually authored match: {p1} vs {p2} ({s1}-{s2}).", {"result_id": res[1].id})
            self.refresh_data() 
            return True
        except Exception:
            return False

    def admin_update_historical_match(self, p1, p2, old_date, s1, s2, new_date=None, admin_email="Unknown", game_scores=None, match_id=None):
        if not self.db:
            return False
        try:
            p1_clean = self._clean_name(p1)
            p2_clean = self._clean_name(p2)
            s1_int = int(s1) if s1 is not None and str(s1) != '' else 0
            s2_int = int(s2) if s2 is not None and str(s2) != '' else 0

            if game_scores and s1_int == 0 and s2_int == 0:
                for g in str(game_scores).split(','):
                    pts = g.strip().split('-')
                    if len(pts) == 2:
                        try:
                            if int(pts[0]) > int(pts[1]):
                                s1_int += 1
                            else:
                                s2_int += 1
                        except Exception:
                            pass

            target_date = new_date if new_date else old_date
            corr_key = match_id if (match_id and str(match_id) not in ['N/A', 'None', 'undefined']) else f"{self._slugify(p1_clean)}_{self._slugify(p2_clean)}_{self._slugify(old_date)}"

            payload = {
                'p1': p1_clean,
                'p2': p2_clean,
                'old_date': old_date,
                'date': target_date,
                'new_date': target_date,
                's1': s1_int,
                's2': s2_int,
                'author': admin_email,
                'timestamp': firestore.SERVER_TIMESTAMP
            }
            if game_scores is not None:
                payload['game_scores'] = str(game_scores).strip()
                payload['details'] = str(game_scores).strip()
            if match_id:
                payload['match_id'] = match_id

            self.db.collection('match_corrections').document(str(corr_key)).set(payload, merge=True)

            if game_scores:
                for m in getattr(self, 'match_history_log', []):
                    if (m.get('match_id') == match_id) or (m.get('p1') == p1_clean and m.get('p2') == p2_clean and m.get('date') == old_date):
                        m['details'] = str(game_scores).strip()
                        m['game_history'] = str(game_scores).strip()
                        m['s1'] = s1_int
                        m['s2'] = s2_int
                for s_dict in getattr(self, 'weekly_matches', {}).values():
                    if isinstance(s_dict, dict):
                        for w_list in s_dict.values():
                            if isinstance(w_list, list):
                                for wm in w_list:
                                    if (wm.get('match_id') == match_id) or (wm.get('p1') == p1_clean and wm.get('p2') == p2_clean):
                                        wm['details'] = str(game_scores).strip()
                                        wm['score'] = f"{s1_int}-{s2_int}"

            desc = f"Logged set scores for {p1_clean} vs {p2_clean}: {game_scores}" if game_scores else f"Updated match {p1_clean} vs {p2_clean} to {s1_int}-{s2_int} ({target_date})"
            self._log_audit(admin_email, 'UPDATE_MATCH', desc, {"correction_id": str(corr_key)})
            threading.Thread(target=self.refresh_data, daemon=True).start()
            return True
        except Exception as e:
            logger.error(f"admin_update_historical_match error: {e}")
            return False

    def admin_get_player_directory(self):
        rc_map = {}
        if self.db:
            try:
                for doc in self.db.collection('player_profiles').stream():
                    d = doc.to_dict()
                    if d.get('name'):
                        rc_map[d['name'].lower()] = {
                            'rc_id': d.get('ratings_central_id', ''),
                            'rc_rating': d.get('rc_rating', '')
                        }
            except Exception:
                pass

        directory = []
        all_names = set(self.player_ids.keys()) | set(self.all_players.keys())
        for name in sorted(all_names):
            pid = self.player_ids.get(name, name)
            rat = self.rating_engine.get_rating(name)
            home_div = rat.get('home_div')
            pub_r = int(round(clamp_public_rating(rat.get('true_rating', rat.get('rating', 1050.0)), home_div)))
            true_r = int(round(rat.get('true_rating', rat.get('rating', 1050.0))))
            rd_val = int(round(rat.get('rd', 35.0)))
            rc_info = rc_map.get(name.lower(), {})

            directory.append({
                "name": name,
                "id": pid,
                "label": f"{name} (ID: {pid})",
                "rating": pub_r,
                "public_rating": pub_r,
                "true_rating": true_r,
                "rd": rd_val,
                "division_cap": self._format_div_label(home_div),
                "rc_id": rc_info.get('rc_id', ''),
                "rc_rating": rc_info.get('rc_rating', '')
            })
        return directory

    def admin_get_chaos_config(self):
        return self.chaos_config

    def admin_vote_chaos(self, weeks, admin_email="Unknown"):
        if not self.db:
            return False
        try:
            doc = self.db.collection('system_config').document('chaos_mode').get()
            data = doc.to_dict() if doc.exists else {'weeks': [], 'approvals': []}
            data['weeks'] = weeks
            if admin_email not in data['approvals']:
                data['approvals'].append(admin_email)
            self.db.collection('system_config').document('chaos_mode').set(data, merge=True)
            self._log_audit(admin_email, 'CHAOS_VOTE', f"Voted to activate chaos mode for weeks: {weeks}", {})
            self.refresh_data()
            return True
        except Exception:
            return False

    def admin_clear_chaos(self, admin_email="Unknown"):
        if not self.db:
            return False
        try:
            self.db.collection('system_config').document('chaos_mode').set({'weeks': [], 'approvals': []})
            self._log_audit(admin_email, 'CHAOS_CLEAR', "Cleared chaos mode settings", {})
            self.refresh_data()
            return True
        except Exception:
            return False
    
    def get_seasons(self):
        seasons = set()
        if hasattr(self, 'seasons_list') and self.seasons_list:
            seasons.update(self.seasons_list)
        if hasattr(self, 'seasons') and self.seasons:
            seasons.update(self.seasons)
        if hasattr(self, 'weekly_matches') and isinstance(self.weekly_matches, dict):
            seasons.update(self.weekly_matches.keys())
        if hasattr(self, 'season_stats') and isinstance(self.season_stats, dict):
            seasons.update(self.season_stats.keys())

        raw_seasons = [str(s).strip() for s in seasons if str(s).strip() and str(s).strip() != "Career"]
        if not raw_seasons:
            return ["Spring 2026"]

        season_rank = {
            'spring': 4,
            'term 4': 4,
            'winter': 3,
            'term 3': 3,
            'autumn': 2,
            'fall': 2,
            'term 2': 2,
            'summer': 1,
            'term 1': 1
        }

        def season_sort_key(s_str):
            s_lower = s_str.lower()
            year_match = re.search(r'(20\d{2})', s_lower)
            year = int(year_match.group(1)) if year_match else 0
            rank = 0
            for k, v in season_rank.items():
                if k in s_lower:
                    rank = v
                    break
            return (year, rank, s_str)

        clean_seasons = sorted(raw_seasons, key=season_sort_key, reverse=True)
        return clean_seasons
        
    def get_divisions(self): 
        divs = set()
        if hasattr(self, 'divisions_list') and self.divisions_list:
            divs.update(self.divisions_list)
        if hasattr(self, 'divisions') and self.divisions:
            divs.update(self.divisions)
        if hasattr(self, 'weekly_matches') and isinstance(self.weekly_matches, dict):
            for s_data in self.weekly_matches.values():
                if isinstance(s_data, dict):
                    for w_data in s_data.values():
                        if isinstance(w_data, list):
                            for m in w_data:
                                if 'division' in m:
                                    divs.add(m['division'])
        clean_divs = sorted(list(divs))
        return clean_divs if clean_divs else ["Division 1"] 
        
    def get_all_players(self): 
        return getattr(self, 'all_players', getattr(self, 'players', {}))
        
    def get_matches_by_week(self, season, week): 
        matches = getattr(self, 'weekly_matches', getattr(self, 'matches_by_week', getattr(self, 'weekly_results', {})))
        if isinstance(matches, dict):
            s_data = matches.get(season, {})
            if isinstance(s_data, dict):
                if str(week).lower() == 'all':
                    all_m = []
                    for w_num in sorted(s_data.keys(), key=lambda x: int(re.search(r'\d+', str(x)).group()) if re.search(r'\d+', str(x)) else 999):
                        all_m.extend(s_data.get(w_num, []))
                    return all_m
                return s_data.get(str(week), [])
        return []

    def get_division_rankings(self, season, division, max_week=None):
        stats_dict = getattr(self, 'season_stats', getattr(self, 'stats_by_season', {}))
        if season not in stats_dict:
            return []
        
        div_num = parse_division_number(division)
        ranking_list = []
        for player_name, stats in stats_dict[season].items():
            rat = self.rating_engine.get_rating(player_name, division)
            reg_hist = [m for m in stats.get('regular', {}).get('history', []) if m.get('division') == division]
            fill_hist = [m for m in stats.get('fillin', {}).get('history', []) if m.get('division') == division]
            if not reg_hist and not fill_hist:
                continue 
            
            def calc_summary(history):
                wins = sum(1 for m in history if m.get('result') == "Win")
                return {'wins': wins, 'losses': len(history) - wins, 'matches': len(history)}

            true_val = float(rat.get('true_rating', rat.get('rating', 1050.0)))
            target_div = rat.get('home_div') if rat.get('home_div') is not None else div_num
            clamped_pub = int(round(clamp_public_rating(true_val, target_div)))
            
            ranking_list.append({
                'name': player_name, 
                'rating_val': clamped_pub,
                'true_rating': int(round(true_val)),
                'sigma': int(round(rat.get('rd', 350))), 
                'vol': rat.get('vol', 25.0), 
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
            if not stats:
                return {'matches': 0, 'wins': 0, 'losses': 0, 'win_rate': "0%", 'match_history': []}
            hist = stats.get('history', [])
            if division != "All":
                hist = [m for m in hist if m.get('division') == division]
            wins = sum(1 for m in hist if m.get('result') == "Win")
            win_rate = round((wins / len(hist)) * 100, 1) if hist else 0
            disp_hist = list(hist)
            disp_hist.reverse()
            return {'matches': len(hist), 'wins': wins, 'losses': len(hist) - wins, 'win_rate': f"{win_rate}%", 'match_history': disp_hist}
            
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
                    if not score:
                        continue
                    parts = score.split('-')
                    if len(parts) != 2:
                        continue
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
                            g1 = games[0].split('-')
                            if len(g1) == 2:
                                if int(g1[0]) > int(g1[1]):
                                    strong_starts.append(match_data)
                                    
                            has_zero = False
                            p1_lead = 0
                            p2_lead = 0
                            had_two_zero_lead = False
                            trailed_zero_two = False

                            for idx_g, g in enumerate(games):
                                pts = g.split('-')
                                if len(pts) == 2:
                                    g_p1, g_p2 = int(pts[0]), int(pts[1])
                                    if g_p1 == 0 or g_p2 == 0:
                                        has_zero = True
                                    if g_p1 > g_p2:
                                        p1_lead += 1
                                    else:
                                        p2_lead += 1
                                    if idx_g == 1:
                                        if p1_lead == 2 and p2_lead == 0:
                                            had_two_zero_lead = True
                                        elif p2_lead == 2 and p1_lead == 0:
                                            trailed_zero_two = True

                            if has_zero:
                                merciless.append(match_data)
                            if had_two_zero_lead and result == "Loss":
                                letdowns.append(match_data)
                            if trailed_zero_two and result == "Win":
                                comebacks.append(match_data)
                                
                            last_g = games[-1].split('-')
                            if len(last_g) == 2:
                                p1_pts, p2_pts = int(last_g[0]), int(last_g[1])
                                if p1_pts >= 10 and p2_pts >= 10 and abs(p1_pts - p2_pts) <= 2:
                                    nail_biters.append(match_data)
                                elif (p1_pts == 11 and p2_pts == 9) or (p1_pts == 9 and p2_pts == 11):
                                    nail_biters.append(match_data)
                except Exception:
                    pass
            
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

        career_raw = players_data.get(player_name, {})
        career_hist = career_raw.get('combined', {}).get('history', [])
        insights_data = calculate_insights(career_hist)
        
        total_career_matches = len(career_hist)
        total_career_wins = sum(1 for m in career_hist if m.get('result') == "Win")
        total_career_losses = total_career_matches - total_career_wins
        
        first_year = "unknown"
        if career_hist:
            first_year_candidates = []
            for m in career_hist:
                date_str = m.get('date', '')
                if date_str and '/' in date_str:
                    parts = date_str.split('/')
                    if len(parts) == 3:
                        first_year_candidates.append(parts[2])
                elif m.get('season'):
                    s_val = str(m.get('season'))
                    match = re.search(r'\d{4}', s_val)
                    if match:
                        first_year_candidates.append(match.group(0))
            if first_year_candidates:
                first_year = min(first_year_candidates)

        home_div = rat.get('home_div')
        if home_div is None and career_hist:
            for m in reversed(career_hist):
                if m.get('type', 'Regular') == 'Regular' and m.get('division'):
                    parsed_d = parse_division_number(m.get('division'))
                    if parsed_d is not None:
                        home_div = parsed_d
                        rat['home_div'] = home_div
                        break

        true_val = float(rat.get('true_rating', rat.get('rating', 1050.0)))
        public_clamped = int(round(clamp_public_rating(true_val, home_div)))

        return {
            'name': player_name, 
            'rating': public_clamped,
            'public_rating': public_clamped,
            'true_rating': int(round(true_val)),
            'rd': int(round(rat.get('rd', 350))), 
            'vol': rat.get('vol', 25.0), 
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
        try:
            s1 = int(s1)
            s2 = int(s2)
        except Exception:
            return {"error": "Invalid scores"}

        kw = float(custom_k_win) if custom_k_win else self.k_win
        kl = float(custom_k_loss) if custom_k_loss else self.k_loss

        temp_engine = RatingEngine() 
        p1_stats = self.rating_engine.get_rating(p1_clean).copy()
        p2_stats = self.rating_engine.get_rating(p2_clean).copy()
        temp_engine.players[p1_clean] = p1_stats
        temp_engine.players[p2_clean] = p2_stats
        res = temp_engine.update_match(p1_clean, p2_clean, s1, s2, '', kw, kl, True)
        h2h_info = self.get_head_to_head(p1_clean, p2_clean)

        return {
            "p1": {
                "name": p1_clean,
                "old_rating": int(round(res['p1_before'])),
                "new_rating": int(round(res['p1_after'])),
                "delta": round(res['p1_delta'], 1),
                "old_true_rating": int(round(res.get('p1_true_before', res['p1_before']))),
                "new_true_rating": int(round(res.get('p1_true_after', res['p1_after']))),
                "true_delta": round(res.get('p1_true_delta', res['p1_delta']), 1),
                "old_rd": int(round(res['p1_rd_before'])),
                "new_rd": int(round(res['p1_rd_after'])),
                "rd_delta": round(res['p1_rd_after'] - res['p1_rd_before'], 1),
                "expected_win_pct": h2h_info.get('p1_prob', 50.0)
            }, 
            "p2": {
                "name": p2_clean,
                "old_rating": int(round(res['p2_before'])),
                "new_rating": int(round(res['p2_after'])),
                "delta": round(res['p2_delta'], 1),
                "old_true_rating": int(round(res.get('p2_true_before', res['p2_before']))),
                "new_true_rating": int(round(res.get('p2_true_after', res['p2_after']))),
                "true_delta": round(res.get('p2_true_delta', res['p2_delta']), 1),
                "old_rd": int(round(res['p2_rd_before'])),
                "new_rd": int(round(res['p2_rd_after'])),
                "rd_delta": round(res['p2_rd_after'] - res['p2_rd_before'], 1),
                "expected_win_pct": h2h_info.get('p2_prob', 50.0)
            },
            "h2h": h2h_info
        }

    def admin_glicko_math(self, p1, p2, s1, s2):
        return self.simulate_match_public(p1, p2, s1, s2)
        
    def admin_set_rating_scales(self, k_win, k_loss, admin_email="Unknown"):
        if not self.db:
            return False
        try:
            doc = self.db.collection('system_config').document('main').get()
            old_k_win = 1.0
            old_k_loss = 1.4
            if doc.exists:
                data = doc.to_dict()
                old_k_win = data.get('k_win_scale', 1.0)
                old_k_loss = data.get('k_loss_scale', 1.4)

            self.db.collection('system_config').document('main').set({
                'k_win_scale': float(k_win),
                'k_loss_scale': float(k_loss)
            }, merge=True)
            self.k_win = float(k_win)
            self.k_loss = float(k_loss)
            self._log_audit(admin_email, 'UPDATE_MATH', f"Set K-Win to {k_win}, K-Loss to {k_loss}", {"old_k_win": old_k_win, "old_k_loss": old_k_loss})
            return True
        except Exception:
            return False

    def get_head_to_head(self, p1, p2):
        if not p1 or not p2:
            return {"error": "Missing players"}
        p1_clean = self._clean_name(p1)
        p2_clean = self._clean_name(p2)
        matches = []
        p1_wins = 0
        p2_wins = 0

        for m in getattr(self, 'match_history_log', getattr(self, 'match_history', [])):
            if (m['p1'] == p1_clean and m['p2'] == p2_clean) or (m['p1'] == p2_clean and m['p2'] == p1_clean):
                matches.append(m)
                if m['p1'] == p1_clean:
                    if m['s1'] > m['s2']:
                        p1_wins += 1
                    elif m['s2'] > m['s1']:
                        p2_wins += 1
                else:
                    if m['s1'] > m['s2']:
                        p2_wins += 1
                    elif m['s2'] > m['s1']:
                        p1_wins += 1

        matches.sort(key=lambda x: datetime.datetime.strptime(x['date'], "%d/%m/%Y") if x.get('date') and x['date'] != "nodate" else datetime.datetime.min, reverse=True)
        
        rat1 = self.rating_engine.get_rating(p1_clean)
        rat2 = self.rating_engine.get_rating(p2_clean)
        r1 = clamp_public_rating(rat1.get('true_rating', rat1.get('rating', 1050.0)), rat1.get('home_div'))
        r2 = clamp_public_rating(rat2.get('true_rating', rat2.get('rating', 1050.0)), rat2.get('home_div'))
        rd1 = rat1.get('rd', 350)
        rd2 = rat2.get('rd', 350)
        
        import math
        
        q = math.log(10) / 400
        rd_combined = math.sqrt(rd1**2 + rd2**2)
        g_rd = 1 / math.sqrt(1 + 3 * (q * rd_combined / math.pi)**2)
        rating_prob_p1 = 1 / (1 + 10 ** (g_rd * (r2 - r1) / 400))
        
        total_m = len(matches)
        h2h_prob_p1 = 0.5
        if total_m > 0:
            weighted_wins_p1 = 0
            weighted_total = 0
            weighted_sets_p1 = 0
            weighted_sets_total = 0
            
            for i, m in enumerate(matches):
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
            h2h_prob_p1 = (h2h_win_rate * 0.7) + (set_ratio * 0.3)
            
        h2h_weight = min(total_m / 25.0, 0.6) 
        final_prob_p1 = (rating_prob_p1 * (1 - h2h_weight)) + (h2h_prob_p1 * h2h_weight)
        final_prob_p1 = max(0.01, min(0.99, final_prob_p1))
        final_prob_p2 = 1 - final_prob_p1
        
        odds_p1 = round(1 / final_prob_p1, 2)
        odds_p2 = round(1 / final_prob_p2, 2)
        
        return {
            "p1": p1_clean,
            "p2": p2_clean, 
            "p1_wins": p1_wins,
            "p2_wins": p2_wins, 
            "total_matches": total_m, 
            "history": matches,
            "p1_rating": int(round(r1)),
            "p1_true_rating": int(round(rat1.get('true_rating', r1))),
            "p1_rd": int(round(rd1)),
            "p2_rating": int(round(r2)),
            "p2_true_rating": int(round(rat2.get('true_rating', r2))),
            "p2_rd": int(round(rd2)),
            "p1_odds": f"{odds_p1:.2f}",
            "p2_odds": f"{odds_p2:.2f}",
            "p1_prob": round(final_prob_p1 * 100, 1),
            "p2_prob": round(final_prob_p2 * 100, 1)
        }

    def get_rating_history(self, player_name, include_true_rating=False):
        if not self.db:
            return []
        try:
            p_clean = self._clean_name(player_name)
            docs = self.db.collection('player_rating_history').where('player_name', '==', p_clean).limit(200).stream()
            history = []
            for d in docs:
                data = d.to_dict()
                date_val = data.get('date', 'Unknown')
                if hasattr(data.get('timestamp'), 'strftime'):
                    date_val = data.get('timestamp').strftime("%d/%m/%Y")
                entry = {
                    "date": date_val,
                    "rating": round(data.get('rating', 0), 1),
                    "sd": round(data.get('sd', 0), 1),
                    "rating_change": round(data.get('rating_change', 0), 1),
                    "sd_change": round(data.get('sd_change', 0), 1),
                    "opponent": data.get('opponent', ''),
                    "result_str": data.get('result_str', ''),
                    "is_decay": data.get('is_decay', False),
                    "_timestamp_val": data.get('timestamp').timestamp() if hasattr(data.get('timestamp'), 'timestamp') else 0
                }
                if include_true_rating:
                    entry["true_rating"] = round(data.get('true_rating', data.get('rating', 0)), 1)
                    entry["true_rating_change"] = round(data.get('true_rating_change', data.get('rating_change', 0)), 1)
                history.append(entry)
            
            history.sort(key=lambda x: x.get('_timestamp_val', 0), reverse=True)
            
            if not history and hasattr(self, 'match_history_log'):
                for m in reversed(self.match_history_log):
                    if p_clean in m.get('home_players', [m.get('p1')]) or p_clean in m.get('away_players', [m.get('p2')]):
                        is_home = p_clean in m.get('home_players', [m.get('p1')])
                        entry = {
                            "date": m.get('date', 'Unknown'),
                            "rating": round((m.get('p1_after') if is_home else m.get('p2_after')) or 0, 1),
                            "sd": round((m.get('p1_rd_after') if is_home else m.get('p2_rd_after')) or 0, 1),
                            "rating_change": round((m.get('p1_delta') if is_home else m.get('p2_delta')) or 0, 1),
                            "sd_change": round(((m.get('p1_rd_after', 0) - m.get('p1_rd_before', 0)) if is_home else (m.get('p2_rd_after', 0) - m.get('p2_rd_before', 0))), 1),
                            "opponent": m.get('p2') if is_home else m.get('p1'),
                            "result_str": m.get('score', ''),
                            "is_decay": False
                        }
                        if include_true_rating:
                            entry["true_rating"] = round((m.get('p1_true_after', m.get('p1_after')) if is_home else m.get('p2_true_after', m.get('p2_after'))) or 0, 1)
                        history.append(entry)
                
            return history[:100]
        except Exception as e:
            logger.error(f"Error fetching rating history: {e}")
            return []