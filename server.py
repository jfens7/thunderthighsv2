import os
import logging
import datetime
import threading
import stripe
from functools import wraps
from flask import Flask, render_template, jsonify, request, session, redirect, url_for, make_response
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from firebase_admin import auth as fb_auth

from backend.backend import ThunderData

app = Flask(__name__, static_folder="frontend/static", template_folder="frontend/templates")
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'gctta-super-secret-session-key')
stripe.api_key = os.environ.get('STRIPE_SECRET_KEY')
app.config['PERMANENT_SESSION_LIFETIME'] = datetime.timedelta(days=30)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

db = None
db_lock = threading.Lock()
sync_lock = threading.Lock()
is_first_snapshot = True

def scheduled_refresh():
    global db
    if db and sync_lock.acquire(blocking=False):
        try: 
            logger.info("🔄 Executing background database sync...")
            db.refresh_data()
            logger.info("✅ Background sync complete!")
        except Exception as e: 
            logger.error(f"Background Sync Error: {e}")
        finally:
            sync_lock.release()

def watch_firebase_for_live_matches():
    global db
    if not db or not db.db: return
    
    def on_snapshot(col_snapshot, changes, read_time):
        global is_first_snapshot
        if is_first_snapshot:
            is_first_snapshot = False
            return
            
        has_new_match = False
        for change in changes:
            if change.type.name == 'ADDED':
                has_new_match = True
                
        if has_new_match:
            logger.info("📡 New Match Detected! Triggering Instant Auto-Sync...")
            threading.Thread(target=scheduled_refresh).start()

    try:
        db.db.collection('match_results').where('status', '==', 'approved').on_snapshot(on_snapshot)
        logger.info("📡 Instant Auto-Sync Listener Attached.")
    except Exception as e:
        logger.error(f"Failed to attach listener: {e}")

@app.before_request
def pre_request_setup():
    global db
    if db is None:
        with db_lock:
            if db is None:
                try:
                    logger.info("Initializing ThunderData safely inside worker process...")
                    db = ThunderData()
                    watch_firebase_for_live_matches()
                    logger.info("✅ Database connected.")
                except Exception as e:
                    logger.error(f"❌ FATAL: Failed to start Backend: {str(e)}")

    if request.path == '/' or request.path == '/index':
        if not request.cookies.get('ghostmode') and not session.get('admin_logged_in'):
            if db:
                ip = request.headers.get('X-Forwarded-For', request.remote_addr)
                if ip: 
                    db.record_page_view(ip.split(',')[0].strip())

scheduler = BackgroundScheduler()
try:
    scheduler.add_job(func=scheduled_refresh, trigger="interval", hours=12, id="scheduled_refresh")
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown())
except Exception as e: pass

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('admin_logged_in'): return jsonify({'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    return decorated_function

def super_admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        role = session.get('admin_role')
        if not session.get('admin_logged_in') or role not in ['super_admin', 'temp_super_admin']: 
            return jsonify({'error': 'Super Admin Required'}), 403
        return f(*args, **kwargs)
    return decorated_function

def verify_player_token(req):
    auth_header = req.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '): return None
    token = auth_header.split('Bearer ')[1]
    try:
        decoded = fb_auth.verify_id_token(token)
        return decoded.get('uid')
    except Exception as e:
        return None

@app.route('/ghostmode')
def activate_ghost_mode():
    resp = make_response(jsonify({"success": True, "message": "👻 GHOST MODE ACTIVATED"}))
    resp.set_cookie('ghostmode', '1', max_age=60*60*24*365*10) 
    return resp

@app.route('/')
def index(): return render_template('index.html')
@app.route('/login')
def login(): return redirect(url_for('admin')) if session.get('admin_logged_in') else render_template('login.html')
@app.route('/player_auth')
def player_auth_page(): return render_template('player_auth.html')
@app.route('/dashboard')
def player_dashboard(): return render_template('dashboard.html')

@app.route('/api/auth/google', methods=['POST'])
def auth_google():
    if not db: return jsonify({"success": False, "error": "Database offline"})
    user_data = db.verify_admin_token(request.json.get('token'))
    if not user_data: return jsonify({"success": False, "error": "Invalid Token"})
        
    if user_data['role'] in ['admin', 'super_admin', 'temp_super_admin']:
        session.permanent = True
        session['admin_logged_in'] = True
        session['admin_email'] = user_data['email']
        session['admin_role'] = user_data['role']
        db._log_audit(user_data['email'], 'SESSION_START', "Admin authenticated and logged in.", {})
        return jsonify({"success": True})
    else: 
        return jsonify({"success": False, "error": "Your account is pending approval."})

@app.route('/api/webhook/sms', methods=['POST', 'GET'])
def sms_webhook():
    if not db: return jsonify({"status": "error"}), 500
    try:
        data = request.json if request.is_json else request.form
        sender = data.get('from', 'Unknown')
        body = data.get('body', '')
        if sender and body: db.db.collection('sms_replies').add({'from': sender, 'body': body, 'timestamp': firestore.SERVER_TIMESTAMP, 'status': 'unread'})
        return jsonify({"status": "received"}), 200
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/logout')
def logout(): 
    if db and session.get('admin_email'): db._log_audit(session.get('admin_email'), 'SESSION_END', "Admin signed out.", {})
    session.clear()
    return redirect(url_for('index'))

@app.route('/admin')
def admin(): 
    if not session.get('admin_logged_in'): return redirect(url_for('login'))
    return render_template('admin.html', email=session.get('admin_email'), role=session.get('admin_role'))

# --- PUBLIC DATA APIS ---
@app.route('/api/players')
def get_players(): return jsonify(list(db.get_all_players().keys())) if db else jsonify([])
@app.route('/api/seasons')
def get_seasons(): return jsonify(db.get_seasons()) if db else jsonify([])
@app.route('/api/divisions')
def get_divisions(): return jsonify(db.get_divisions()) if db else jsonify([])
@app.route('/api/stats/<player_name>')
def get_player_stats(player_name):
    if not db: return jsonify({"error": "Offline"}), 500
    if player_name not in db.all_players:
        for real_name in db.all_players.keys():
            if real_name.lower() == player_name.lower(): 
                player_name = real_name
                break
    stats = db.get_player_stats(player_name, request.args.get('season', 'Career'), request.args.get('division', 'All'))
    if stats: return jsonify(stats)
    return jsonify({"error": "Not found"}), 404

@app.route('/api/rankings/<season>/<division>')
def get_rankings(season, division): return jsonify(db.get_division_rankings(season, division, request.args.get('week', 'Latest'))) if db else jsonify([])
@app.route('/api/week/<season>/<week>')
def get_week_results(season, week): return jsonify(db.get_matches_by_week(season, week)) if db else jsonify([])
@app.route('/api/h2h')
def get_h2h(): return jsonify(db.get_head_to_head(request.args.get('p1', ''), request.args.get('p2', ''))) if db else jsonify({"error": "Offline"}), 500

@app.route('/api/create-payment-intent', methods=['POST'])
def create_payment():
    try: 
        amount = int(float(request.json.get('amount', 5.00)) * 100)
        return jsonify({'clientSecret': stripe.PaymentIntent.create(amount=amount, currency='aud', automatic_payment_methods={'enabled': True}).client_secret})
    except Exception as e: return jsonify(error=str(e)), 403

@app.route('/api/record_donation', methods=['POST'])
def record_donation(): return jsonify({"success": db.record_donation(request.json.get('intent_id'), request.json.get('name'), request.json.get('amount'))}) if db else jsonify({"success": False})
@app.route('/api/top_donors')
def top_donors(): return jsonify(db.get_top_donors()) if db else jsonify([])
@app.route('/api/notices')
def get_notices(): return jsonify(db.get_notices()) if db else jsonify([])

# --- ADMIN REST APIS ---
@app.route('/api/admin/pending_accounts')
@login_required
def get_pending_accounts():
    if db: return jsonify(db.admin_get_pending_accounts())
    return jsonify([])

@app.route('/api/admin/reports')
@login_required
def get_reports(): 
    if db: 
        db._log_audit(session.get('admin_email'), 'VIEW_FEEDBACK', "Viewed App Feedback Logs", {})
        return jsonify(db.admin_get_reports())
    return jsonify([])

@app.route('/api/admin/date_errors')
@login_required
def get_date_errors(): 
    if db: 
        db._log_audit(session.get('admin_email'), 'VIEW_ERRORS', "Scanned for missing date errors", {})
        return jsonify(db.admin_get_date_errors())
    return jsonify([])

@app.route('/api/admin/history')
@login_required
def search_history(): 
    q = request.args.get('q', '')
    if db:
        if q: db._log_audit(session.get('admin_email'), 'SEARCH_HISTORY', f"Searched match history for: {q}", {})
        return jsonify(db.admin_search_history(q, source_filter=request.args.get('source', 'All')))
    return jsonify([])

@app.route('/api/admin/player_profile/<player_name>', methods=['GET'])
@login_required
def get_player_profile(player_name):
    if db: 
        db._log_audit(session.get('admin_email'), 'VIEW_PROFILE', f"Opened profile editor for {player_name}", {})
        return jsonify(db.admin_get_player_profile(player_name))
    return jsonify({})

@app.route('/api/admin/update_player_profile', methods=['POST'])
@login_required
def update_player_profile():
    if db: return jsonify({"success": db.admin_update_player_profile(request.json.get('player_name'), request.json.get('ratings_central_id'), session.get('admin_email'))})
    return jsonify({"success": False})

@app.route('/api/admin/manual_match', methods=['POST'])
@login_required
def manual_match(): return jsonify({"success": db.admin_add_manual_match(request.json.get('p1'), request.json.get('p2'), request.json.get('date'), request.json.get('scores'), session.get('admin_email'))}) if db else jsonify({"success": False})
@app.route('/api/admin/update_match', methods=['POST'])
@login_required
def update_match(): return jsonify({"success": db.admin_update_historical_match(request.json.get('p1'), request.json.get('p2'), request.json.get('date'), request.json.get('s1'), request.json.get('s2'), request.json.get('new_date'), session.get('admin_email'))}) if db else jsonify({"success": False})
@app.route('/api/admin/bulk_fix_date', methods=['POST'])
@login_required
def bulk_fix_date(): return jsonify({"success": db.admin_bulk_fix_date(request.json.get('season'), request.json.get('division'), request.json.get('week'), request.json.get('date'), session.get('admin_email'))}) if db else jsonify({"success": False})
@app.route('/api/admin/override_deltas', methods=['POST'])
@login_required
def override_deltas(): return jsonify({"success": db.admin_override_match_deltas(request.json.get('match_id'), request.json.get('p1_delta'), request.json.get('p2_delta'), session.get('admin_email'))}) if db else jsonify({"success": False})
@app.route('/api/admin/set_rating_scales', methods=['POST'])
@super_admin_required
def set_rating_scales(): return jsonify({"success": db.admin_set_rating_scales(request.json.get('k_win'), request.json.get('k_loss'), session.get('admin_email'))}) if db else jsonify({"success": False})
@app.route('/api/admin/get_rating_scales', methods=['GET'])
@login_required
def get_rating_scales(): return jsonify({"k_win": db.k_win, "k_loss": db.k_loss}) if db else jsonify({"k_win": 1.0, "k_loss": 1.4})
@app.route('/api/admin/chaos_config', methods=['GET'])
@login_required
def get_chaos_config(): return jsonify(db.admin_get_chaos_config()) if db else jsonify({"success": False})
@app.route('/api/admin/chaos_vote', methods=['POST'])
@login_required
def chaos_vote(): return jsonify({"success": db.admin_vote_chaos(request.json.get('weeks', []), session.get('admin_email'))}) if db else jsonify({"success": False})
@app.route('/api/admin/chaos_clear', methods=['POST'])
@super_admin_required
def chaos_clear(): return jsonify({"success": db.admin_clear_chaos(session.get('admin_email'))}) if db else jsonify({"success": False})

@app.route('/api/admin/teams', methods=['GET'])
@login_required
def admin_teams(): 
    if db: 
        db._log_audit(session.get('admin_email'), 'VIEW_TEAMS', "Viewed Team Roster Editor", {})
        return jsonify(db.admin_get_teams())
    return jsonify([])

@app.route('/api/admin/update_team', methods=['POST'])
@login_required
def admin_update_team(): return jsonify({"success": db.admin_update_team(request.json.get('team_id'), request.json.get('players'), session.get('admin_email'))}) if db else jsonify({"success": False})

@app.route('/api/admin/upload_schedule', methods=['POST'])
@login_required
def upload_schedule():
    if not db: return jsonify({"success": False, "error": "DB Offline"})
    if 'pdf' not in request.files: return jsonify({"success": False, "error": "No file uploaded"})
    season = request.form.get('season', 'Unknown')
    division = request.form.get('division', 'Unknown')
    return jsonify(db.admin_upload_pdf_schedule(season, division, request.files['pdf'], session.get('admin_email')))

@app.route('/api/admin/upcoming_schedules', methods=['GET'])
@login_required
def get_upcoming_schedules():
    return jsonify(db.admin_get_upcoming_schedules()) if db else jsonify([])

@app.route('/api/admin/delete_schedule', methods=['POST'])
@login_required
def delete_schedule():
    return jsonify({"success": db.admin_delete_upcoming_schedule(request.json.get('schedule_id'), session.get('admin_email'))}) if db else jsonify({"success": False})

@app.route('/api/admin/export_zermelo/<tournament_id>', methods=['GET'])
@login_required
def export_zermelo(tournament_id):
    if not db: return "Offline", 500
    db._log_audit(session.get('admin_email'), 'EXPORT_ZERMELO', f"Downloaded Zermelo CSV for {tournament_id}", {})
    csv_data = db.generate_zermelo_csv(tournament_id)
    if not csv_data: return "Tournament not found", 404
    res = make_response(csv_data)
    res.headers["Content-Disposition"] = f"attachment; filename=zermelo_{tournament_id}.csv"
    res.headers["Content-type"] = "text/csv"
    return res

@app.route('/api/admin/merge', methods=['POST'])
@login_required
def merge_players(): return jsonify({"success": db.admin_merge_players(request.json.get('bad_name'), request.json.get('good_name'), session.get('admin_email'))}) if db else jsonify({"success": False})
@app.route('/api/admin/override_rating', methods=['POST'])
@login_required
def override_rating(): return jsonify({"success": db.admin_override_rating(request.json.get('player_id'), request.json.get('rating'), request.json.get('retroactive', True), session.get('admin_email'))}) if db else jsonify({"success": False})
@app.route('/api/admin/rating_context/<player_id>', methods=['GET'])
@login_required
def rating_context(player_id): return jsonify(db.get_recent_rating_context(player_id)) if db else jsonify([])
@app.route('/api/admin/bulk_pull_ratings', methods=['POST'])
@login_required
def bulk_pull_ratings(): return jsonify(db.admin_bulk_pull_ratings(session.get('admin_email', 'Unknown'))) if db else jsonify({"success": False})
@app.route('/api/admin/force_finish_live', methods=['POST'])
@login_required
def force_finish_live(): return jsonify({"success": db.admin_force_finish_live(request.json.get('id'), request.json.get('s1'), request.json.get('s2'), session.get('admin_email'))}) if db else jsonify({"success": False})
@app.route('/api/admin/wipe_live', methods=['POST'])
@login_required
def wipe_live(): return jsonify({"success": db.admin_wipe_live(request.json.get('id'), session.get('admin_email'))}) if db else jsonify({"success": False})
@app.route('/api/admin/set_fixture_format', methods=['POST'])
@login_required
def set_fixture_format(): return jsonify({"success": db.admin_set_fixture_format(request.json.get('fixture_id'), request.json.get('format_type'), session.get('admin_email'))}) if db else jsonify({"success": False})
@app.route('/api/admin/player_directory')
@login_required
def get_player_directory(): return jsonify(db.admin_get_player_directory()) if db else jsonify([])

@app.route('/api/admin/glicko_calc', methods=['POST'])
@login_required
def glicko_calc(): 
    if db:
        p1 = request.json.get('p1')
        p2 = request.json.get('p2')
        db._log_audit(session.get('admin_email'), 'SIMULATOR', f"Ran Glicko match diagnostic on {p1} vs {p2}", {})
        return jsonify({"success": True, "data": db.admin_glicko_math(p1, p2, request.json.get('s1'), request.json.get('s2'))})
    return jsonify({"success": False})

@app.route('/api/admin/add_notice', methods=['POST'])
@login_required
def add_notice(): return jsonify({"success": db.admin_add_notice(request.json.get('title'), request.json.get('message'), request.json.get('type'), session.get('admin_email'))}) if db else jsonify({"success": False})
@app.route('/api/admin/delete_notice', methods=['POST'])
@login_required
def delete_notice(): return jsonify({"success": db.admin_delete_notice(request.json.get('notice_id'), session.get('admin_email'))}) if db else jsonify({"success": False})
@app.route('/api/admin/delete_post', methods=['POST'])
@login_required
def delete_community_post(): return jsonify({"success": db.admin_delete_community_post(request.json.get('post_id'), session.get('admin_email'))}) if db else jsonify({"success": False})

@app.route('/api/admin/messages')
@login_required
def get_admin_messages(): 
    if db: 
        db._log_audit(session.get('admin_email'), 'VIEW_NOTES', "Viewed internal Admin Chat Notes", {})
        return jsonify(db.get_admin_messages())
    return jsonify([])

@app.route('/api/admin/add_message', methods=['POST'])
@login_required
def add_admin_message(): return jsonify({"success": db.add_admin_message(request.json.get('message'), session.get('admin_email'))}) if db else jsonify({"success": False})

@app.route('/api/admin/contacts')
@login_required
def get_contacts():
    if not db: return jsonify({"emails": "", "phones": "", "preview": [], "error": "DB Offline"})
    try:
        db._log_audit(session.get('admin_email'), 'VIEW_CONTACTS', "Exported/Viewed Member Contact Lists", {})
        return jsonify(db.get_contact_lists())
    except Exception as e:
        return jsonify({"emails": "", "phones": "", "preview": [], "error": str(e)})

@app.route('/api/admin/send_sms', methods=['POST'])
@login_required
def send_sms():
    if not db: return jsonify({"success": False, "error": "Database offline"}), 503
    req_data = request.get_json(silent=True)
    if not req_data or not req_data.get('message'): 
        return jsonify({"success": False, "error": "Message cannot be empty."}), 400
    try:
        return jsonify(db.admin_send_sms_broadcast(req_data.get('message'), req_data.get('phones'), session.get('admin_email'))), 200
    except Exception as e:
        return jsonify({"success": False, "error": f"Internal Server Error: {str(e)}"}), 500

@app.route('/api/admin/sms_inbox')
@login_required
def get_sms_inbox(): 
    if not db: return jsonify([])
    try:
        db._log_audit(session.get('admin_email'), 'VIEW_SMS', "Viewed player SMS replies inbox", {})
        return jsonify(db.get_sms_inbox())
    except Exception as e:
        return jsonify([])

@app.route('/api/admin/donations')
@login_required
def admin_donations(): 
    if db: 
        db._log_audit(session.get('admin_email'), 'VIEW_DONATIONS', "Viewed Financial Ledger", {})
        return jsonify(db.admin_get_all_donations())
    return jsonify([])

@app.route('/api/admin/traffic')
@login_required
def admin_traffic(): return jsonify(db.get_traffic_stats()) if db else jsonify({'views': 0, 'uniques': 0})
@app.route('/api/admin/audit_logs')
@login_required
def audit_logs(): return jsonify(db.get_audit_logs()) if db else jsonify([])
@app.route('/api/admin/undo_action', methods=['POST'])
@super_admin_required
def undo_action(): return jsonify({"success": db.undo_audit_action(request.json.get('log_id'), session.get('admin_email'))}) if db else jsonify({"success": False})
@app.route('/api/admin/users')
@super_admin_required
def admin_users(): return jsonify(db.get_admin_users()) if db else jsonify([])
@app.route('/api/admin/approve_user', methods=['POST'])
@super_admin_required
def approve_user(): return jsonify({"success": db.approve_admin(request.json.get('email'), request.json.get('action'))}) if db else jsonify({"success": False})

@app.route('/api/refresh', methods=['POST'])
@login_required
def force_refresh():
    if db: db._log_audit(session.get('admin_email'), 'FORCE_REFRESH', "Manually pulled from Google Sheets", {})
    threading.Thread(target=scheduled_refresh).start()
    return jsonify({"success": True, "status": "Refreshed"})

@app.route('/api/admin/tournaments/create', methods=['POST'])
@login_required
def create_tournament():
    if not db: return jsonify({"success": False, "error": "DB Offline"}), 503
    req = request.get_json(silent=True)
    return jsonify(db.admin_create_tournament(req, session.get('admin_email')))

@app.route('/api/admin/tournaments/events/create', methods=['POST'])
@login_required
def create_event():
    if not db: return jsonify({"success": False, "error": "DB Offline"}), 503
    req = request.get_json(silent=True)
    return jsonify(db.admin_create_event(req.get('tournament_id'), req.get('event_data', {}), session.get('admin_email')))

@app.route('/api/hub/register', methods=['POST'])
def hub_register(): return jsonify(db.register_player_account(request.json.get('name'), request.json.get('dob'), request.json.get('email'), request.json.get('uid'), request.json.get('estimated_rating'))) if db else jsonify({"success": False})

@app.route('/api/hub/tournaments/check_eligibility', methods=['POST'])
def hub_check_eligibility():
    if not db: return jsonify({"eligible": False, "reasons": ["Database offline"]})
    req = request.get_json(silent=True)
    uid = req.get('uid')
    event_data = req.get('event_data', {})
    partner_uid = req.get('partner_uid')
    eligible, reasons = db.check_eligibility(uid, event_data, partner_uid)
    return jsonify({"eligible": eligible, "reasons": reasons})

@app.route('/api/hub/forum', methods=['GET'])
def get_forum(): return jsonify(db.get_community_feed()) if db else jsonify([])
@app.route('/api/hub/post', methods=['POST'])
def make_post(): return jsonify({"success": db.create_community_post(request.json.get('uid'), request.json.get('name'), request.json.get('content'), request.json.get('type', 'General'), request.json.get('image_url'), request.json.get('poll_options'))}) if db else jsonify({"success": False})
@app.route('/api/hub/vote', methods=['POST'])
def vote_poll(): return jsonify({"success": db.vote_community_poll(request.json.get('post_id'), request.json.get('option'), request.json.get('uid'))}) if db else jsonify({"success": False})
@app.route('/api/hub/comment', methods=['POST'])
def add_comment(): return jsonify({"success": db.add_community_comment(request.json.get('post_id'), request.json.get('name'), request.json.get('content'))}) if db else jsonify({"success": False})
@app.route('/api/hub/upvote', methods=['POST'])
def toggle_upvote(): return jsonify({"success": db.toggle_post_upvote(request.json.get('post_id'), request.json.get('uid'))}) if db else jsonify({"success": False})
@app.route('/api/hub/schedule/<player_name>', methods=['GET'])
def get_schedule(player_name): return jsonify(db.get_player_upcoming_schedule(player_name)) if db else jsonify([])
@app.route('/api/hub/tournament/update_cart', methods=['POST'])
def update_cart(): return jsonify(db.process_tournament_cart(request.json.get('uid'), request.json.get('tournament_id'), request.json.get('events'), request.json.get('total'))) if db else jsonify({"success": False})

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5001))
    app.run(debug=True, host='0.0.0.0', port=port)