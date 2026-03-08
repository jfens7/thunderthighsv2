import os
import logging
import datetime
import stripe
from functools import wraps
from flask import Flask, render_template, jsonify, request, session, redirect, url_for, make_response
from apscheduler.schedulers.background import BackgroundScheduler
import atexit

from backend.backend import ThunderData

app = Flask(__name__, static_folder="frontend/static", template_folder="frontend/templates")

app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'gctta-super-secret-session-key')
stripe.api_key = os.environ.get('STRIPE_SECRET_KEY')

app.config['PERMANENT_SESSION_LIFETIME'] = datetime.timedelta(days=30)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

db = None
try:
    logger.info("Initializing ThunderData backend...")
    db = ThunderData()
    if __name__ != '__main__':
        logger.info("🌐 Render Deployment Detected: Forcing initial data sync before accepting web traffic...")
        db.refresh_data()
        logger.info("✅ Initial Render sync complete.")
except Exception as e:
    logger.error(f"❌ FATAL: Failed to start Backend: {str(e)}", exc_info=True)

def scheduled_refresh():
    if db:
        try:
            logger.info("⏰ Background Data Sync Started...")
            db.refresh_data()
            logger.info("✅ Background Data Sync Complete!")
        except Exception as e:
            logger.error(f"🚨 ERROR in scheduled_refresh: {str(e)}", exc_info=True)

scheduler = BackgroundScheduler()

try:
    if __name__ == '__main__':
        first_run = datetime.datetime.now()
        scheduler.add_job(func=scheduled_refresh, trigger="interval", hours=12, id="scheduled_refresh", next_run_time=first_run)
    else:
        first_run = datetime.datetime.now() + datetime.timedelta(minutes=30)
        scheduler.add_job(func=scheduled_refresh, trigger="interval", hours=12, id="scheduled_refresh", next_run_time=first_run)
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
        if not session.get('admin_logged_in') or session.get('admin_role') != 'super_admin': 
            return jsonify({'error': 'Super Admin Required'}), 403
        return f(*args, **kwargs)
    return decorated_function

@app.before_request
def track_traffic():
    if request.path == '/' or request.path == '/index':
        if not request.cookies.get('ghostmode') and not session.get('admin_logged_in'):
            if db:
                ip = request.headers.get('X-Forwarded-For', request.remote_addr)
                if ip: ip = ip.split(',')[0].strip()
                db.record_page_view(ip)

@app.route('/ghostmode')
def activate_ghost_mode():
    resp = make_response(jsonify({"success": True, "message": "👻 GHOST MODE ACTIVATED: Your visits to this site will no longer be tracked in the analytics."}))
    resp.set_cookie('ghostmode', '1', max_age=60*60*24*365*10) 
    return resp

@app.route('/')
def index(): return render_template('index.html')

@app.route('/login')
def login(): 
    if session.get('admin_logged_in'): return redirect(url_for('admin'))
    return render_template('login.html')

@app.route('/api/auth/google', methods=['POST'])
def auth_google():
    if not db: return jsonify({"success": False, "error": "Database offline"})
    token = request.json.get('token')
    user_data = db.verify_admin_token(token)
    if not user_data: return jsonify({"success": False, "error": "Invalid Token"})
    if user_data['role'] in ['admin', 'super_admin']:
        session.permanent = True 
        session['admin_logged_in'] = True
        session['admin_email'] = user_data['email']
        session['admin_role'] = user_data['role']
        return jsonify({"success": True})
    else: return jsonify({"success": False, "error": "Your account is pending Super Admin approval."})

@app.route('/logout')
def logout(): session.clear(); return redirect(url_for('index'))

@app.route('/admin')
def admin(): 
    if not session.get('admin_logged_in'): return redirect(url_for('login'))
    return render_template('admin.html', email=session.get('admin_email'), role=session.get('admin_role'))

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
            if real_name.lower() == player_name.lower(): player_name = real_name; break
    season = request.args.get('season', 'Career')
    division = request.args.get('division', 'All')
    stats = db.get_player_stats(player_name, season, division)
    return jsonify(stats) if stats else (jsonify({"error": "Not found"}), 404)

@app.route('/api/rankings/<season>/<division>')
def get_rankings(season, division): return jsonify(db.get_division_rankings(season, division, request.args.get('week', 'Latest'))) if db else jsonify([])

@app.route('/api/week/<season>/<week>')
def get_week_results(season, week): return jsonify(db.get_matches_by_week(season, week)) if db else jsonify([])

@app.route('/api/h2h')
def get_h2h():
    if not db: return jsonify({"error": "Offline"}), 500
    return jsonify(db.get_head_to_head(request.args.get('p1', ''), request.args.get('p2', '')))

@app.route('/api/feedback', methods=['POST'])
def submit_feedback():
    if not db: return jsonify({"success": False, "code": 500, "message": "Database connection offline"}), 500
    data = request.json
    if not data.get('message') or not data.get('contact'): return jsonify({"success": False, "code": 400, "message": "Missing required fields"}), 400
    success = db.user_submit_feedback(data.get('type'), data.get('message'), data.get('contact'), data.get('context'))
    if success: return jsonify({"success": True, "code": 200, "message": "Feedback securely saved to Firebase Database."}), 200
    else: return jsonify({"success": False, "code": 500, "message": "Firebase rejected the write operation."}), 500

@app.route('/api/report', methods=['POST'])
def submit_report():
    if not db: return jsonify({"success": False, "code": 500, "message": "Database connection offline"}), 500
    data = request.json
    success = db.user_submit_report(data.get('match_id'), data.get('p1'), data.get('p2'), data.get('date'), data.get('reporter'), data.get('problem'), data.get('suggested_home'), data.get('suggested_away'))
    if success: return jsonify({"success": True, "code": 200, "message": "Match Report securely saved to Firebase Database."}), 200
    else: return jsonify({"success": False, "code": 500, "message": "Firebase rejected the write operation."}), 500

@app.route('/api/create-payment-intent', methods=['POST'])
def create_payment():
    try:
        amount = int(float(request.json.get('amount', 5.00)) * 100) 
        intent = stripe.PaymentIntent.create(amount=amount, currency='aud', automatic_payment_methods={'enabled': True})
        return jsonify({'clientSecret': intent.client_secret})
    except Exception as e: return jsonify(error=str(e)), 403

@app.route('/api/record_donation', methods=['POST'])
def record_donation():
    if not db: return jsonify({"success": False})
    data = request.json
    success = db.record_donation(data.get('intent_id'), data.get('name'), data.get('amount'))
    return jsonify({"success": success})

@app.route('/api/top_donors')
def top_donors(): return jsonify(db.get_top_donors()) if db else jsonify([])

@app.route('/api/admin/traffic')
@login_required
def admin_traffic(): return jsonify(db.get_traffic_stats()) if db else jsonify({'views': 0, 'uniques': 0})

@app.route('/api/admin/reports')
@login_required
def get_reports(): return jsonify(db.admin_get_reports()) if db else jsonify([])

@app.route('/api/admin/date_errors')
@login_required
def get_date_errors(): return jsonify(db.admin_get_date_errors()) if db else jsonify([])

@app.route('/api/admin/resolve', methods=['POST'])
@login_required
def resolve_report():
    if not db: return jsonify({"success": False})
    return jsonify({"success": db.admin_resolve_report(request.json.get('report_id'), request.json.get('action'), session.get('admin_email'))})

@app.route('/api/admin/history')
@login_required
def search_history(): return jsonify(db.admin_search_history(request.args.get('q', ''))) if db else jsonify([])

@app.route('/api/admin/update_match', methods=['POST'])
@login_required
def update_match():
    if not db: return jsonify({"success": False})
    d = request.json
    return jsonify({"success": db.admin_update_historical_match(d.get('p1'), d.get('p2'), d.get('date'), d.get('s1'), d.get('s2'), d.get('new_date'), session.get('admin_email'))})

@app.route('/api/admin/bulk_fix_date', methods=['POST'])
@login_required
def bulk_fix_date():
    if not db: return jsonify({"success": False})
    d = request.json
    return jsonify({"success": db.admin_bulk_fix_date(d.get('season'), d.get('division'), d.get('week'), d.get('date'), session.get('admin_email'))})

@app.route('/api/admin/override_deltas', methods=['POST'])
@login_required
def override_deltas():
    if not db: return jsonify({"success": False})
    d = request.json
    return jsonify({"success": db.admin_override_match_deltas(d.get('match_id'), d.get('p1_delta'), d.get('p2_delta'), session.get('admin_email'))})

@app.route('/api/admin/set_rating_scales', methods=['POST'])
@super_admin_required
def set_rating_scales():
    if not db: return jsonify({"success": False})
    d = request.json
    return jsonify({"success": db.admin_set_rating_scales(d.get('k_win'), d.get('k_loss'), session.get('admin_email'))})

@app.route('/api/admin/chaos_config', methods=['GET'])
@login_required
def get_chaos_config():
    if not db: return jsonify({"success": False})
    return jsonify(db.admin_get_chaos_config())

@app.route('/api/admin/chaos_vote', methods=['POST'])
@login_required
def chaos_vote():
    if not db: return jsonify({"success": False})
    d = request.json
    return jsonify({"success": db.admin_vote_chaos(d.get('weeks', []), session.get('admin_email'))})

@app.route('/api/admin/chaos_clear', methods=['POST'])
@super_admin_required
def chaos_clear():
    if not db: return jsonify({"success": False})
    return jsonify({"success": db.admin_clear_chaos(session.get('admin_email'))})

@app.route('/api/admin/merge', methods=['POST'])
@login_required
def merge_players():
    if not db: return jsonify({"success": False})
    return jsonify({"success": db.admin_merge_players(request.json.get('bad_name'), request.json.get('good_name'), session.get('admin_email'))})

@app.route('/api/admin/override_rating', methods=['POST'])
@login_required
def override_rating():
    if not db: return jsonify({"success": False})
    return jsonify({"success": db.admin_override_rating(request.json.get('player_id'), request.json.get('rating'), request.json.get('retroactive', True), session.get('admin_email'))})

@app.route('/api/admin/bulk_pull_ratings', methods=['POST'])
@login_required
def bulk_pull_ratings():
    if not db: return jsonify({"success": False})
    return jsonify(db.admin_bulk_pull_ratings(session.get('admin_email', 'Unknown')))

@app.route('/api/admin/force_finish_live', methods=['POST'])
@login_required
def force_finish_live():
    if not db: return jsonify({"success": False})
    return jsonify({"success": db.admin_force_finish_live(request.json.get('id'), request.json.get('s1'), request.json.get('s2'), session.get('admin_email'))})

@app.route('/api/admin/wipe_live', methods=['POST'])
@login_required
def wipe_live():
    if not db: return jsonify({"success": False})
    return jsonify({"success": db.admin_wipe_live(request.json.get('id'), session.get('admin_email'))})

@app.route('/api/admin/set_fixture_format', methods=['POST'])
@login_required
def set_fixture_format():
    if not db: return jsonify({"success": False})
    return jsonify({"success": db.admin_set_fixture_format(request.json.get('fixture_id'), request.json.get('format_type'), session.get('admin_email'))})

@app.route('/api/admin/player_directory')
@login_required
def get_player_directory(): return jsonify(db.admin_get_player_directory()) if db else jsonify([])

@app.route('/api/admin/glicko_calc', methods=['POST'])
@login_required
def glicko_calc():
    if not db: return jsonify({"success": False})
    data = request.json
    res = db.admin_glicko_math(data.get('p1'), data.get('p2'), data.get('s1'), data.get('s2'))
    return jsonify({"success": True, "data": res})

@app.route('/api/admin/messages')
@login_required
def get_admin_messages(): return jsonify(db.get_admin_messages()) if db else jsonify([])

@app.route('/api/admin/add_message', methods=['POST'])
@login_required
def add_admin_message():
    if not db: return jsonify({"success": False})
    return jsonify({"success": db.add_admin_message(request.json.get('message'), session.get('admin_email'))})

@app.route('/api/notices')
def get_notices(): return jsonify(db.get_notices()) if db else jsonify([])

@app.route('/api/admin/add_notice', methods=['POST'])
@login_required
def add_notice():
    if not db: return jsonify({"success": False})
    data = request.json
    return jsonify({"success": db.admin_add_notice(data.get('title'), data.get('message'), data.get('type'), session.get('admin_email'))})

@app.route('/api/admin/delete_notice', methods=['POST'])
@login_required
def delete_notice():
    if not db: return jsonify({"success": False})
    return jsonify({"success": db.admin_delete_notice(request.json.get('notice_id'), session.get('admin_email'))})

@app.route('/api/admin/contacts')
@login_required
def get_contacts(): return jsonify(db.get_contact_lists()) if db else jsonify({"emails": "", "phones": ""})

@app.route('/api/admin/send_sms', methods=['POST'])
@login_required
def send_sms():
    if not db: return jsonify({"success": False, "error": "Database offline"})
    data = request.json
    msg = data.get('message')
    phones = data.get('phones') 
    if not msg: return jsonify({"success": False, "error": "Message cannot be empty."})
    result = db.admin_send_sms_broadcast(msg, phones, session.get('admin_email', 'Unknown'))
    return jsonify(result)

@app.route('/api/admin/donations')
@login_required
def admin_donations(): return jsonify(db.admin_get_all_donations()) if db else jsonify([])

@app.route('/api/admin/audit_logs')
@super_admin_required
def audit_logs(): return jsonify(db.get_audit_logs()) if db else jsonify([])

@app.route('/api/admin/undo_action', methods=['POST'])
@super_admin_required
def undo_action():
    if not db: return jsonify({"success": False})
    return jsonify({"success": db.undo_audit_action(request.json.get('log_id'), session.get('admin_email'))})

@app.route('/api/admin/users')
@super_admin_required
def admin_users(): return jsonify(db.get_admin_users()) if db else jsonify([])

@app.route('/api/admin/approve_user', methods=['POST'])
@super_admin_required
def approve_user():
    if not db: return jsonify({"success": False})
    return jsonify({"success": db.approve_admin(request.json.get('email'), request.json.get('action'))})

@app.route('/api/refresh', methods=['POST'])
@login_required
def force_refresh():
    if db: 
        db.refresh_data()
        db._log_audit(session.get('admin_email', 'Unknown'), 'FORCE_REFRESH', "Manually triggered a database sync with the Master Google Sheet.", {})
    return jsonify({"success": True, "status": "Refreshed"})

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5001))
    app.run(debug=True, host='0.0.0.0', port=port)