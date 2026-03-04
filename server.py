import os
import logging
import datetime
import stripe
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from functools import wraps
from flask import Flask, render_template, jsonify, request, send_from_directory, session, redirect, url_for
from apscheduler.schedulers.background import BackgroundScheduler
import atexit

from backend.backend import ThunderData

app = Flask(__name__, static_folder="frontend/static", template_folder="frontend/templates")

app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'gctta-super-secret-session-key')
ADMIN_PASSWORD = os.environ.get('ADMIN_PASS', 'TTgctta67-')
stripe.api_key = os.environ.get('STRIPE_SECRET_KEY')

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
        logger.info("💻 Local environment detected: Background scheduler starting immediately.")
    else:
        first_run = datetime.datetime.now() + datetime.timedelta(minutes=30)
        logger.info("🌐 Render environment detected: Background scheduler delayed by 30 mins.")

    scheduler.add_job(func=scheduled_refresh, trigger="interval", hours=12, id="scheduled_refresh", next_run_time=first_run)
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown())
except Exception as e:
    logger.error(f"❌ ERROR starting BackgroundScheduler: {str(e)}", exc_info=True)

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('admin_logged_in'): return jsonify({'error': 'Unauthorized. Please log in.'}), 401
        return f(*args, **kwargs)
    return decorated_function

@app.route('/')
def index(): return render_template('index.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        if request.form.get('password') == ADMIN_PASSWORD:
            session['admin_logged_in'] = True
            return redirect(url_for('admin'))
        else:
            error = "Incorrect password. Access Denied."
    return render_template('login.html', error=error)

@app.route('/logout')
def logout():
    session.pop('admin_logged_in', None)
    return redirect(url_for('index'))

@app.route('/admin')
def admin(): 
    if not session.get('admin_logged_in'): return redirect(url_for('login'))
    return render_template('admin.html')

@app.route('/api/players')
def get_players(): return jsonify(list(db.get_all_players().keys())) if db else jsonify([])

@app.route('/api/roster_detailed')
def get_roster_detailed(): return jsonify(db.get_roster_with_meta()) if db else jsonify([])

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
                player_name = real_name; break
    season = request.args.get('season', 'Career')
    division = request.args.get('division', 'All')
    stats = db.get_player_stats(player_name, season, division)
    return jsonify(stats) if stats else (jsonify({"error": "Not found"}), 404)

@app.route('/api/rankings/<season>/<division>')
def get_rankings(season, division): return jsonify(db.get_division_rankings(season, division, request.args.get('week', 'Latest'))) if db else jsonify([])

@app.route('/api/week/<season>/<week>')
def get_week_results(season, week): return jsonify(db.get_matches_by_week(season, week)) if db else jsonify([])

@app.route('/api/report', methods=['POST'])
def submit_report():
    if not db: return jsonify({"success": False})
    d = request.json
    # NEW: Captures Match ID from the user
    if db.user_submit_report(d.get('p1'), d.get('p2'), d.get('date'), d.get('reporter'), d.get('problem'), d.get('suggested_home'), d.get('suggested_away'), d.get('match_id', '')): return jsonify({"success": True})
    return jsonify({"success": False}), 500

@app.route('/api/feedback', methods=['POST'])
def handle_feedback():
    if not db: return jsonify({"success": False}), 500
    data = request.json
    db.user_submit_feedback(data.get('type', 'Feedback'), data.get('message', ''), data.get('contact', 'Anonymous'), data.get('context', 'Not provided'))
    return jsonify({"success": True})

@app.route('/api/create-payment-intent', methods=['POST'])
def create_payment():
    try:
        data = request.json
        amount = int(float(data.get('amount', 5.00)) * 100) 
        intent = stripe.PaymentIntent.create(amount=amount, currency='aud', automatic_payment_methods={'enabled': True})
        return jsonify({'clientSecret': intent.client_secret})
    except Exception as e: return jsonify(error=str(e)), 403

@app.route('/api/admin/reports')
@login_required
def get_reports(): return jsonify(db.admin_get_reports()) if db else jsonify([])

@app.route('/api/admin/resolve', methods=['POST'])
@login_required
def resolve_report():
    if not db: return jsonify({"success": False})
    d = request.json
    if db.admin_resolve_report(d.get('report_id'), d.get('action')): return jsonify({"success": True})
    return jsonify({"success": False}), 500

@app.route('/api/admin/history')
@login_required
def search_history():
    q = request.args.get('q', '')
    season = request.args.get('season', '')
    division = request.args.get('division', '')
    week = request.args.get('week', '')
    date = request.args.get('date', '')
    return jsonify(db.admin_search_history(q, season, division, week, date)) if db else jsonify([])

@app.route('/api/admin/update_match', methods=['POST'])
@login_required
def update_match():
    if not db: return jsonify({"success": False})
    d = request.json
    if db.admin_update_historical_match(d.get('p1'), d.get('p2'), d.get('date'), d.get('s1'), d.get('s2')): return jsonify({"success": True})
    return jsonify({"success": False}), 500

@app.route('/api/admin/duplicates')
@login_required
def get_duplicates(): return jsonify(db.admin_get_merge_suggestions()) if db else jsonify([])

@app.route('/api/admin/merge', methods=['POST'])
@login_required
def merge_players():
    if not db: return jsonify({"success": False})
    return jsonify({"success": db.admin_merge_players(request.json.get('bad_name'), request.json.get('good_name'))})

@app.route('/api/admin/approvals')
@login_required
def get_approvals(): return jsonify(db.admin_get_pending_approvals()) if db else jsonify([])

@app.route('/api/admin/resolve_approval', methods=['POST'])
@login_required
def resolve_approval():
    if not db: return jsonify({"success": False})
    data = request.json
    success = db.admin_resolve_approval(data.get('id'), data.get('action'), data.get('s1'), data.get('s2'))
    return jsonify({"success": success})

@app.route('/api/admin/recent_approved')
@login_required
def recent_approved(): return jsonify(db.admin_get_recent_approved()) if db else jsonify([])

@app.route('/api/admin/delete_recent', methods=['POST'])
@login_required
def delete_recent():
    if not db: return jsonify({"success": False})
    success = db.admin_delete_match_result(request.json.get('id'))
    return jsonify({"success": success})

@app.route('/api/admin/ranks', methods=['GET', 'POST'])
@login_required
def manage_ranks():
    if request.method == 'GET': return jsonify(db.admin_get_ranks()) if db else jsonify({})
    else:
        data = request.json
        success = db.admin_set_rank(data.get('name'), data.get('rank'))
        return jsonify({"success": success})

@app.route('/api/admin/override_rating', methods=['POST'])
@login_required
def override_rating():
    if not db: return jsonify({"success": False})
    data = request.json
    success = db.admin_override_rating(data.get('name'), data.get('rating'))
    return jsonify({"success": success})

@app.route('/api/refresh')
@login_required
def force_refresh():
    if db: db.refresh_data()
    return jsonify({"status": "Refreshed"})

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5001))
    app.run(debug=True, host='0.0.0.0', port=port)