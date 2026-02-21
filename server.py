print("🚀 [BOOT 1] Starting server.py...")
from flask import Flask, render_template, jsonify, request, send_from_directory
print("🚀 [BOOT 2] Flask imported successfully.")

print("🚀 [BOOT 3] Importing Backend...")
from backend.backend import ThunderData
print("🚀 [BOOT 4] Backend imported successfully.")

import os
import logging
import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import atexit

app = Flask(__name__, static_folder="frontend/static", template_folder="frontend/templates")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

db = None
try:
    print("🚀 [BOOT 5] Initializing ThunderData (Connecting to Google/Firebase)...")
    db = ThunderData()
    print("🚀 [BOOT 6] ThunderData Initialized Successfully!")
except Exception as e:
    logger.error(f"❌ Failed to start Backend: {e}")

def scheduled_refresh():
    if db:
        logger.info("⏰ Background Data Sync Started...")
        db.refresh_data()
        logger.info("✅ Background Data Sync Complete!")

print("🚀 [BOOT 7] Setting up background scheduler...")
scheduler = BackgroundScheduler()
scheduler.add_job(func=scheduled_refresh, trigger="interval", minutes=30, id="scheduled_refresh", next_run_time=datetime.datetime.now())
scheduler.start()
atexit.register(lambda: scheduler.shutdown())
print("🚀 [BOOT 8] Scheduler ready.")

# --- ROUTES ---
@app.route('/')
def index(): return render_template('index.html')

@app.route('/admin')
def admin(): return render_template('admin.html')

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
    if db.user_submit_report(d.get('p1'), d.get('p2'), d.get('date'), d.get('reporter'), d.get('problem'), d.get('suggested_home'), d.get('suggested_away')): return jsonify({"success": True})
    return jsonify({"success": False}), 500

@app.route('/api/admin/reports')
def get_reports(): return jsonify(db.admin_get_reports()) if db else jsonify([])

@app.route('/api/admin/resolve', methods=['POST'])
def resolve_report():
    if not db: return jsonify({"success": False})
    d = request.json
    if db.admin_resolve_report(d.get('report_id'), d.get('action')): return jsonify({"success": True})
    return jsonify({"success": False}), 500

@app.route('/api/admin/history')
def search_history(): return jsonify(db.admin_search_history(request.args.get('q', ''))) if db else jsonify([])

@app.route('/api/admin/update_match', methods=['POST'])
def update_match():
    if not db: return jsonify({"success": False})
    d = request.json
    if db.admin_update_historical_match(d.get('p1'), d.get('p2'), d.get('date'), d.get('s1'), d.get('s2')): return jsonify({"success": True})
    return jsonify({"success": False}), 500

@app.route('/api/admin/duplicates')
def get_duplicates(): return jsonify(db.admin_get_merge_suggestions()) if db else jsonify([])

@app.route('/api/admin/merge', methods=['POST'])
def merge_players():
    if not db: return jsonify({"success": False})
    return jsonify({"success": db.admin_merge_players(request.json.get('bad_name'), request.json.get('good_name'))})

@app.route('/api/refresh')
def force_refresh():
    if db: db.refresh_data()
    return jsonify({"status": "Refreshed"})

if __name__ == '__main__':
    print("🚀 [BOOT 9] Starting Flask Server on Port 5001...")
    app.run(debug=True, host='0.0.0.0', port=5001)