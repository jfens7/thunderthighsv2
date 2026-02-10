from flask import Flask, render_template, jsonify, request, send_from_directory
from backend.backend import ThunderData
import os
import logging
from apscheduler.schedulers.background import BackgroundScheduler
import atexit

# --- CONFIG ---
app = Flask(__name__, 
            static_folder="frontend/static", 
            template_folder="frontend/templates")

# --- BETTER DEBUGGING LOGS ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Backend
db = None
try:
    db = ThunderData()
    logger.info("✅ ThunderData Backend Loaded Successfully.")
except Exception as e:
    logger.error(f"❌ Failed to start Backend: {e}")

# --- SCHEDULER (Auto-Refresh Data) ---
def scheduled_refresh():
    if db:
        logger.info("⏰ Auto-Refresh Triggered")
        db.refresh_data()

scheduler = BackgroundScheduler()
scheduler.add_job(func=scheduled_refresh, trigger="interval", minutes=30, id="scheduled_refresh")
scheduler.start()
atexit.register(lambda: scheduler.shutdown())

# --- ROUTES ---

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/admin')
def admin():
    return render_template('admin.html')

# --- DEBUG API ---
@app.route('/api/debug_data')
def debug_data():
    """Returns a small sample of data to verify format in browser"""
    if not db: return jsonify({"status": "Offline"})
    sample_players = list(db.all_players.keys())[:5]
    return jsonify({
        "total_players": len(db.all_players),
        "total_matches_loaded": sum(len(s['combined']['history']) for s in db.all_players.values()) if db.all_players else 0,
        "sample_names": sample_players,
        "backend_status": "Online"
    })

# --- CRITICAL FIX: PLAYERS API ---
@app.route('/api/players')
def get_players():
    if not db: return jsonify([])
    
    # DEBUG LOG: Print exactly what we are sending
    player_list = list(db.get_all_players().keys())
    logger.info(f"🔍 API Request: /api/players")
    logger.info(f"📤 Sending list of {len(player_list)} players to Frontend.")
    
    # FIX: Send LIST, not DICT
    return jsonify(player_list)

@app.route('/api/seasons')
def get_seasons():
    if not db: return jsonify([])
    seasons = db.get_seasons()
    return jsonify(seasons)

@app.route('/api/divisions')
def get_divisions():
    if not db: return jsonify([])
    return jsonify(db.get_divisions())

@app.route('/api/stats/<player_name>')
def get_player_stats(player_name):
    if not db: return jsonify({"error": "Offline"}), 500
    
    # Case-insensitive lookup
    if player_name not in db.all_players:
        for real_name in db.all_players.keys():
            if real_name.lower() == player_name.lower():
                player_name = real_name
                break
                
    season = request.args.get('season', 'Career')
    division = request.args.get('division', 'All')
    
    logger.info(f"🔍 Fetching Profile: {player_name} (Season: {season})")
    stats = db.get_player_stats(player_name, season, division)
    return jsonify(stats) if stats else (jsonify({"error": "Not found"}), 404)

@app.route('/api/rankings/<season>/<division>')
def get_rankings(season, division):
    if not db: return jsonify([])
    week = request.args.get('week', 'Latest')
    
    logger.info(f"📊 Fetching Rankings: {season} | {division} | Week: {week}")
    data = db.get_division_rankings(season, division, week)
    
    return jsonify(data)

@app.route('/api/week/<season>/<week>')
def get_week_results(season, week):
    if not db: return jsonify([])
    return jsonify(db.get_matches_by_week(season, week))

@app.route('/static/<path:path>')
def send_static(path):
    return send_from_directory('frontend/static', path)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)